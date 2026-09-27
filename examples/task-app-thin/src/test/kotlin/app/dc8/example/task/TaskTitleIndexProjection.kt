package app.dc8.example.task

import app.dc8.axonthin.api.ReplayInto
import app.dc8.example.task.api.TaskCreatedEvent
import app.dc8.example.task.api.TaskDeletedEvent
import app.dc8.example.task.api.TaskEvent
import app.dc8.example.task.api.TaskRenamedEvent
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.PostLoad
import jakarta.persistence.PostPersist
import jakarta.persistence.Table
import jakarta.persistence.Transient
import org.axonframework.config.ProcessingGroup
import org.axonframework.eventhandling.EventHandler
import org.axonframework.eventhandling.EventMessage
import org.springframework.data.domain.Persistable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Component
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Read model written by a batch projection. Implements [Persistable] so `save` of a new row is a plain INSERT: with an
 * assigned id, Spring Data would otherwise `merge` it — one SELECT per row.
 */
@Entity
@Table(name = "task_title_index")
class TaskTitleIndex(@Id val taskId: String, var title: String) : Persistable<String> {

    @Transient
    private var fresh = false

    override fun getId(): String = taskId

    override fun isNew(): Boolean = fresh

    @PostLoad
    @PostPersist
    fun markStored() {
        fresh = false
    }

    companion object {
        fun create(taskId: String, title: String) = TaskTitleIndex(taskId, title).apply { fresh = true }
    }
}

interface TaskTitleIndexRepository : JpaRepository<TaskTitleIndex, String>

/**
 * Thin-only batch projection (Axon 4 never calls `List` handlers): per chunk (or replay page) one read of the affected
 * rows, then batched writes — the pattern for hot projections.
 */
@Component
@ProcessingGroup("task-title-index")
@ReplayInto(TaskTitleIndex::class)
class TaskTitleIndexProjection(private val rows: TaskTitleIndexRepository) {

    /** Sizes of the batches received (for tests). */
    val batches = CopyOnWriteArrayList<Int>()

    @EventHandler
    fun on(events: List<EventMessage<TaskEvent>>) {
        batches += events.size
        val byId = rows.findAllById(events.map { it.payload.taskId }.toSet()).associateByTo(HashMap()) { it.taskId } // 1 read
        val deleted = HashSet<String>()
        for (event in events) {
            when (val payload = event.payload) {
                is TaskCreatedEvent -> byId[payload.taskId] = TaskTitleIndex.create(payload.taskId, payload.title)
                is TaskRenamedEvent -> byId[payload.taskId]?.title = payload.title
                is TaskDeletedEvent -> byId.remove(payload.taskId)?.let { deleted += payload.taskId }
                else -> Unit
            }
        }
        rows.saveAll(byId.values)                 // new → INSERT, loaded → dirty-checked UPDATE; JDBC-batched at flush
        if (deleted.isNotEmpty()) rows.deleteAllByIdInBatch(deleted) // 1 delete
    }
}
