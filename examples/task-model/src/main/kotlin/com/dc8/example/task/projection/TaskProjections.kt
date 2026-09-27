package com.dc8.example.task.projection

import com.dc8.example.task.api.CommentAddedEvent
import com.dc8.example.task.api.CommentDeletedEvent
import com.dc8.example.task.api.TaskAssignedEvent
import com.dc8.example.task.api.TaskCreatedEvent
import com.dc8.example.task.api.TaskEvent
import com.dc8.example.task.api.TaskRenamedEvent
import com.dc8.example.task.api.TaskStatus
import com.dc8.example.task.api.TaskStatusChangedEvent
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.axonframework.eventhandling.EventHandler
import org.axonframework.eventhandling.Timestamp
import org.axonframework.messaging.annotation.MessageIdentifier
import org.axonframework.messaging.annotation.MetaDataValue
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Component
import java.time.Instant

// ---- read model -----------------------------------------------------------------------------------------------------

@Entity
@Table(name = "task_summary")
class TaskSummary(
    @Id val taskId: String,
    var title: String,
    @Enumerated(EnumType.STRING) var status: TaskStatus,
    var assignee: String? = null,
    var commentCount: Int = 0,
)

@Entity
@Table(name = "task_activity")
class TaskActivity(
    val taskId: String,
    val type: String,
    val eventId: String,
    val correlationId: String?,
    val occurredAt: Instant,
) {
    @Id
    @GeneratedValue
    var id: Long? = null
}

interface TaskSummaryRepository : JpaRepository<TaskSummary, String> {
    fun findByStatusOrderByTitle(status: TaskStatus): List<TaskSummary>
}

interface TaskActivityRepository : JpaRepository<TaskActivity, Long> {
    fun findByTaskIdOrderById(taskId: String): List<TaskActivity>
}

// ---- projections ----------------------------------------------------------------------------------------------------

/** Denormalized list view, updated in the command's transaction (subscribing processor semantics). */
@Component
class TaskSummaryProjection(private val summaries: TaskSummaryRepository) {

    @EventHandler
    fun on(event: TaskCreatedEvent) {
        summaries.save(TaskSummary(event.taskId, event.title, TaskStatus.TODO))
    }

    @EventHandler
    fun on(event: TaskRenamedEvent) = update(event.taskId) { title = event.title }

    @EventHandler
    fun on(event: TaskStatusChangedEvent) = update(event.taskId) { status = event.to }

    @EventHandler
    fun on(event: TaskAssignedEvent) = update(event.taskId) { assignee = event.assignee }

    @EventHandler
    fun on(event: CommentAddedEvent) = update(event.taskId) { commentCount++ }

    @EventHandler
    fun on(event: CommentDeletedEvent) = update(event.taskId) { commentCount-- }

    private fun update(taskId: String, change: TaskSummary.() -> Unit) {
        summaries.findById(taskId).orElseThrow { IllegalStateException("No summary for task $taskId") }.change()
    }
}

/** Audit trail: one supertype handler for every [TaskEvent], using message-level parameter resolvers. */
@Component
class TaskActivityProjection(private val activities: TaskActivityRepository) {

    @EventHandler
    fun on(
        event: TaskEvent,
        @MessageIdentifier eventId: String,
        @Timestamp occurredAt: Instant,
        @MetaDataValue("correlationId") correlationId: String?,
    ) {
        activities.save(TaskActivity(event.taskId, event::class.simpleName!!, eventId, correlationId, occurredAt))
    }
}
