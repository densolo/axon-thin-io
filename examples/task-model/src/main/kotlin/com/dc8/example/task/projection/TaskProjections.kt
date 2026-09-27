package com.dc8.example.task.projection

import com.dc8.example.task.api.CommentAddedEvent
import com.dc8.example.task.api.CommentDeletedEvent
import com.dc8.example.task.api.CommentEditedEvent
import com.dc8.example.task.api.TaskAssignedEvent
import com.dc8.example.task.api.TaskCreatedEvent
import com.dc8.example.task.api.TaskDeletedEvent
import com.dc8.example.task.api.TaskEvent
import com.dc8.example.task.api.TaskRenamedEvent
import com.dc8.example.task.api.TaskStatus
import com.dc8.example.task.api.TaskStatusChangedEvent
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.axonframework.config.ProcessingGroup
import org.axonframework.eventhandling.EventHandler
import org.axonframework.eventhandling.SequenceNumber
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
    val createdBy: String?,
    var assignee: String? = null,
    var commentCount: Int = 0,
)

@Entity
@Table(name = "task_comment")
class CommentView(
    @Id val commentId: String,
    val taskId: String,
    val author: String,
    @Column(length = 4000) var text: String,
    val createdAt: Instant,
)

@Entity
@Table(name = "task_activity")
class TaskActivity(
    val taskId: String,
    val type: String,
    val eventId: String,
    val sequenceNumber: Long?,
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

interface CommentViewRepository : JpaRepository<CommentView, String> {
    fun findByTaskIdOrderByCreatedAt(taskId: String): List<CommentView>
}

interface TaskActivityRepository : JpaRepository<TaskActivity, Long> {
    fun findByTaskIdOrderById(taskId: String): List<TaskActivity>
}

// ---- projections (processing groups are declared as in a real app; axon-thin ignores them) --------------------------

/** Denormalized list view, updated in the command's transaction (subscribing processor semantics). */
@Component
@ProcessingGroup("task-summary")
class TaskSummaryProjection(private val summaries: TaskSummaryRepository) {

    @EventHandler
    fun on(event: TaskCreatedEvent) {
        summaries.save(TaskSummary(event.taskId, event.title, TaskStatus.TODO, event.createdBy))
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

    @EventHandler
    fun on(event: TaskDeletedEvent) = summaries.deleteById(event.taskId)

    private fun update(taskId: String, change: TaskSummary.() -> Unit) {
        summaries.findById(taskId).orElseThrow { IllegalStateException("No summary for task $taskId") }.change()
    }
}

@Component
@ProcessingGroup("task-comments")
class CommentProjection(private val comments: CommentViewRepository) {

    @EventHandler
    fun on(event: CommentAddedEvent, @Timestamp at: Instant) {
        comments.save(CommentView(event.commentId, event.taskId, event.author, event.text, at))
    }

    @EventHandler
    fun on(event: CommentEditedEvent) {
        comments.findById(event.commentId).ifPresent { it.text = event.text }
    }

    @EventHandler
    fun on(event: CommentDeletedEvent) = comments.deleteById(event.commentId)
}

/** Audit trail: one supertype handler for every [TaskEvent], using message-level parameter resolvers. */
@Component
@ProcessingGroup("task-activity")
class TaskActivityProjection(private val activities: TaskActivityRepository) {

    @EventHandler
    fun on(
        event: TaskEvent,
        @MessageIdentifier eventId: String,
        @SequenceNumber sequenceNumber: Long?,
        @Timestamp occurredAt: Instant,
        @MetaDataValue("correlationId") correlationId: String?,
    ) {
        activities.save(TaskActivity(event.taskId, event::class.simpleName!!, eventId, sequenceNumber, correlationId, occurredAt))
    }
}
