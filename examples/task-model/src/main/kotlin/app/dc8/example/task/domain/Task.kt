package app.dc8.example.task.domain

import app.dc8.example.task.api.AddCommentCommand
import com.fasterxml.jackson.annotation.JsonAutoDetect
import app.dc8.example.task.api.AssignTaskCommand
import app.dc8.example.task.api.ChangeTaskStatusCommand
import app.dc8.example.task.api.CommentAddedEvent
import app.dc8.example.task.api.CommentDeletedEvent
import app.dc8.example.task.api.CommentEditedEvent
import app.dc8.example.task.api.CommentNotFoundException
import app.dc8.example.task.api.CreateTaskCommand
import app.dc8.example.task.api.DeleteCommentCommand
import app.dc8.example.task.api.DeleteTaskCommand
import app.dc8.example.task.api.EditCommentCommand
import app.dc8.example.task.api.ImportTaskCommand
import app.dc8.example.task.api.RenameTaskCommand
import app.dc8.example.task.api.TaskAssignedEvent
import app.dc8.example.task.api.TaskClosedException
import app.dc8.example.task.api.TaskCreatedEvent
import app.dc8.example.task.api.TaskDeletedEvent
import app.dc8.example.task.api.TaskRenamedEvent
import app.dc8.example.task.api.TaskStatus
import app.dc8.example.task.api.TaskStatusChangedEvent
import org.axonframework.commandhandling.CommandHandler
import org.axonframework.eventsourcing.EventSourcingHandler
import org.axonframework.messaging.annotation.MetaDataValue
import org.axonframework.modelling.command.AggregateCreationPolicy
import org.axonframework.modelling.command.AggregateIdentifier
import org.axonframework.modelling.command.AggregateLifecycle.apply
import org.axonframework.modelling.command.AggregateLifecycle.markDeleted
import org.axonframework.modelling.command.AggregateVersion
import org.axonframework.modelling.command.CreationPolicy
import org.axonframework.spring.stereotype.Aggregate

/**
 * Event-sourced aggregate: state is rebuilt from `domain_event_entry` on load, starting from the latest snapshot.
 * Snapshots serialize the aggregate itself with the event serializer (Jackson), hence the field visibility.
 */
@Aggregate(snapshotTriggerDefinition = "taskSnapshotTrigger")
@JsonAutoDetect(
    fieldVisibility = JsonAutoDetect.Visibility.ANY,
    getterVisibility = JsonAutoDetect.Visibility.NONE,
    isGetterVisibility = JsonAutoDetect.Visibility.NONE,
)
class Task() {

    @AggregateIdentifier
    private var taskId: String? = null

    /** Only read by Axon (state-stored aggregates); never written for event-sourced ones — use getVersion(). */
    @AggregateVersion
    private var version: Long? = null

    private var title: String = ""
    private var status: TaskStatus = TaskStatus.TODO
    private var assignee: String? = null
    private val comments = mutableSetOf<String>()

    /** Creation: parameters beyond the payload are resolved from metadata. */
    @CommandHandler
    constructor(command: CreateTaskCommand, @MetaDataValue("userId") userId: String?) : this() {
        require(command.title.isNotBlank()) { "Task title must not be blank" }
        apply(TaskCreatedEvent(command.taskId, command.title.trim(), command.description, userId))
    }

    /** Upsert used by bulk imports; returns the task id either way. */
    @CommandHandler
    @CreationPolicy(AggregateCreationPolicy.CREATE_IF_MISSING)
    fun handle(command: ImportTaskCommand): String {
        require(command.title.isNotBlank()) { "Task title must not be blank" }
        if (taskId == null) apply(TaskCreatedEvent(command.taskId, command.title, null, null))
        else if (title != command.title) apply(TaskRenamedEvent(command.taskId, command.title))
        return command.taskId
    }

    @CommandHandler
    fun handle(command: RenameTaskCommand) {
        require(command.title.isNotBlank()) { "Task title must not be blank" }
        ensureOpen()
        if (title != command.title) apply(TaskRenamedEvent(command.taskId, command.title, command.bulk))
    }

    /** Returns the previous status (non-Unit result through sendAllAndWait). */
    @CommandHandler
    fun handle(command: ChangeTaskStatusCommand): TaskStatus {
        val previous = status
        if (previous != command.status) apply(TaskStatusChangedEvent(command.taskId, previous, command.status))
        return previous
    }

    @CommandHandler
    fun handle(command: AssignTaskCommand) {
        ensureOpen()
        if (assignee != command.assignee) apply(TaskAssignedEvent(command.taskId, command.assignee))
    }

    @CommandHandler
    fun handle(command: DeleteTaskCommand) {
        apply(TaskDeletedEvent(command.taskId))
    }

    @CommandHandler
    fun handle(command: AddCommentCommand): String {
        require(command.text.isNotBlank()) { "Comment text must not be blank" }
        ensureOpen()
        apply(CommentAddedEvent(command.taskId, command.commentId, command.author, command.text))
        return command.commentId
    }

    @CommandHandler
    fun handle(command: EditCommentCommand) {
        ensureOpen()
        if (command.commentId !in comments) throw CommentNotFoundException(command.commentId)
        apply(CommentEditedEvent(command.taskId, command.commentId, command.text))
    }

    @CommandHandler
    fun handle(command: DeleteCommentCommand) {
        if (command.commentId !in comments) throw CommentNotFoundException(command.commentId)
        apply(CommentDeletedEvent(command.taskId, command.commentId))
    }

    private fun ensureOpen() {
        if (status == TaskStatus.DONE) throw TaskClosedException(taskId!!)
    }

    // ---- state ------------------------------------------------------------------------------------------------------

    @EventSourcingHandler
    fun on(event: TaskCreatedEvent) {
        taskId = event.taskId
        title = event.title
    }

    @EventSourcingHandler
    fun on(event: TaskRenamedEvent) {
        title = event.title
    }

    @EventSourcingHandler
    fun on(event: TaskStatusChangedEvent) {
        status = event.to
    }

    @EventSourcingHandler
    fun on(event: TaskAssignedEvent) {
        assignee = event.assignee
    }

    @EventSourcingHandler
    fun on(event: CommentAddedEvent) {
        comments += event.commentId
    }

    @EventSourcingHandler
    fun on(event: CommentDeletedEvent) {
        comments -= event.commentId
    }

    @EventSourcingHandler
    fun on(@Suppress("UNUSED_PARAMETER") event: TaskDeletedEvent) {
        markDeleted()
    }
}
