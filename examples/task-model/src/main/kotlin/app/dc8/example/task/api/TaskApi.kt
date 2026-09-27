package app.dc8.example.task.api

import org.axonframework.modelling.command.TargetAggregateIdentifier
import org.axonframework.serialization.Revision

// ---- shared types ---------------------------------------------------------------------------------------------------

enum class TaskStatus { TODO, IN_PROGRESS, DONE }

class CommentNotFoundException(commentId: String) : RuntimeException("Comment $commentId not found")

/** Business rule violation: a DONE task is read-only (no comments, no edits) until reopened. */
class TaskClosedException(taskId: String) : RuntimeException("Task $taskId is closed")

// ---- commands (aggregate) -------------------------------------------------------------------------------------------

/** Handled by the Task constructor. */
data class CreateTaskCommand(val taskId: String, val title: String, val description: String? = null)

/** Upsert: `@CreationPolicy(CREATE_IF_MISSING)` — creates the task or renames an existing one. */
data class ImportTaskCommand(@TargetAggregateIdentifier val taskId: String, val title: String)

data class RenameTaskCommand(@TargetAggregateIdentifier val taskId: String, val title: String)

data class ChangeTaskStatusCommand(@TargetAggregateIdentifier val taskId: String, val status: TaskStatus)

data class AssignTaskCommand(@TargetAggregateIdentifier val taskId: String, val assignee: String?)

data class DeleteTaskCommand(@TargetAggregateIdentifier val taskId: String)

data class AddCommentCommand(
    @TargetAggregateIdentifier val taskId: String,
    val commentId: String,
    val author: String,
    val text: String,
)

data class EditCommentCommand(@TargetAggregateIdentifier val taskId: String, val commentId: String, val text: String)

data class DeleteCommentCommand(@TargetAggregateIdentifier val taskId: String, val commentId: String)

// ---- commands (external handler) ------------------------------------------------------------------------------------

/** Handled by a Spring bean that dispatches one nested CreateTaskCommand per title. */
data class CreateTasksFromTemplateCommand(val prefix: String, val titles: List<String>)

// ---- events ---------------------------------------------------------------------------------------------------------

/** Common supertype: lets projections subscribe to "any task event" (Axon resolves handlers by assignability). */
sealed interface TaskEvent {
    val taskId: String
}

/** Revision 2 added `createdBy`; stored as `payload_revision = '2'`. */
@Revision("2")
data class TaskCreatedEvent(
    override val taskId: String,
    val title: String,
    val description: String?,
    val createdBy: String?,
) : TaskEvent

data class TaskRenamedEvent(override val taskId: String, val title: String) : TaskEvent

data class TaskStatusChangedEvent(override val taskId: String, val from: TaskStatus, val to: TaskStatus) : TaskEvent

data class TaskAssignedEvent(override val taskId: String, val assignee: String?) : TaskEvent

data class TaskDeletedEvent(override val taskId: String) : TaskEvent

data class CommentAddedEvent(
    override val taskId: String,
    val commentId: String,
    val author: String,
    val text: String,
) : TaskEvent

data class CommentEditedEvent(override val taskId: String, val commentId: String, val text: String) : TaskEvent

data class CommentDeletedEvent(override val taskId: String, val commentId: String) : TaskEvent

/** Not an aggregate event: published through EventGateway (stored with type = null, sequence 0). */
data class TasksImportedEvent(val count: Int, val source: String)
