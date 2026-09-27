package com.dc8.example.task.api

// ---- shared types ---------------------------------------------------------------------------------------------------

enum class TaskStatus { TODO, IN_PROGRESS, DONE }

class TaskNotFoundException(taskId: String) : RuntimeException("Task $taskId not found")

class CommentNotFoundException(commentId: String) : RuntimeException("Comment $commentId not found")

/** Business rule violation: a DONE task is read-only (no comments, no edits) until reopened. */
class TaskClosedException(taskId: String) : RuntimeException("Task $taskId is closed")

// ---- commands -------------------------------------------------------------------------------------------------------

data class CreateTaskCommand(val taskId: String, val title: String, val description: String? = null)

data class RenameTaskCommand(val taskId: String, val title: String)

data class ChangeTaskStatusCommand(val taskId: String, val status: TaskStatus)

data class AssignTaskCommand(val taskId: String, val assignee: String?)

data class AddCommentCommand(val commentId: String, val taskId: String, val author: String, val text: String)

data class EditCommentCommand(val commentId: String, val text: String)

data class DeleteCommentCommand(val commentId: String)

// ---- events ---------------------------------------------------------------------------------------------------------

/** Common supertype: lets projections subscribe to "any task event" (Axon resolves handlers by assignability). */
sealed interface TaskEvent {
    val taskId: String
}

data class TaskCreatedEvent(
    override val taskId: String,
    val title: String,
    val description: String?,
    val createdBy: String?,
) : TaskEvent

data class TaskRenamedEvent(override val taskId: String, val title: String) : TaskEvent

data class TaskStatusChangedEvent(override val taskId: String, val from: TaskStatus, val to: TaskStatus) : TaskEvent

data class TaskAssignedEvent(override val taskId: String, val assignee: String?) : TaskEvent

data class CommentAddedEvent(
    override val taskId: String,
    val commentId: String,
    val author: String,
    val text: String,
) : TaskEvent

data class CommentEditedEvent(override val taskId: String, val commentId: String, val text: String) : TaskEvent

data class CommentDeletedEvent(override val taskId: String, val commentId: String) : TaskEvent
