package com.dc8.example.task.command

import com.dc8.example.task.api.AddCommentCommand
import com.dc8.example.task.api.AssignTaskCommand
import com.dc8.example.task.api.ChangeTaskStatusCommand
import com.dc8.example.task.api.CommentAddedEvent
import com.dc8.example.task.api.CommentDeletedEvent
import com.dc8.example.task.api.CommentEditedEvent
import com.dc8.example.task.api.CommentNotFoundException
import com.dc8.example.task.api.CreateTaskCommand
import com.dc8.example.task.api.DeleteCommentCommand
import com.dc8.example.task.api.EditCommentCommand
import com.dc8.example.task.api.RenameTaskCommand
import com.dc8.example.task.api.TaskAssignedEvent
import com.dc8.example.task.api.TaskClosedException
import com.dc8.example.task.api.TaskCreatedEvent
import com.dc8.example.task.api.TaskNotFoundException
import com.dc8.example.task.api.TaskRenamedEvent
import com.dc8.example.task.api.TaskStatus
import com.dc8.example.task.api.TaskStatusChangedEvent
import com.dc8.example.task.domain.Comment
import com.dc8.example.task.domain.CommentRepository
import com.dc8.example.task.domain.Task
import com.dc8.example.task.domain.TaskRepository
import org.axonframework.commandhandling.CommandHandler
import org.axonframework.eventhandling.gateway.EventGateway
import org.axonframework.messaging.annotation.MetaDataValue
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Instant

/**
 * External (non-aggregate) command handlers: load/mutate JPA state and publish events through Axon's EventGateway.
 * The engine (Axon 4 or axon-thin) provides the transaction, so projections run in it as well.
 */
@Component
class TaskCommandHandlers(
    private val tasks: TaskRepository,
    private val events: EventGateway,
) {

    /** Shows the parameter resolvers in use: payload, metadata value and a Spring bean ([Clock]). */
    @CommandHandler
    fun handle(command: CreateTaskCommand, @MetaDataValue("userId") userId: String?, clock: Clock): String {
        require(command.title.isNotBlank()) { "Task title must not be blank" }
        check(!tasks.existsById(command.taskId)) { "Task ${command.taskId} already exists" }
        tasks.save(
            Task(
                id = command.taskId,
                title = command.title.trim(),
                description = command.description,
                createdAt = Instant.now(clock),
                createdBy = userId,
            ),
        )
        events.publish(TaskCreatedEvent(command.taskId, command.title.trim(), command.description, userId))
        return command.taskId
    }

    @CommandHandler
    fun handle(command: RenameTaskCommand) {
        require(command.title.isNotBlank()) { "Task title must not be blank" }
        val task = openTask(command.taskId)
        if (task.title == command.title) return
        task.title = command.title
        events.publish(TaskRenamedEvent(task.id, task.title))
    }

    /** Returns the previous status, to exercise non-Unit results through sendAllAndWait. */
    @CommandHandler
    fun handle(command: ChangeTaskStatusCommand): TaskStatus {
        val task = tasks.findById(command.taskId).orElseThrow { TaskNotFoundException(command.taskId) }
        val previous = task.status
        if (previous != command.status) {
            task.status = command.status
            events.publish(TaskStatusChangedEvent(task.id, previous, command.status))
        }
        return previous
    }

    @CommandHandler
    fun handle(command: AssignTaskCommand) {
        val task = openTask(command.taskId)
        task.assignee = command.assignee
        events.publish(TaskAssignedEvent(task.id, command.assignee))
    }

    private fun openTask(taskId: String): Task {
        val task = tasks.findById(taskId).orElseThrow { TaskNotFoundException(taskId) }
        if (task.status == TaskStatus.DONE) throw TaskClosedException(taskId)
        return task
    }
}

@Component
class CommentCommandHandlers(
    private val tasks: TaskRepository,
    private val comments: CommentRepository,
    private val events: EventGateway,
    private val clock: Clock,
) {

    @CommandHandler
    fun handle(command: AddCommentCommand): String {
        require(command.text.isNotBlank()) { "Comment text must not be blank" }
        val task = tasks.findById(command.taskId).orElseThrow { TaskNotFoundException(command.taskId) }
        if (task.status == TaskStatus.DONE) throw TaskClosedException(task.id)
        comments.save(Comment(command.commentId, task.id, command.author, command.text, Instant.now(clock)))
        events.publish(CommentAddedEvent(task.id, command.commentId, command.author, command.text))
        return command.commentId
    }

    @CommandHandler
    fun handle(command: EditCommentCommand) {
        val comment = comments.findById(command.commentId).orElseThrow { CommentNotFoundException(command.commentId) }
        comment.text = command.text
        events.publish(CommentEditedEvent(comment.taskId, comment.id, command.text))
    }

    @CommandHandler
    fun handle(command: DeleteCommentCommand) {
        val comment = comments.findById(command.commentId).orElseThrow { CommentNotFoundException(command.commentId) }
        comments.delete(comment)
        events.publish(CommentDeletedEvent(comment.taskId, comment.id))
    }
}
