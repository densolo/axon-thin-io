package app.dc8.example.task

import app.dc8.axonthin.api.BulkCommandGateway
import app.dc8.example.task.api.ChangeTaskStatusCommand
import app.dc8.example.task.api.CreateTaskCommand
import app.dc8.example.task.api.ImportTaskCommand
import app.dc8.example.task.api.TaskStatus
import org.axonframework.commandhandling.gateway.CommandGateway
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

/** Application service: the typical caller of the gateways. */
@Service
class TaskService(
    private val commandGateway: CommandGateway,
    private val bulk: BulkCommandGateway,
) {
    fun create(title: String, description: String? = null): String =
        commandGateway.sendAndWait(CreateTaskCommand(UUID.randomUUID().toString(), title, description))

    /** Bulk import: all tasks or none. */
    fun importTasks(titles: List<String>): List<String> =
        bulk.sendAllAndWait(titles.map { CreateTaskCommand(UUID.randomUUID().toString(), it) }).map { it as String }

    /** Bulk upsert (CREATE_IF_MISSING): creates missing tasks, renames existing ones. */
    fun upsertTasks(titlesById: Map<String, String>): List<String> =
        bulk.sendAllAndWait(titlesById.map { (id, title) -> ImportTaskCommand(id, title) }).map { it as String }

    /** Joins the caller's transaction: sendAllAndWait participates instead of opening its own. */
    @Transactional
    fun closeAll(taskIds: List<String>): Map<String, TaskStatus> {
        val previous = bulk.sendAllAndWait(taskIds.map { ChangeTaskStatusCommand(it, TaskStatus.DONE) })
        return taskIds.zip(previous.map { it as TaskStatus }).toMap()
    }
}
