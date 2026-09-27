package com.dc8.example.task

import com.dc8.axonthin.api.BulkCommandGateway
import com.dc8.example.task.api.ChangeTaskStatusCommand
import com.dc8.example.task.api.CreateTaskCommand
import com.dc8.example.task.api.TaskStatus
import org.axonframework.commandhandling.gateway.CommandGateway
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Clock
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

    /** Joins the caller's transaction: sendAllAndWait participates instead of opening its own. */
    @Transactional
    fun closeAll(taskIds: List<String>): Map<String, TaskStatus> {
        val previous = bulk.sendAllAndWait(taskIds.map { ChangeTaskStatusCommand(it, TaskStatus.DONE) })
        return taskIds.zip(previous.map { it as TaskStatus }).toMap()
    }
}

@Configuration(proxyBeanMethods = false)
class TaskModelConfiguration {

    @Bean
    fun clock(): Clock = Clock.systemUTC()
}
