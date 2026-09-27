package com.dc8.example.task.command

import com.dc8.example.task.api.CreateTaskCommand
import com.dc8.example.task.api.CreateTasksFromTemplateCommand
import com.dc8.example.task.api.TasksImportedEvent
import org.axonframework.commandhandling.CommandHandler
import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.eventhandling.gateway.EventGateway
import org.springframework.stereotype.Component
import java.util.UUID

/**
 * External (non-aggregate) command handler: orchestrates nested commands to the Task aggregate and publishes a
 * non-aggregate event. Everything runs in the caller's transaction.
 */
@Component
class TaskTemplateCommandHandler(private val events: EventGateway) {

    /** The gateway is resolved as a handler parameter (Spring bean injection). */
    @CommandHandler
    fun handle(command: CreateTasksFromTemplateCommand, commands: CommandGateway): List<String> {
        require(command.titles.isNotEmpty()) { "Template has no tasks" }
        val ids = command.titles.map { title ->
            commands.sendAndWait<String>(CreateTaskCommand(UUID.randomUUID().toString(), "${command.prefix} $title"))
        }
        events.publish(TasksImportedEvent(ids.size, "template:${command.prefix}"))
        return ids
    }
}
