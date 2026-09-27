package com.dc8.example.task.contract

import com.dc8.axonthin.api.ChunkContext
import com.dc8.axonthin.api.pendingEvents
import com.dc8.example.task.api.TaskCreatedEvent
import com.dc8.example.task.api.TaskRenamedEvent
import com.dc8.example.task.projection.TaskSummaryRepository
import org.axonframework.commandhandling.CommandHandler
import org.springframework.stereotype.Component
import java.util.concurrent.CopyOnWriteArrayList

data class ReserveTitleCommand(val title: String)
data class RecordChunkPositionCommand(val marker: String)

class TitleTakenException(title: String) : RuntimeException("Title '$title' is already used")

/**
 * The validation pattern for chunks: "projection + what this chunk changed so far". Engine-agnostic — on Axon 4 the
 * projection is already current and pendingEvents() is empty; on thin the chunk's own changes are pending.
 */
@Component
class TitleGuard(private val summaries: TaskSummaryRepository, private val chunk: ChunkContext) {

    @CommandHandler
    fun handle(command: ReserveTitleCommand) {
        val inProjection = summaries.findAll().any { it.title == command.title }
        val inChunk = chunk.pendingEvents<TaskCreatedEvent>().any { it.title == command.title } ||
            chunk.pendingEvents<TaskRenamedEvent>().any { it.title == command.title }
        if (inProjection || inChunk) throw TitleTakenException(command.title)
    }
}

/** Records what ChunkContext reports while a command is handled. */
@Component
class ChunkPositionRecorder(private val chunk: ChunkContext) {
    val seen = CopyOnWriteArrayList<Triple<String, Int, Int>>() // marker, index, chunk size

    @CommandHandler
    fun handle(command: RecordChunkPositionCommand) {
        seen += Triple(command.marker, chunk.currentCommandIndex(), chunk.commands().size)
    }
}
