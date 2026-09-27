package com.dc8.axonthin.v4

import com.dc8.axonthin.api.ChunkContext
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork

/**
 * [ChunkContext] on Axon 4: projections are updated after every command (subscribing processors in the command's
 * unit of work), so nothing is ever pending and validation against projections alone is already correct.
 * Chunk commands are those of the running [Axon4BulkCommandGateway.sendAllAndWait], or the single command.
 */
class Axon4ChunkContext : ChunkContext {

    override val active: Boolean get() = CurrentUnitOfWork.isStarted()

    override fun pendingEvents(): List<Any> = emptyList()

    override fun commands(): List<Any> =
        chunk.get()?.commands ?: CurrentUnitOfWork.map { listOf<Any>(it.message.payload) }.orElse(emptyList())

    override fun currentCommandIndex(): Int = chunk.get()?.index ?: if (CurrentUnitOfWork.isStarted()) 0 else -1

    override fun <T : Any> aggregate(type: Class<T>, id: String): T? = null

    internal class Chunk(val commands: List<Any>) {
        var index = -1
    }

    internal companion object {
        val chunk = ThreadLocal<Chunk>()
    }
}
