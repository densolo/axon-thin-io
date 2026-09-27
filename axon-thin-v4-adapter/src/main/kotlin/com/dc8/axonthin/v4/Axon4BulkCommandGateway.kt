package com.dc8.axonthin.v4

import com.dc8.axonthin.api.BulkCommandGateway
import com.dc8.axonthin.api.BulkOptions
import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.messaging.Message
import org.axonframework.modelling.command.ConcurrencyException
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate

/**
 * [BulkCommandGateway] on top of the real Axon 4 engine: wraps sequential `sendAndWait` calls in one Spring
 * transaction. Axon's `SpringTransactionManager` uses PROPAGATION_REQUIRED, so every command's unit of work
 * (and its subscribing event processors) joins the outer transaction.
 *
 * Requires a local, synchronous command bus (SimpleCommandBus) — a distributed/async bus would escape the transaction.
 */
class Axon4BulkCommandGateway(
    private val commandGateway: CommandGateway,
    transactionManager: PlatformTransactionManager,
) : BulkCommandGateway {

    private val transaction = TransactionTemplate(transactionManager)

    override fun sendAllAndWait(commands: List<Any>, options: BulkOptions): List<Any?> {
        val ownsTransaction = !TransactionSynchronizationManager.isActualTransactionActive()
        var attempt = 0
        while (true) {
            try {
                return transaction.execute { runChunk(commands) }!!
            } catch (e: ConcurrencyException) {
                if (!ownsTransaction || attempt >= options.concurrencyRetries) throw e
                attempt++
            }
        }
    }

    private fun runChunk(commands: List<Any>): List<Any?> {
        val chunk = Axon4ChunkContext.Chunk(commands.map { (it as? Message<*>)?.payload ?: it })
        val previous = Axon4ChunkContext.chunk.get()
        Axon4ChunkContext.chunk.set(chunk)
        try {
            return commands.mapIndexed { index, command ->
                chunk.index = index
                commandGateway.sendAndWait<Any?>(command)
            }
        } finally {
            if (previous == null) Axon4ChunkContext.chunk.remove() else Axon4ChunkContext.chunk.set(previous)
        }
    }
}
