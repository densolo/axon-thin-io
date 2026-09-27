package com.dc8.axonthin

import com.dc8.axonthin.aggregate.AggregateModel
import com.dc8.axonthin.aggregate.readAggregateStreams
import com.dc8.axonthin.api.BulkCommandGateway
import com.dc8.axonthin.api.BulkOptions
import com.dc8.axonthin.eventstore.ThinEventStore
import org.axonframework.commandhandling.CommandCallback
import org.axonframework.commandhandling.CommandMessage
import org.axonframework.commandhandling.CommandResultMessage
import org.axonframework.commandhandling.GenericCommandMessage
import org.axonframework.commandhandling.GenericCommandResultMessage
import org.axonframework.commandhandling.NoHandlerForCommandException
import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.common.Registration
import org.axonframework.messaging.MessageDispatchInterceptor
import org.axonframework.modelling.command.ConcurrencyException
import org.slf4j.LoggerFactory
import org.springframework.transaction.support.TransactionSynchronizationManager
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

/**
 * Axon [CommandGateway] that handles commands synchronously on the calling thread (like SimpleCommandBus),
 * each inside a Spring transaction together with the event handlers of the events it publishes.
 *
 * Every top-level dispatch is a *chunk* (`sendAndWait` = chunk of one, `sendAllAndWait` = chunk of N):
 * 1. preload: snapshots + events of all target aggregates, two queries;
 * 2. handle the commands in order against the in-memory aggregates, collecting their events;
 * 3. append all events in one batch, then hand them to event handlers (batch handlers get one list);
 * 4. commit — all or nothing. Optionally re-run on ConcurrencyException ([BulkOptions.concurrencyRetries]).
 * Commands dispatched from inside a handler join the running chunk.
 *
 * `send(...)` variants return already-completed futures; timeouts are accepted but have nothing to wait for.
 */
class ThinCommandGateway internal constructor(
    private val registry: ThinHandlerRegistry,
    private val eventGateway: ThinEventGateway,
    private val transactions: ThinTransactions,
    private val eventStore: ThinEventStore?,
    private val defaultConcurrencyRetries: Int = 0,
) : CommandGateway, BulkCommandGateway {

    private val log = LoggerFactory.getLogger(ThinCommandGateway::class.java)

    private val dispatchInterceptors = CopyOnWriteArrayList<MessageDispatchInterceptor<in CommandMessage<*>>>()

    override fun <C : Any?, R : Any?> send(command: C & Any, callback: CommandCallback<in C, in R>) {
        val message = prepare(command)
        @Suppress("UNCHECKED_CAST")
        val typed = message as CommandMessage<C>
        val result: CommandResultMessage<R> = try {
            @Suppress("UNCHECKED_CAST")
            GenericCommandResultMessage(execute(listOf(message), defaultConcurrencyRetries).single() as R)
        } catch (e: RuntimeException) {
            GenericCommandResultMessage.asCommandResultMessage(e)
        }
        callback.onResult(typed, result)
    }

    @Suppress("UNCHECKED_CAST")
    override fun <R : Any?> sendAndWait(command: Any): R =
        execute(listOf(prepare(command)), defaultConcurrencyRetries).single() as R

    override fun <R : Any?> sendAndWait(command: Any, timeout: Long, unit: TimeUnit): R = sendAndWait(command)

    override fun <R : Any?> send(command: Any): CompletableFuture<R> =
        try {
            CompletableFuture.completedFuture(sendAndWait(command))
        } catch (e: RuntimeException) {
            CompletableFuture.failedFuture(e)
        }

    override fun sendAllAndWait(commands: List<Any>, options: BulkOptions): List<Any?> =
        execute(commands.map(::prepare), options.concurrencyRetries)

    override fun registerDispatchInterceptor(
        dispatchInterceptor: MessageDispatchInterceptor<in CommandMessage<*>>,
    ): Registration {
        dispatchInterceptors += dispatchInterceptor
        return Registration { dispatchInterceptors.remove(dispatchInterceptor) }
    }

    private fun prepare(command: Any): CommandMessage<*> {
        var message: CommandMessage<*> = GenericCommandMessage.asCommandMessage<Any>(command)
        ThinUnitOfWork.currentOrNull()?.currentMessage?.let { message = message.andMetaData(correlationData(it)) }
        @Suppress("UNCHECKED_CAST")
        return dispatchInterceptors.fold(message) { msg, interceptor ->
            (interceptor as MessageDispatchInterceptor<CommandMessage<*>>).handle(msg)
        }
    }

    /**
     * Runs [commands] as one chunk (or inside the running one, when called from a handler). A chunk that owns its
     * transaction is re-run up to [retries] times on ConcurrencyException, with freshly loaded aggregates.
     */
    private fun execute(commands: List<CommandMessage<*>>, retries: Int): List<Any?> {
        if (ThinUnitOfWork.currentOrNull() != null) return ThinUnitOfWork.joinOrStart { uow, _ -> commands.map { handle(it, uow) } }
        val ownsTransaction = !TransactionSynchronizationManager.isActualTransactionActive()
        var attempt = 0
        while (true) {
            try {
                return transactions.inTransaction { ThinUnitOfWork.joinOrStart { uow, _ -> runChunk(commands, uow) } }
            } catch (e: ConcurrencyException) {
                // AggregateStreamCreationException is not a ConcurrencyException: a duplicate create is not retried
                if (!ownsTransaction || attempt >= retries) throw e
                attempt++
                val pause = retryBackoff(attempt)
                log.info(
                    "Chunk of {} command(s) conflicted with another writer ({}); retry {}/{} in {} ms",
                    commands.size, e.message, attempt, retries, pause,
                )
                Thread.sleep(pause)
            }
        }
    }

    /**
     * Exponential backoff with full jitter (5 ms, 10 ms, 20 ms … capped at 1 s): retrying immediately would collide
     * with the same writer again; jitter keeps competing chunks from retrying in lockstep.
     */
    private fun retryBackoff(attempt: Int): Long {
        val cap = minOf(1_000L, 5L shl minOf(attempt - 1, 8))
        return ThreadLocalRandom.current().nextLong(cap / 2, cap + 1)
    }

    private fun runChunk(commands: List<CommandMessage<*>>, uow: ThinUnitOfWork): List<Any?> {
        preload(commands, uow)
        uow.chunkCommands = commands
        val results = commands.mapIndexed { index, command ->
            uow.currentCommandIndex = index
            handle(command, uow)
        }
        uow.currentCommandIndex = -1
        eventGateway.flush(uow)
        return results
    }

    /** Loads every aggregate the chunk targets in two queries (snapshots, then events after them). */
    private fun preload(commands: List<CommandMessage<*>>, uow: ThinUnitOfWork) {
        val targets = LinkedHashMap<String, AggregateModel>()
        for (command in commands) {
            val handler = (registry.commandRoute(command.commandName) as? CommandRoute.Aggregate)?.handler ?: continue
            val id = handler.preloadTarget(command) ?: continue
            if ((handler.model.typeName to id) !in uow.aggregates) targets[id] = handler.model
        }
        if (targets.isEmpty()) return
        val streams = requireEventStore().readAggregateStreams(targets)
        for ((id, model) in targets) {
            val stream = streams.getValue(id)
            if (stream.isEmpty()) uow.missing += model.typeName to id
            else uow.aggregates[model.typeName to id] = model.rebuild(stream, model.newSnapshotTrigger())
        }
    }

    private fun handle(command: CommandMessage<*>, uow: ThinUnitOfWork): Any? {
        val route = registry.commandRoute(command.commandName)
            ?: throw NoHandlerForCommandException(
                "No handler was subscribed for command [${command.commandName}].",
            )
        return uow.handling(command) {
            when (route) {
                is CommandRoute.Bean -> route.handler.invoke(command)
                is CommandRoute.Aggregate -> route.handler.handle(command, uow, requireEventStore())
            }
        }
    }

    private fun requireEventStore(): ThinEventStore =
        eventStore ?: throw IllegalStateException("Aggregates need the event store (axon.thin.event-store.enabled + a DataSource)")
}
