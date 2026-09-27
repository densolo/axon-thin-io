package com.dc8.axonthin

import com.dc8.axonthin.aggregate.ThinAggregate
import org.axonframework.eventhandling.EventMessage
import org.axonframework.messaging.Message

/**
 * Thread-bound processing scope, the thin counterpart of Axon's root UnitOfWork. It is not exposed as Axon's
 * `UnitOfWork` API; it exists because a transaction alone does not give:
 *
 * - **deferred events**: events published while a command is handled are stored and dispatched to event handlers
 *   only after the root handler returned — Axon's prepare-commit moment — still inside the transaction;
 * - **aggregate identity map**: an aggregate is loaded once and shared by nested commands, so sequence numbers
 *   stay consistent. Inside `sendAllAndWait` the map spans the whole batch (see [inBatch]);
 * - **correlation**: the message being handled is the source of `correlationId`/`traceId` for anything it publishes;
 * - **nested failure isolation**: events queued by a failed nested dispatch are discarded.
 */
internal class ThinUnitOfWork private constructor(
    /** Aggregates by (type, id); owned by the batch when running inside `sendAllAndWait`. */
    val aggregates: MutableMap<Pair<String, String>, ThinAggregate>,
) {

    private val pendingEvents = ArrayDeque<EventMessage<*>>()
    private val messages = ArrayDeque<Message<*>>()

    /** The message currently being handled; source of correlation data for anything it publishes. */
    val currentMessage: Message<*>? get() = messages.lastOrNull()

    fun queue(event: EventMessage<*>) {
        pendingEvents.addLast(event)
    }

    /** Runs [block] while [message] is the current message; events queued by a failing block are discarded. */
    fun <T> handling(message: Message<*>, block: () -> T): T {
        val queuedBefore = pendingEvents.size
        messages.addLast(message)
        try {
            return block()
        } catch (e: Throwable) {
            while (pendingEvents.size > queuedBefore) pendingEvents.removeLast()
            throw e
        } finally {
            messages.removeLast()
        }
    }

    /** Takes all events queued so far (handlers may queue more while these are dispatched). */
    fun drainPendingEvents(): List<EventMessage<*>> = pendingEvents.toList().also { pendingEvents.clear() }

    companion object {
        private val current = ThreadLocal<ThinUnitOfWork>()
        private val batchAggregates = ThreadLocal<MutableMap<Pair<String, String>, ThinAggregate>>()

        fun currentOrNull(): ThinUnitOfWork? = current.get()

        /** Joins the active unit of work, or starts a root one; [block] receives `true` when it owns the root. */
        fun <T> joinOrStart(block: (uow: ThinUnitOfWork, isRoot: Boolean) -> T): T {
            current.get()?.let { return block(it, false) }
            val root = ThinUnitOfWork(batchAggregates.get() ?: HashMap())
            current.set(root)
            try {
                return block(root, true)
            } finally {
                current.remove()
            }
        }

        /**
         * Shares one aggregate identity map between all root units of work started by [block]: a batch loads each
         * aggregate once. Safe because the batch is one transaction — any failure rolls everything back.
         */
        fun <T> inBatch(block: () -> T): T {
            if (batchAggregates.get() != null) return block()
            batchAggregates.set(HashMap())
            try {
                return block()
            } finally {
                batchAggregates.remove()
            }
        }
    }
}
