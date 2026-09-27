package com.dc8.axonthin

import org.axonframework.eventhandling.EventMessage
import org.axonframework.messaging.Message

/**
 * Thread-bound processing scope, the thin counterpart of Axon's root UnitOfWork.
 *
 * Events published while a command (or event) is handled are queued here and dispatched to event handlers
 * only after the root command handler returned successfully, still inside the transaction — the same moment
 * Axon's SimpleEventBus hands them to subscribing processors (UnitOfWork prepare-commit phase).
 * Nested dispatches (a command sent from a handler) share the root queue, as in Axon.
 */
internal class ThinUnitOfWork private constructor() {

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

    fun nextPendingEvent(): EventMessage<*>? = pendingEvents.removeFirstOrNull()

    companion object {
        private val current = ThreadLocal<ThinUnitOfWork>()

        fun currentOrNull(): ThinUnitOfWork? = current.get()

        /** Joins the active unit of work, or starts a root one; [block] receives `true` when it owns the root. */
        fun <T> joinOrStart(block: (uow: ThinUnitOfWork, isRoot: Boolean) -> T): T {
            current.get()?.let { return block(it, false) }
            val root = ThinUnitOfWork()
            current.set(root)
            try {
                return block(root, true)
            } finally {
                current.remove()
            }
        }
    }
}
