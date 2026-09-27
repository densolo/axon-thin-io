package com.dc8.axonthin

import com.dc8.axonthin.eventstore.ThinEventStore
import org.axonframework.common.Registration
import org.axonframework.eventhandling.EventMessage
import org.axonframework.eventhandling.GenericEventMessage
import org.axonframework.eventhandling.gateway.EventGateway
import org.axonframework.messaging.MessageDispatchInterceptor
import org.slf4j.LoggerFactory
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Axon [EventGateway] with synchronous dispatch — the equivalent of Axon's EmbeddedEventStore (or SimpleEventBus when
 * the store is disabled) with subscribing event processors: events are appended, then handed to event handlers,
 * in the same transaction.
 */
class ThinEventGateway internal constructor(
    private val registry: ThinHandlerRegistry,
    private val transactions: ThinTransactions,
    private val errorMode: ThinAxonProperties.EventHandlerErrorMode,
    private val eventStore: ThinEventStore?,
) : EventGateway {

    private val log = LoggerFactory.getLogger(ThinEventGateway::class.java)
    private val dispatchInterceptors = CopyOnWriteArrayList<MessageDispatchInterceptor<in EventMessage<*>>>()

    override fun publish(events: List<*>) {
        if (events.isEmpty()) return
        val messages = events.map { GenericEventMessage.asEventMessage<Any>(it!!) }
        ThinUnitOfWork.joinOrStart { uow, isRoot ->
            if (isRoot) {
                // Published outside any handler: behave like a subscribing processor's own unit of work.
                transactions.inTransaction {
                    messages.forEach { uow.queue(intercept(it)) }
                    flush(uow)
                }
            } else {
                messages.forEach { uow.queue(intercept(correlate(it, uow))) }
            }
        }
    }

    override fun registerDispatchInterceptor(
        dispatchInterceptor: MessageDispatchInterceptor<in EventMessage<*>>,
    ): Registration {
        dispatchInterceptors += dispatchInterceptor
        return Registration { dispatchInterceptors.remove(dispatchInterceptor) }
    }

    /**
     * Stores all queued events in one batch, then hands them to event handlers — per bean, in `@Order` order:
     * - a single-event handler is invoked once per event it handles, in publication order;
     * - a batch handler (`List<…>` parameter) is invoked once with all events it handles, in publication order.
     * (Within one bean, single-event handlers run before its batch handlers; don't mix both for related events.)
     * Events published by the handlers themselves are stored and dispatched in the next round.
     */
    internal fun flush(uow: ThinUnitOfWork) {
        while (true) {
            val events = uow.drainPendingEvents()
            if (events.isEmpty()) return
            eventStore?.append(events)
            dispatch(registry.eventHandlingBeans, events, uow, errorMode)
        }
    }

    /**
     * Hands [events] to [beans], bean by bean in order: single-event handlers once per event, batch handlers once with
     * all events they handle (both in the order of [events]). Shared by live chunks and projection replays.
     */
    internal fun dispatch(
        beans: List<ThinHandlerRegistry.EventHandlingBean>,
        events: List<EventMessage<*>>,
        uow: ThinUnitOfWork,
        mode: ThinAxonProperties.EventHandlerErrorMode,
    ) {
        for (bean in beans) {
            val batches = LinkedHashMap<HandlerMethod, MutableList<EventMessage<*>>>()
            for (event in events) {
                val handler = bean.handlerFor(event.payloadType) ?: continue
                if (handler.isBatch) batches.getOrPut(handler) { ArrayList() } += event
                else uow.handling(event) { guarded(mode, handler, event.identifier) { handler.invoke(event) } }
            }
            for ((handler, batch) in batches) {
                uow.handling(batch.last()) {
                    guarded(mode, handler, "${batch.size} events") { handler.invokeBatch(batch) }
                }
            }
        }
    }

    private inline fun guarded(
        mode: ThinAxonProperties.EventHandlerErrorMode,
        handler: HandlerMethod,
        what: String,
        block: () -> Unit,
    ) {
        try {
            block()
        } catch (e: RuntimeException) {
            when (mode) {
                ThinAxonProperties.EventHandlerErrorMode.PROPAGATE -> throw e
                ThinAxonProperties.EventHandlerErrorMode.LOG ->
                    log.error("EventListener [{}] failed to handle event [{}]; continuing", handler, what, e)
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun intercept(event: EventMessage<*>): EventMessage<*> =
        dispatchInterceptors.fold(event) { message, interceptor ->
            (interceptor as MessageDispatchInterceptor<EventMessage<*>>).handle(message)
        }

    private fun correlate(event: EventMessage<*>, uow: ThinUnitOfWork): EventMessage<*> =
        uow.currentMessage?.let { event.andMetaData(correlationData(it)) } ?: event
}
