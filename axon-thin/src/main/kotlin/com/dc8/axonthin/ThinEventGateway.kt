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
     * Stores, then dispatches all queued events in publication order. Events published by the handlers themselves
     * are queued and processed in the next round.
     */
    internal fun flush(uow: ThinUnitOfWork) {
        while (true) {
            val events = uow.drainPendingEvents()
            if (events.isEmpty()) return
            eventStore?.append(events)
            for (event in events) {
                uow.handling(event) {
                    for (handler in registry.eventHandlers(event.payloadType)) {
                        invoke(handler, event)
                    }
                }
            }
        }
    }

    private fun invoke(handler: HandlerMethod, event: EventMessage<*>) {
        try {
            handler.invoke(event)
        } catch (e: RuntimeException) {
            when (errorMode) {
                ThinAxonProperties.EventHandlerErrorMode.PROPAGATE -> throw e
                ThinAxonProperties.EventHandlerErrorMode.LOG ->
                    log.error("EventListener [{}] failed to handle event [{}]; continuing", handler, event.identifier, e)
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
