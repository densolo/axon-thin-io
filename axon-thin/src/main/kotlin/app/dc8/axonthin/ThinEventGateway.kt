package app.dc8.axonthin

import app.dc8.axonthin.eventstore.ThinEventStore
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
     * Stores all queued events in one batch, then hands them to event handlers (see [dispatch]).
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
     * Hands [events] to [beans], bean by bean, **in event order**: consecutive events that go to the same batch handler
     * (`List<…>` parameter) are delivered as one call; a single-event handler gets one call per event. So a bean mixing
     * single and batch handlers still sees events in their original order, and bulk operations (long runs of events
     * for one handler) arrive as large batches. Events a bean does not handle do not interrupt its runs.
     * Shared by live chunks and projection replays.
     */
    internal fun dispatch(
        beans: List<ThinHandlerRegistry.EventHandlingBean>,
        events: List<EventMessage<*>>,
        uow: ThinUnitOfWork,
        mode: ThinAxonProperties.EventHandlerErrorMode,
    ) {
        for (bean in beans) {
            var runHandler: HandlerMethod? = null
            val run = ArrayList<EventMessage<*>>()
            fun flushRun() {
                val handler = runHandler ?: return
                val batch = run.toList()
                uow.handling(batch.last()) { guarded(mode, handler, "${batch.size} events") { handler.invokeBatch(batch) } }
                runHandler = null
                run.clear()
            }
            for (event in events) {
                val handler = bean.handlerFor(event.payloadType) ?: continue
                if (handler.isBatch) {
                    if (handler !== runHandler) flushRun()
                    runHandler = handler
                    run += event
                } else {
                    flushRun()
                    uow.handling(event) { guarded(mode, handler, event.identifier) { handler.invoke(event) } }
                }
            }
            flushRun()
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
