package app.dc8.axonthin.eventstore

import app.dc8.axonthin.ThinEventGateway
import app.dc8.axonthin.ThinUnitOfWork
import org.axonframework.common.Registration
import org.axonframework.eventhandling.DomainEventMessage
import org.axonframework.eventhandling.EventMessage
import org.axonframework.eventhandling.TrackedEventMessage
import org.axonframework.eventhandling.TrackingToken
import org.axonframework.eventsourcing.eventstore.DomainEventStream
import org.axonframework.eventsourcing.eventstore.EventStore
import org.axonframework.messaging.MessageDispatchInterceptor
import org.axonframework.common.stream.BlockingStream
import java.util.Optional
import java.util.function.Consumer
import kotlin.streams.asStream

/**
 * Axon's [EventStore] interface on top of thin, for application code that uses it directly — reading an aggregate's
 * stream (e.g. a debug page) or publishing domain events that bypass the aggregate.
 *
 * - [readEvents] (aggregate): like Axon's EmbeddedEventStore — the latest snapshot first (if any), then the events
 *   after it, then events of that aggregate staged in the running chunk. Read lazily, [PAGE_SIZE] rows at a time.
 * - [readEvents] (aggregate, first sequence): events from that sequence, no snapshot, plus staged ones.
 * - [publish]: the EventGateway path — a `DomainEventMessage` is stored exactly as given (aggregate id, sequence
 *   number, type) and dispatched like any event (inside the running chunk, or in its own transaction). A sequence
 *   number already taken fails with ConcurrencyException / AggregateStreamCreationException, as on Axon.
 * - [storeSnapshot], [lastSequenceNumberFor]: as in Axon.
 * - Tracking ([openStream], tokens) and [subscribe]: not supported — thin has no tracking processors.
 */
class ThinAxonEventStore internal constructor(
    private val store: ThinEventStore,
    private val gateway: ThinEventGateway,
) : EventStore {

    override fun readEvents(aggregateIdentifier: String): DomainEventStream {
        val snapshot = store.readSnapshot(aggregateIdentifier)
        val from = snapshot?.let { it.sequenceNumber + 1 } ?: 0
        val events = DomainEventStream.concat(lazyEvents(aggregateIdentifier, from), staged(aggregateIdentifier, from))
        return if (snapshot == null) events else DomainEventStream.concat(DomainEventStream.of(snapshot), events)
    }

    override fun readEvents(aggregateIdentifier: String, firstSequenceNumber: Long): DomainEventStream =
        DomainEventStream.concat(lazyEvents(aggregateIdentifier, firstSequenceNumber), staged(aggregateIdentifier, firstSequenceNumber))

    override fun publish(events: List<EventMessage<*>>) = gateway.publish(events)

    override fun storeSnapshot(snapshot: DomainEventMessage<*>) = store.storeSnapshot(snapshot)

    override fun lastSequenceNumberFor(aggregateIdentifier: String): Optional<Long> {
        val stagedMax = staged(aggregateIdentifier, 0).asStream().map { it.sequenceNumber }.reduce(::maxOf)
        val storedMax = Optional.ofNullable(store.lastSequenceNumber(aggregateIdentifier))
        return listOf(stagedMax, storedMax).filter { it.isPresent }.map { it.get() }.maxOrNull().let { Optional.ofNullable(it) }
    }

    override fun registerDispatchInterceptor(dispatchInterceptor: MessageDispatchInterceptor<in EventMessage<*>>): Registration =
        gateway.registerDispatchInterceptor(dispatchInterceptor)

    override fun subscribe(messageProcessor: Consumer<MutableList<out EventMessage<*>>>): Registration =
        unsupported("subscribe (event handlers are Spring beans with @EventHandler)")

    override fun openStream(trackingToken: TrackingToken?): BlockingStream<TrackedEventMessage<*>> =
        unsupported("openStream (no tracking processors)")

    override fun createTailToken(): TrackingToken = unsupported("tracking tokens")

    override fun createHeadToken(): TrackingToken = unsupported("tracking tokens")

    override fun createTokenAt(dateTime: java.time.Instant): TrackingToken = unsupported("tracking tokens")

    /** Stored events from [from], fetched one page at a time as the stream is consumed. */
    private fun lazyEvents(aggregateIdentifier: String, from: Long): DomainEventStream {
        val pages = generateSequence(store.readEvents(aggregateIdentifier, from, PAGE_SIZE)) { page ->
            if (page.size < PAGE_SIZE) null
            else store.readEvents(aggregateIdentifier, page.last().sequenceNumber + 1, PAGE_SIZE).takeIf { it.isNotEmpty() }
        }
        return DomainEventStream.of(pages.flatten().asStream())
    }

    /** Events of the aggregate applied in the running chunk, not yet stored (Axon: "staged" events of the unit of work). */
    private fun staged(aggregateIdentifier: String, from: Long): DomainEventStream =
        DomainEventStream.of(
            ThinUnitOfWork.currentOrNull()?.pendingEventsSnapshot.orEmpty()
                .filterIsInstance<DomainEventMessage<*>>()
                .filter { it.aggregateIdentifier == aggregateIdentifier && it.sequenceNumber >= from },
        )

    private fun unsupported(what: String): Nothing =
        throw UnsupportedOperationException("axon-thin does not support EventStore.$what")

    private companion object {
        const val PAGE_SIZE = 100
    }
}
