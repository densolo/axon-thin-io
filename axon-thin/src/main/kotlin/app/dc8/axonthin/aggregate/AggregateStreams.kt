package app.dc8.axonthin.aggregate

import app.dc8.axonthin.eventstore.ThinEventStore
import org.axonframework.eventhandling.DomainEventMessage

/** One aggregate's stream: latest usable snapshot (if any) followed by the events after it. */
internal fun ThinEventStore.readAggregateStream(model: AggregateModel, aggregateId: String): List<DomainEventMessage<*>> =
    readAggregateStreams(mapOf(aggregateId to model)).getValue(aggregateId)

/**
 * Streams of many aggregates in two queries (snapshots, then events after each snapshot) — Axon's
 * AbstractEventStore.readEvents, batched. A snapshot whose payload is not the aggregate's type is ignored.
 * Every requested id is in the result; an empty list means the aggregate does not exist.
 */
internal fun ThinEventStore.readAggregateStreams(
    targets: Map<String, AggregateModel>,
): Map<String, List<DomainEventMessage<*>>> {
    if (targets.isEmpty()) return emptyMap()
    val snapshots = readSnapshots(targets.keys).filter { (id, snapshot) -> targets.getValue(id).rootType.isInstance(snapshot.payload) }
    val events = readEventsAfter(targets.keys.associateWith { snapshots[it]?.sequenceNumber ?: -1L })
    return targets.keys.associateWith { id -> listOfNotNull(snapshots[id]) + events[id].orEmpty() }
}
