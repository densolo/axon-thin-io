package com.dc8.axonthin.eventstore

import org.axonframework.common.DateTimeUtils
import org.axonframework.eventhandling.DomainEventMessage
import org.axonframework.eventhandling.EventMessage
import org.axonframework.eventhandling.GenericDomainEventMessage
import org.axonframework.messaging.GenericMessage
import org.axonframework.messaging.MetaData
import org.axonframework.modelling.command.AggregateStreamCreationException
import org.axonframework.modelling.command.ConcurrencyException
import org.axonframework.eventsourcing.eventstore.EventStoreException
import org.axonframework.serialization.Serializer
import org.axonframework.serialization.SimpleSerializedObject
import org.axonframework.serialization.SimpleSerializedType
import org.springframework.dao.DataAccessException
import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.JdbcTemplate
import java.sql.ResultSet
import java.util.function.Supplier

/**
 * Reads and appends events in Axon 4's `domain_event_entry` layout, row-for-row what Axon's
 * JpaEventStorageEngine writes (see AbstractEventEntry / AbstractDomainEventEntry):
 *
 * | column               | value                                                                |
 * |----------------------|----------------------------------------------------------------------|
 * | global_index         | pooled sequence / identity                                           |
 * | event_identifier     | message id                                                           |
 * | payload_type         | serialized type name (class name)                                    |
 * | payload_revision     | `@Revision` or null                                                  |
 * | payload, meta_data   | serializer output (bytes)                                            |
 * | time_stamp           | `DateTimeUtils.formatInstant` (ISO-8601, millis)                     |
 * | aggregate_identifier | aggregate id; for non-aggregate events the event id                  |
 * | sequence_number      | aggregate sequence; 0 for non-aggregate events                       |
 * | type                 | aggregate type; null for non-aggregate events                        |
 *
 * Duplicate keys are translated like Axon: first event at sequence 0 → [AggregateStreamCreationException],
 * otherwise [ConcurrencyException].
 */
class ThinEventStore internal constructor(
    private val jdbc: JdbcTemplate,
    private val serializer: Serializer,
    private val table: String,
    private val globalIndex: GlobalIndexAllocator,
    private val storeNonAggregateEvents: Boolean,
) {
    private val metaDataType = SimpleSerializedType(MetaData::class.java.name, null)

    private val withIndexSql = "insert into $table (global_index, event_identifier, payload_type, payload_revision, " +
        "payload, meta_data, time_stamp, aggregate_identifier, sequence_number, type) values (?,?,?,?,?,?,?,?,?,?)"
    private val identitySql = "insert into $table (event_identifier, payload_type, payload_revision, " +
        "payload, meta_data, time_stamp, aggregate_identifier, sequence_number, type) values (?,?,?,?,?,?,?,?,?)"
    private val readSql = "select event_identifier, payload_type, payload_revision, payload, meta_data, time_stamp, " +
        "aggregate_identifier, sequence_number, type from $table where aggregate_identifier = ? " +
        "and sequence_number >= ? order by sequence_number asc"

    internal fun append(events: List<EventMessage<*>>) {
        val domainEvents = events.mapNotNull { event ->
            when {
                event is DomainEventMessage<*> -> event
                storeNonAggregateEvents -> GenericDomainEventMessage(null, event.identifier, 0L, event, event::getTimestamp)
                else -> null
            }
        }
        if (domainEvents.isEmpty()) return
        val rows = domainEvents.map { event ->
            val payload = event.serializePayload(serializer, ByteArray::class.java)
            val metaData = event.serializeMetaData(serializer, ByteArray::class.java)
            val values = listOf(
                event.identifier,
                payload.type.name,
                payload.type.revision,
                payload.data,
                metaData.data,
                DateTimeUtils.formatInstant(event.timestamp),
                event.aggregateIdentifier,
                event.sequenceNumber,
                event.type,
            )
            val index = globalIndex.next()
            (if (index != null) listOf(index) + values else values).toTypedArray()
        }
        try {
            jdbc.batchUpdate(if (globalIndex === GlobalIndexAllocator.IDENTITY) identitySql else withIndexSql, rows)
        } catch (e: DuplicateKeyException) {
            val first = domainEvents.first()
            val description = "An event for aggregate [${first.aggregateIdentifier}] at sequence " +
                "[${first.sequenceNumber}] was already inserted"
            if (first.sequenceNumber == 0L) throw AggregateStreamCreationException(description, e)
            throw ConcurrencyException(description, e)
        } catch (e: DataAccessException) {
            throw EventStoreException("An event for aggregate [${domainEvents.first().aggregateIdentifier}] could not be stored", e)
        }
    }

    internal fun readEvents(aggregateIdentifier: String, fromSequence: Long = 0): List<DomainEventMessage<*>> =
        jdbc.query(readSql, { rs, _ -> toMessage(rs) }, aggregateIdentifier, fromSequence)

    private fun toMessage(rs: ResultSet): DomainEventMessage<*> {
        val payloadBytes = rs.getBytes("payload")
        val payloadType = SimpleSerializedType(rs.getString("payload_type"), rs.getString("payload_revision"))
        val metaDataBytes = rs.getBytes("meta_data")
        val payload: Any = serializer.deserialize(SimpleSerializedObject(payloadBytes, ByteArray::class.java, payloadType))
        val metaData: MetaData = metaDataBytes
            ?.let { serializer.deserialize<ByteArray, MetaData>(SimpleSerializedObject(it, ByteArray::class.java, metaDataType)) }
            ?: MetaData.emptyInstance()
        val timestamp = DateTimeUtils.parseInstant(rs.getString("time_stamp"))
        return GenericDomainEventMessage(
            rs.getString("type"),
            rs.getString("aggregate_identifier"),
            rs.getLong("sequence_number"),
            GenericMessage(rs.getString("event_identifier"), payload, metaData),
            Supplier { timestamp },
        )
    }
}
