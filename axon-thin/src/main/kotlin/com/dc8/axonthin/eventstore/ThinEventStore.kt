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
import org.slf4j.LoggerFactory
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
    private val snapshotTable: String,
    private val globalIndex: GlobalIndexAllocator,
    private val storeNonAggregateEvents: Boolean,
) {
    private val log = LoggerFactory.getLogger(ThinEventStore::class.java)

    private val metaDataType = SimpleSerializedType(MetaData::class.java.name, null)

    private val withIndexSql = "insert into $table (global_index, event_identifier, payload_type, payload_revision, " +
        "payload, meta_data, time_stamp, aggregate_identifier, sequence_number, type) values (?,?,?,?,?,?,?,?,?,?)"
    private val identitySql = "insert into $table (event_identifier, payload_type, payload_revision, " +
        "payload, meta_data, time_stamp, aggregate_identifier, sequence_number, type) values (?,?,?,?,?,?,?,?,?)"
    private val columns = "event_identifier, payload_type, payload_revision, payload, meta_data, time_stamp, " +
        "aggregate_identifier, sequence_number, type"
    private val snapshotDeleteSql = "delete from $snapshotTable where aggregate_identifier = ? and sequence_number < ?"
    private val snapshotInsertSql = "insert into $snapshotTable (event_identifier, payload_type, payload_revision, " +
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
            values
        }.let { rows ->
            val indexes = globalIndex.allocate(rows.size) // one round trip for the whole chunk
            if (indexes == null) rows.map { it.toTypedArray() }
            else rows.mapIndexed { i, values -> (listOf(indexes[i]) + values).toTypedArray() }
        }
        try {
            jdbc.batchUpdate(if (globalIndex === GlobalIndexAllocator.IDENTITY) identitySql else withIndexSql, rows)
        } catch (e: DuplicateKeyException) {
            // a chunk appends many aggregates: blame the one the database names (PG/H2 print the key), else the first
            val detail = e.mostSpecificCause.message.orEmpty()
            val failed = domainEvents.firstOrNull { detail.contains(it.aggregateIdentifier) } ?: domainEvents.first()
            val description = "An event for aggregate [${failed.aggregateIdentifier}] at sequence " +
                "[${failed.sequenceNumber}] was already inserted"
            if (failed.sequenceNumber == 0L) throw AggregateStreamCreationException(description, e)
            throw ConcurrencyException(description, e)
        } catch (e: DataAccessException) {
            throw EventStoreException("An event for aggregate [${domainEvents.first().aggregateIdentifier}] could not be stored", e)
        }
    }

    internal fun readEvents(aggregateIdentifier: String, fromSequence: Long = 0): List<DomainEventMessage<*>> =
        jdbc.query(readSql, { rs, _ -> toMessage(rs) }, aggregateIdentifier, fromSequence)

    /**
     * Events of many aggregates, each after its own sequence number (e.g. its snapshot), in one query per
     * [MAX_IDS_PER_QUERY] aggregates: `join (values (?, ?), …) v(aggregate_identifier, after_seq)`.
     */
    internal fun readEventsAfter(afterSequence: Map<String, Long>): Map<String, List<DomainEventMessage<*>>> {
        val result = HashMap<String, MutableList<DomainEventMessage<*>>>()
        afterSequence.entries.chunked(MAX_IDS_PER_QUERY).forEach { part ->
            val values = part.joinToString(", ") { "(?, ?)" }
            val sql = "select ${columns.split(", ").joinToString(", ") { "e.$it" }} from $table e " +
                "join (values $values) as v(aid, after_seq) " +
                "on e.aggregate_identifier = v.aid and e.sequence_number > v.after_seq " +
                "order by e.aggregate_identifier, e.sequence_number"
            val args = part.flatMap { listOf<Any>(it.key, it.value) }.toTypedArray()
            jdbc.query(sql, { rs, _ -> toMessage(rs) }, *args)
                .forEach { result.getOrPut(it.aggregateIdentifier) { ArrayList() } += it }
        }
        return result
    }

    /** Every payload type stored (one scan; an index on `payload_type` makes it an index-only scan). */
    internal fun distinctPayloadTypes(): List<String> =
        jdbc.queryForList("select distinct payload_type from $table", String::class.java)

    /**
     * One replay page: events of [payloadTypes] in `(aggregate_identifier, sequence_number)` order, after [after]
     * (exclusive; `null` = from the start). Keyset pagination on the unique index: every page is a range scan.
     * Per-aggregate order is guaranteed and the order is the same on every run.
     */
    internal fun readReplayPage(payloadTypes: Collection<String>, after: Pair<String, Long>?, limit: Int): List<DomainEventMessage<*>> {
        if (payloadTypes.isEmpty()) return emptyList()
        val types = payloadTypes.joinToString(", ") { "?" }
        val keyset = if (after == null) "" else "and (aggregate_identifier > ? or (aggregate_identifier = ? and sequence_number > ?)) "
        val sql = "select $columns from $table where payload_type in ($types) $keyset" +
            "order by aggregate_identifier, sequence_number limit $limit"
        val args: List<Any> = payloadTypes.toList() + (after?.let { listOf<Any>(it.first, it.first, it.second) } ?: emptyList())
        return jdbc.query(sql, { rs, _ -> toMessage(rs) }, *args.toTypedArray())
    }

    /** Latest readable snapshot of one aggregate. */
    internal fun readSnapshot(aggregateIdentifier: String): DomainEventMessage<*>? =
        readSnapshots(listOf(aggregateIdentifier))[aggregateIdentifier]

    /**
     * Latest snapshot per aggregate that can be deserialized (Axon: AbstractEventStorageEngine.readSnapshot), in one
     * query per [MAX_IDS_PER_QUERY] aggregates. A snapshot that fails to deserialize is skipped with a warning, so the
     * aggregate is rebuilt from the full stream (or from an older snapshot).
     */
    internal fun readSnapshots(aggregateIdentifiers: Collection<String>): Map<String, DomainEventMessage<*>> {
        val result = HashMap<String, DomainEventMessage<*>>()
        aggregateIdentifiers.distinct().chunked(MAX_IDS_PER_QUERY).forEach { part ->
            val sql = "select $columns from $snapshotTable where aggregate_identifier in " +
                "(${part.joinToString(", ") { "?" }}) order by aggregate_identifier, sequence_number desc"
            val rows = jdbc.query(sql, { rs, _ -> SnapshotRow(rs) }, *part.toTypedArray())
            for (row in rows) {
                if (row.aggregateIdentifier in result) continue
                try {
                    result[row.aggregateIdentifier] = row.toMessage()
                } catch (e: Exception) {
                    log.warn("Error reading snapshot for aggregate [{}]. Reconstructing from entire event stream.", row.aggregateIdentifier, e)
                } catch (e: LinkageError) {
                    log.warn("Error reading snapshot for aggregate [{}]. Reconstructing from entire event stream.", row.aggregateIdentifier, e)
                }
            }
        }
        return result
    }

    /** Axon's JpaEventStorageEngine.storeSnapshot: drop older snapshots of the aggregate, insert the new one. */
    internal fun storeSnapshot(snapshot: DomainEventMessage<*>) {
        val payload = snapshot.serializePayload(serializer, ByteArray::class.java)
        val metaData = snapshot.serializeMetaData(serializer, ByteArray::class.java)
        try {
            jdbc.update(snapshotDeleteSql, snapshot.aggregateIdentifier, snapshot.sequenceNumber)
            jdbc.update(
                snapshotInsertSql,
                snapshot.identifier, payload.type.name, payload.type.revision, payload.data, metaData.data,
                DateTimeUtils.formatInstant(snapshot.timestamp), snapshot.aggregateIdentifier, snapshot.sequenceNumber,
                snapshot.type,
            )
        } catch (e: DuplicateKeyException) {
            throw ConcurrencyException("A snapshot for aggregate [${snapshot.aggregateIdentifier}] at sequence " +
                "[${snapshot.sequenceNumber}] was already inserted", e)
        }
    }

    /** Raw row, so a snapshot that cannot be deserialized does not break reading the others. */
    private inner class SnapshotRow(rs: ResultSet) {
        private val eventIdentifier = rs.getString("event_identifier")
        private val payloadType = SimpleSerializedType(rs.getString("payload_type"), rs.getString("payload_revision"))
        private val payload: ByteArray = rs.getBytes("payload")
        private val metaData: ByteArray? = rs.getBytes("meta_data")
        private val timeStamp = rs.getString("time_stamp")
        val aggregateIdentifier: String = rs.getString("aggregate_identifier")
        private val sequenceNumber = rs.getLong("sequence_number")
        private val type = rs.getString("type")

        fun toMessage(): DomainEventMessage<*> =
            message(eventIdentifier, payloadType, payload, metaData, timeStamp, aggregateIdentifier, sequenceNumber, type)
    }

    private fun toMessage(rs: ResultSet): DomainEventMessage<*> = message(
        rs.getString("event_identifier"),
        SimpleSerializedType(rs.getString("payload_type"), rs.getString("payload_revision")),
        rs.getBytes("payload"),
        rs.getBytes("meta_data"),
        rs.getString("time_stamp"),
        rs.getString("aggregate_identifier"),
        rs.getLong("sequence_number"),
        rs.getString("type"),
    )

    private fun message(
        eventIdentifier: String,
        payloadType: SimpleSerializedType,
        payloadBytes: ByteArray,
        metaDataBytes: ByteArray?,
        timeStamp: String,
        aggregateIdentifier: String,
        sequenceNumber: Long,
        type: String?,
    ): DomainEventMessage<*> {
        val payload: Any = serializer.deserialize(SimpleSerializedObject(payloadBytes, ByteArray::class.java, payloadType))
        val metaData: MetaData = metaDataBytes
            ?.let { serializer.deserialize<ByteArray, MetaData>(SimpleSerializedObject(it, ByteArray::class.java, metaDataType)) }
            ?: MetaData.emptyInstance()
        val timestamp = DateTimeUtils.parseInstant(timeStamp)
        return GenericDomainEventMessage(
            type,
            aggregateIdentifier,
            sequenceNumber,
            GenericMessage(eventIdentifier, payload, metaData),
            Supplier { timestamp },
        )
    }

    private companion object {
        /** Keeps statements well below PostgreSQL's 32767 bind parameters. */
        const val MAX_IDS_PER_QUERY = 1000
    }
}
