package app.dc8.axonthin.api

import java.sql.Connection
import java.sql.ResultSet
import javax.sql.DataSource

/**
 * Read-only access to the raw event table (Axon's `domain_event_entry` layout), for debug and admin pages.
 *
 * - Rows are returned as stored: payload and metadata as text (the serializer's JSON), nothing deserialized — so event
 *   types the running code no longer knows are listed too.
 * - Paging is keyset-based (no OFFSET): pass the last row's position to get the next page.
 * - Plain JDBC on its own connection: it sees committed data only, and works the same whichever engine wrote the
 *   table. Both `axon-thin` and `axon-thin-v4-adapter` register it as a bean for the configured table
 *   (`axon.thin.event-store.table-prefix` / `domain-event-table`), so pages built on it survive the engine switch.
 * - [decode] turns rows into metadata + (lazily) payload objects with the engine's event serializer: a list can show
 *   metadata without ever deserializing payloads; a detail view fetches rows by id ([event], [events]) and decodes.
 */
class EventStoreBrowser(
    private val dataSource: DataSource,
    private val table: String = "domain_event_entry",
    /** Provided by the engine's auto-configuration (the event serializer); without it [decode] is unavailable. */
    private val decoder: EventDecoder? = null,
) {
    /** One stored event. [payload] and [metaData] are the stored bytes as UTF-8 text. */
    data class Row(
        val globalIndex: Long,
        val eventIdentifier: String,
        val aggregateIdentifier: String,
        val sequenceNumber: Long,
        /** Aggregate type; `null` for events published outside an aggregate. */
        val type: String?,
        val payloadType: String,
        val payloadRevision: String?,
        val timeStamp: String,
        val payload: String,
        val metaData: String?,
    )

    /** Filters for [latest]; `null` = any. */
    data class Filter(
        val aggregateType: String? = null,
        val payloadType: String? = null,
        val aggregateIdentifier: String? = null,
        /** ISO-8601 bounds on `time_stamp` (inclusive from, exclusive to), e.g. `2026-09-28T00:00:00.000Z`. */
        val from: String? = null,
        val to: String? = null,
    )

    /** One event by its `event_identifier`, or `null`. */
    fun event(eventIdentifier: String): Row? = events(listOf(eventIdentifier)).firstOrNull()

    /** Events by `event_identifier`, in the order requested; unknown ids are left out. */
    fun events(eventIdentifiers: Collection<String>): List<Row> {
        val ids = eventIdentifiers.distinct()
        if (ids.isEmpty()) return emptyList()
        val found = ids.chunked(MAX_LIMIT).flatMap { part ->
            query("select $COLUMNS from $table where event_identifier in (${part.joinToString(", ") { "?" }})", part)
        }.associateBy { it.eventIdentifier }
        return ids.mapNotNull(found::get)
    }

    /**
     * One aggregate's events, decoded, as a lazy sequence fetching [pageSize] rows at a time as it is consumed —
     * the browser's counterpart of `eventStore.readEvents(id).asSequence()`:
     * `browser.readEvents(id).map { it.metaData to it.payloadAs<MyEvent>() }`.
     *
     * Unlike `EventStore.readEvents`, it never starts with a snapshot (every event from [fromSequence], inclusive) and
     * never fails on an event it cannot decode ([Payload.Unknown] / [Payload.Failed] instead).
     */
    fun readEvents(aggregateIdentifier: String, fromSequence: Long = 0, pageSize: Int = 100): Sequence<DecodedEvent> {
        val decoder = requireDecoder()
        return generateSequence(aggregate(aggregateIdentifier, fromSequence - 1, pageSize)) { page ->
            if (page.size < pageSize) null
            else aggregate(aggregateIdentifier, page.last().sequenceNumber, pageSize).takeIf { it.isNotEmpty() }
        }.flatten().map { DecodedEvent(it, decoder) }
    }

    /** Metadata now, payload on first access — never throws for a bad row (see [Payload]). */
    fun decode(row: Row): DecodedEvent = DecodedEvent(row, requireDecoder())

    fun decode(rows: List<Row>): List<DecodedEvent> = requireDecoder().let { d -> rows.map { DecodedEvent(it, d) } }

    private fun requireDecoder(): EventDecoder =
        checkNotNull(decoder) { "No EventDecoder: use the EventStoreBrowser bean provided by axon-thin / axon-thin-v4-adapter" }

    /** One aggregate's stream in sequence order, [limit] events after [afterSequence] (exclusive; `null` = from the start). */
    fun aggregate(aggregateIdentifier: String, afterSequence: Long? = null, limit: Int = 50): List<Row> = query(
        "select $COLUMNS from $table where aggregate_identifier = ? and sequence_number > ? " +
            "order by sequence_number limit ${limit.coerceIn(1, MAX_LIMIT)}",
        listOf(aggregateIdentifier, afterSequence ?: -1L),
    )

    /** The whole table, newest first: [limit] events before [beforeGlobalIndex] (exclusive; `null` = the newest). */
    fun latest(limit: Int = 50, beforeGlobalIndex: Long? = null, filter: Filter = Filter()): List<Row> {
        val conditions = ArrayList<String>()
        val args = ArrayList<Any>()
        beforeGlobalIndex?.let { conditions += "global_index < ?"; args += it }
        filter.aggregateType?.let { conditions += "type = ?"; args += it }
        filter.payloadType?.let { conditions += "payload_type = ?"; args += it }
        filter.aggregateIdentifier?.let { conditions += "aggregate_identifier = ?"; args += it }
        filter.from?.let { conditions += "time_stamp >= ?"; args += it }
        filter.to?.let { conditions += "time_stamp < ?"; args += it }
        val where = if (conditions.isEmpty()) "" else "where " + conditions.joinToString(" and ")
        return query("select $COLUMNS from $table $where order by global_index desc limit ${limit.coerceIn(1, MAX_LIMIT)}", args)
    }

    /** Number of stored events of an aggregate (for "page x of y" displays). */
    fun count(aggregateIdentifier: String): Long = connection { c ->
        c.prepareStatement("select count(*) from $table where aggregate_identifier = ?").use { st ->
            st.setString(1, aggregateIdentifier)
            st.executeQuery().use { rs -> rs.next(); rs.getLong(1) }
        }
    }

    private fun query(sql: String, args: List<Any>): List<Row> = connection { c ->
        c.prepareStatement(sql).use { st ->
            args.forEachIndexed { i, arg -> st.setObject(i + 1, arg) }
            st.executeQuery().use { rs -> generateSequence { if (rs.next()) row(rs) else null }.toList() }
        }
    }

    private fun <T> connection(block: (Connection) -> T): T = dataSource.connection.use(block)

    private fun row(rs: ResultSet) = Row(
        globalIndex = rs.getLong("global_index"),
        eventIdentifier = rs.getString("event_identifier"),
        aggregateIdentifier = rs.getString("aggregate_identifier"),
        sequenceNumber = rs.getLong("sequence_number"),
        type = rs.getString("type"),
        payloadType = rs.getString("payload_type"),
        payloadRevision = rs.getString("payload_revision"),
        timeStamp = rs.getString("time_stamp"),
        payload = rs.getBytes("payload").toString(Charsets.UTF_8),
        metaData = rs.getBytes("meta_data")?.toString(Charsets.UTF_8),
    )

    private companion object {
        const val COLUMNS = "global_index, event_identifier, aggregate_identifier, sequence_number, type, payload_type, " +
            "payload_revision, time_stamp, payload, meta_data"
        const val MAX_LIMIT = 1000
    }
}

/** A stored event decoded with the engine's event serializer. */
class DecodedEvent internal constructor(val row: Row, decoder: EventDecoder) {

    /** Decoded metadata (plain values); empty if it could not be read — see [metaDataError]. */
    val metaData: Map<String, Any?>

    /** Why [metaData] is empty although the row has metadata, else `null`. */
    val metaDataError: Throwable?

    init {
        val result = runCatching { row.metaData?.let { decoder.metaData(it.toByteArray(Charsets.UTF_8)) } ?: emptyMap() }
        metaData = result.getOrDefault(emptyMap())
        metaDataError = result.exceptionOrNull()
    }

    /** The payload, deserialized on first access. */
    val payload: Payload by lazy {
        decoder.payload(row.payloadType, row.payloadRevision, row.payload.toByteArray(Charsets.UTF_8))
    }

    /** The payload if it decoded to a [T], else `null` (another type, unknown class, or unreadable). */
    inline fun <reified T : Any> payloadAs(): T? = (payload as? Payload.Decoded)?.value as? T
}

private typealias Row = EventStoreBrowser.Row

/** Outcome of decoding one payload. */
sealed interface Payload {
    /** Deserialized by the event serializer. */
    data class Decoded(val value: Any) : Payload

    /** The class is not on this classpath (e.g. written by another branch, or removed). */
    data class Unknown(val payloadType: String, val payloadRevision: String?) : Payload

    /** The class exists but this row could not be read (e.g. incompatible JSON). */
    data class Failed(val error: Throwable) : Payload
}

/** Decodes stored bytes with an engine's serializer; implemented by `axon-thin` and `axon-thin-v4-adapter`. */
interface EventDecoder {
    fun metaData(data: ByteArray): Map<String, Any?>

    fun payload(payloadType: String, payloadRevision: String?, data: ByteArray): Payload
}
