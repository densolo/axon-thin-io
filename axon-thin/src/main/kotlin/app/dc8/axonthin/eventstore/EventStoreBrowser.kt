package app.dc8.axonthin.eventstore

import org.springframework.jdbc.core.JdbcTemplate
import java.sql.ResultSet

/**
 * Read-only access to the raw event table, for debug and admin pages. Rows are returned as stored — payload and
 * metadata as JSON text, nothing deserialized — so event types the running code no longer knows are listed too.
 * Paging is keyset-based (no OFFSET): pass the last row's position to get the next page.
 */
class EventStoreBrowser internal constructor(
    private val jdbc: JdbcTemplate,
    private val table: String,
) {
    /** One stored event. [payload] and [metaData] are the stored JSON. */
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

    /** One aggregate's stream in sequence order, [limit] events after [afterSequence] (exclusive; `null` = from the start). */
    fun aggregate(aggregateIdentifier: String, afterSequence: Long? = null, limit: Int = 50): List<Row> =
        jdbc.query(
            "select $COLUMNS from $table where aggregate_identifier = ? and sequence_number > ? " +
                "order by sequence_number limit ${limit.coerceIn(1, MAX_LIMIT)}",
            ::row, aggregateIdentifier, afterSequence ?: -1L,
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
        return jdbc.query(
            "select $COLUMNS from $table $where order by global_index desc limit ${limit.coerceIn(1, MAX_LIMIT)}",
            ::row, *args.toTypedArray(),
        )
    }

    /** Number of stored events of an aggregate (for "page x of y" displays). */
    fun count(aggregateIdentifier: String): Long =
        jdbc.queryForObject("select count(*) from $table where aggregate_identifier = ?", Long::class.java, aggregateIdentifier)!!

    @Suppress("UNUSED_PARAMETER")
    private fun row(rs: ResultSet, rowNum: Int) = Row(
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
