package com.dc8.example.task.contract

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.springframework.jdbc.core.JdbcTemplate

/** A raw `domain_event_entry` / `snapshot_event_entry` row, decoded for assertions (payload / meta_data as JSON). */
data class StoredEvent(
    val globalIndex: Long?,
    val eventIdentifier: String,
    val payloadType: String,
    val payloadRevision: String?,
    val payload: Map<String, Any?>,
    val metaData: Map<String, Any?>,
    val timeStamp: String,
    val aggregateIdentifier: String,
    val sequenceNumber: Long,
    val type: String?,
)

/** Reads the event table directly with JDBC — independent of the engine that wrote it. */
class StoredEvents(
    private val jdbc: JdbcTemplate,
    private val table: String,
    private val snapshotTable: String = table.replace("domain_event_entry", "snapshot_event_entry"),
) {

    private val json = ObjectMapper()

    fun all(): List<StoredEvent> = query("select * from $table order by global_index")

    fun forAggregate(id: String): List<StoredEvent> =
        query("select * from $table where aggregate_identifier = ? order by sequence_number", id)

    fun snapshots(id: String): List<StoredEvent> =
        query("select * from $snapshotTable where aggregate_identifier = ? order by sequence_number", id)

    /** Simulates purged history: only a snapshot can restore the aggregate afterwards. */
    fun deleteEventsUpTo(id: String, sequenceNumber: Long) {
        jdbc.update("delete from $table where aggregate_identifier = ? and sequence_number <= ?", id, sequenceNumber)
    }

    fun corruptSnapshotPayload(id: String) {
        jdbc.update("update $snapshotTable set payload = ? where aggregate_identifier = ?", "{not json".toByteArray(), id)
    }

    fun corruptSnapshotType(id: String) {
        jdbc.update("update $snapshotTable set payload_type = 'com.example.NoSuchClass' where aggregate_identifier = ?", id)
    }

    fun count(): Long = jdbc.queryForObject("select count(*) from $table", Long::class.java)!!

    fun deleteAll() {
        jdbc.update("delete from $table")
        jdbc.update("delete from $snapshotTable")
    }

    private fun java.sql.ResultSetMetaData.columnNames() = (1..columnCount).map { getColumnName(it).uppercase() }

    private fun query(sql: String, vararg args: Any): List<StoredEvent> = jdbc.query(sql, { rs, _ ->
        StoredEvent(
            globalIndex = if (rs.metaData.columnNames().contains("GLOBAL_INDEX")) rs.getLong("global_index") else null,
            eventIdentifier = rs.getString("event_identifier"),
            payloadType = rs.getString("payload_type"),
            payloadRevision = rs.getString("payload_revision"),
            payload = json.readValue(rs.getBytes("payload")),
            metaData = rs.getBytes("meta_data")?.let { json.readValue<Map<String, Any?>>(it) } ?: emptyMap(),
            timeStamp = rs.getString("time_stamp"),
            aggregateIdentifier = rs.getString("aggregate_identifier"),
            sequenceNumber = rs.getLong("sequence_number"),
            type = rs.getString("type"),
        )
    }, *args)
}
