package com.dc8.example.task

import com.dc8.example.task.api.CreateTaskCommand
import com.dc8.example.task.api.RenameTaskCommand
import com.dc8.example.task.contract.PostgresSupport
import com.dc8.example.task.contract.StoredEvents
import com.dc8.example.task.query.TaskQueryService
import org.assertj.core.api.Assertions.assertThat
import org.axonframework.commandhandling.gateway.CommandGateway
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.util.UUID

/**
 * Deliberate difference from Axon 4: an unreadable snapshot (unknown class or malformed payload) is skipped and the
 * aggregate is rebuilt from the full event stream. Axon 4.13 fails the command instead (IncompatibleAggregateException
 * / SerializationException) because payloads are deserialized lazily, after its own fallback had its chance.
 */
@SpringBootTest(properties = ["task.snapshot-threshold=5"])
class ThinSnapshotFallbackTest {

    @Autowired lateinit var commandGateway: CommandGateway
    @Autowired lateinit var queries: TaskQueryService
    @Autowired lateinit var jdbc: JdbcTemplate

    @ParameterizedTest
    @ValueSource(strings = ["type", "payload"])
    fun `unreadable snapshot falls back to the full event stream`(corruption: String) {
        val stored = StoredEvents(jdbc, "axon_domain_event_entry")
        val taskId = UUID.randomUUID().toString()
        commandGateway.sendAndWait<String>(CreateTaskCommand(taskId, "r0"))
        (1..4).forEach { commandGateway.sendAndWait<Any>(RenameTaskCommand(taskId, "r$it")) }
        assertThat(stored.snapshots(taskId)).hasSize(1)
        if (corruption == "type") stored.corruptSnapshotType(taskId) else stored.corruptSnapshotPayload(taskId)

        commandGateway.sendAndWait<Any>(RenameTaskCommand(taskId, "after corruption"))

        assertThat(queries.summary(taskId)!!.title).isEqualTo("after corruption")
        assertThat(stored.forAggregate(taskId).last().sequenceNumber).isEqualTo(5L)
    }

    companion object {
        /** PostgreSQL database for `-Ppostgres`; no-op on H2. */
        @JvmStatic
        @DynamicPropertySource
        fun database(registry: DynamicPropertyRegistry) = PostgresSupport.register(registry, "snapshot_fallback")
    }
}
