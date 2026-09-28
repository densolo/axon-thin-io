package app.dc8.example.task

import app.dc8.axonthin.eventstore.EventStoreBrowser
import app.dc8.example.task.api.CreateTaskCommand
import app.dc8.example.task.api.RenameTaskCommand
import app.dc8.example.task.contract.PostgresSupport
import app.dc8.example.task.contract.StoredEvents
import org.assertj.core.api.Assertions.assertThat
import org.axonframework.commandhandling.gateway.CommandGateway
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.util.UUID

/** EventStoreBrowser: raw, keyset-paged reads for debug pages (thin only). */
@SpringBootTest(properties = ["spring.datasource.url=jdbc:h2:mem:event-store-browser;DB_CLOSE_DELAY=-1"])
class EventStoreBrowserTest {

    @Autowired lateinit var browser: EventStoreBrowser
    @Autowired lateinit var commandGateway: CommandGateway
    @Autowired lateinit var jdbc: JdbcTemplate

    @BeforeEach
    fun clean() = StoredEvents(jdbc, "axon_domain_event_entry").deleteAll()

    private fun taskWithRenames(renames: Int): String = UUID.randomUUID().toString().also { id ->
        commandGateway.sendAndWait<String>(CreateTaskCommand(id, "r0"))
        (1..renames).forEach { commandGateway.sendAndWait<Any>(RenameTaskCommand(id, "r$it")) }
    }

    @Test
    fun `pages through one aggregate's stream`() {
        val id = taskWithRenames(11) // 12 events

        val pages = generateSequence(browser.aggregate(id, limit = 5)) { page ->
            if (page.size < 5) null else browser.aggregate(id, afterSequence = page.last().sequenceNumber, limit = 5)
        }.toList()

        assertThat(pages.map { it.size }).containsExactly(5, 5, 2)
        assertThat(pages.flatten().map { it.sequenceNumber }).isEqualTo((0L..11L).toList())
        assertThat(pages.first().first().payload).contains("\"title\":\"r0\"") // raw JSON
        assertThat(browser.count(id)).isEqualTo(12)
    }

    @Test
    fun `lists the whole table newest first, with filters, including unknown event types`() {
        val a = taskWithRenames(2)
        val b = taskWithRenames(1)
        jdbc.update(
            "insert into axon_domain_event_entry (global_index, event_identifier, payload_type, payload_revision, payload, " +
                "meta_data, time_stamp, aggregate_identifier, sequence_number, type) values (?,?,?,?,?,?,?,?,?,?)",
            999_999_999, UUID.randomUUID().toString(), "com.example.gone.OldEvent", null, "{\"x\":1}".toByteArray(),
            null, "2026-01-01T00:00:00.000Z", "old-1", 0, "Old",
        )

        val first = browser.latest(limit = 3)
        val second = browser.latest(limit = 3, beforeGlobalIndex = first.last().globalIndex)

        assertThat(first.first().payloadType).isEqualTo("com.example.gone.OldEvent") // listed without deserializing
        assertThat(first.first().payload).isEqualTo("{\"x\":1}")
        assertThat((first + second).map { it.globalIndex }).isSortedAccordingTo(Comparator.reverseOrder()).hasSize(6)
        assertThat(browser.latest(filter = EventStoreBrowser.Filter(aggregateIdentifier = a)).map { it.sequenceNumber })
            .containsExactly(2L, 1L, 0L)
        assertThat(browser.latest(filter = EventStoreBrowser.Filter(payloadType = "app.dc8.example.task.api.TaskRenamedEvent")))
            .hasSize(3).allSatisfy { assertThat(it.aggregateIdentifier).isIn(a, b) }
    }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun database(registry: DynamicPropertyRegistry) = PostgresSupport.register(registry, "event_store_browser")
    }
}
