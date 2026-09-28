package app.dc8.example.task.contract

import app.dc8.axonthin.api.BulkCommandGateway
import app.dc8.example.task.TaskBulkRenameService
import app.dc8.example.task.api.CreateTaskCommand
import app.dc8.example.task.api.RenameTaskCommand
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import kotlin.system.measureTimeMillis

/**
 * The same bulk scenarios on each engine (subclassed by both app modules, PostgreSQL only): commands in one
 * `sendAllAndWait` chunk, and the pre-thin bulk pattern (flagged per-aggregate events + one bulk event published to the
 * EventStore). Prints time and round trips per table — the numbers to decide what to keep after switching.
 */
abstract class EngineBenchmark(private val engine: String) {

    @Autowired lateinit var bulk: BulkCommandGateway
    @Autowired lateinit var bulkRename: TaskBulkRenameService
    @Autowired lateinit var recorder: SqlRecorder

    private data class Row(val scenario: String, val ms: Long, val eventStore: Int, val summary: Int, val titleIndex: Int, val other: Int)

    private fun measure(scenario: String, block: () -> Unit): Row {
        recorder.clear()
        val ms = measureTimeMillis(block)
        val eventStore = recorder.count { it.contains("axon_domain_event_entry") || it.contains("axon_snapshot_event_entry") }
        val summary = recorder.count { Regex("""\btask_summary\b""").containsMatchIn(it) }
        val titleIndex = recorder.count { it.contains("task_title_index") }
        return Row(scenario, ms, eventStore, summary, titleIndex, recorder.count { true } - eventStore - summary - titleIndex)
    }

    private fun createTasks(n: Int): List<String> =
        bulk.sendAllAndWait<String>(List(n) { CreateTaskCommand(UUID.randomUUID().toString(), "t$it") })

    @Test
    fun `engine benchmark`() {
        repeat(2) { createTasks(200).also { ids -> bulk.sendAllAndWait<Any?>(ids.map { RenameTaskCommand(it, "warm") }) } } // warm-up

        lateinit var ids: List<String>
        val rows = listOf(
            measure("create 1k: commands, one chunk") { ids = createTasks(1_000) },
            measure("rename 1k: commands, one chunk") { bulk.sendAllAndWait<Any?>(ids.map { RenameTaskCommand(it, "r1") }) },
            measure("rename 1k: bulk pattern (flagged + bulk event)") { bulkRename.renameAll(ids.associateWith { "r2" }) },
        )

        println()
        println("| engine | scenario | ms | event store | task_summary | task_title_index | other |")
        println("|---|---|---:|---:|---:|---:|---:|")
        rows.forEach { println("| $engine | ${it.scenario} | ${it.ms} | ${it.eventStore} | ${it.summary} | ${it.titleIndex} | ${it.other} |") }
        println()
        assertThat(rows).allSatisfy { assertThat(it.ms).isPositive() }
    }
}
