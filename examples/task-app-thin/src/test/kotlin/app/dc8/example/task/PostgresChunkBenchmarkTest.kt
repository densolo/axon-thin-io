package app.dc8.example.task

import app.dc8.axonthin.api.BulkCommandGateway
import app.dc8.example.task.ThinChunkPipelineTest.SqlRecorder
import app.dc8.example.task.api.CreateTaskCommand
import app.dc8.example.task.api.RenameTaskCommand
import app.dc8.example.task.contract.PostgresSupport
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.util.UUID
import kotlin.system.measureTimeMillis

/**
 * Chunk timings and round trips on PostgreSQL (`mvn test -Ppostgres -Dtest=PostgresChunkBenchmarkTest`).
 * Prints a table; asserts only that event-store round trips per chunk do not grow with the chunk size.
 *
 * `task_summary` is written by a naive per-event projection (one `findById`/merge per event); `task_title_index` by
 * the batch projection [TaskTitleIndexProjection] (`findAllById` + JDBC-batched writes) — the difference is the point.
 */
@EnabledIfSystemProperty(named = "thin.test.db", matches = "postgres")
@SpringBootTest(properties = ["task.snapshot-threshold=100"])
@Import(ThinChunkPipelineTest.Config::class)
class PostgresChunkBenchmarkTest {

    @Autowired lateinit var bulk: BulkCommandGateway
    @Autowired lateinit var recorder: SqlRecorder

    private val eventStore = { sql: String ->
        sql.contains("axon_domain_event_entry") || sql.contains("axon_snapshot_event_entry")
    }

    private val perEventProjection = { sql: String -> Regex("""\btask_summary\b""").containsMatchIn(sql) }
    private val batchProjection = { sql: String -> sql.contains("task_title_index") }

    private data class Row(
        val scenario: String, val chunks: Int, val commands: Int, val ms: Long,
        val eventStore: Int, val perEvent: Int, val batch: Int, val other: Int,
    )

    private fun measure(scenario: String, chunks: List<List<Any>>): Row {
        recorder.clear()
        val ms = measureTimeMillis { chunks.forEach { bulk.sendAllAndWait(it) } }
        val es = recorder.count(eventStore)
        val perEvent = recorder.count(perEventProjection)
        val batch = recorder.count(batchProjection)
        return Row(scenario, chunks.size, chunks.sumOf { it.size }, ms, es, perEvent, batch, recorder.count { true } - es - perEvent - batch)
    }

    @Test
    fun `chunk benchmark`() {
        fun creates(n: Int) = List(n) { CreateTaskCommand(UUID.randomUUID().toString(), "task $it") }
        measure("warm-up", listOf(creates(200), creates(200))) // JIT, connection pool, sequence blocks

        val ids100 = bulk.sendAllAndWait(creates(100)).map { it as String }
        val ids1k = bulk.sendAllAndWait(creates(1_000)).map { it as String }
        val rows = listOf(
            measure("create 100, one chunk", listOf(creates(100))),
            measure("create 1k, one chunk", listOf(creates(1_000))),
            measure("rename 100, one chunk", listOf(ids100.map { RenameTaskCommand(it, "r1") })),
            measure("rename 1k, one chunk", listOf(ids1k.map { RenameTaskCommand(it, "r1") })),
            measure("create 10k, chunks of 500", creates(10_000).chunked(500)),
        )

        println()
        println("| scenario | chunks | commands | ms | ms/command | event store | task_summary (per-event) | task_title_index (batch) | other projections |")
        println("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        rows.forEach {
            println(
                "| ${it.scenario} | ${it.chunks} | ${it.commands} | ${it.ms} | ${"%.2f".format(it.ms.toDouble() / it.commands)} " +
                    "| ${it.eventStore} | ${it.perEvent} | ${it.batch} | ${it.other} |",
            )
        }
        println()

        // event store: ~4 round trips per chunk (snapshots, events, index, insert), whatever the chunk size
        rows.forEach { assertThat(it.eventStore).describedAs(it.scenario).isLessThanOrEqualTo(it.chunks * 6) }
    }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun database(registry: DynamicPropertyRegistry) = PostgresSupport.register(registry, "benchmark")
    }
}
