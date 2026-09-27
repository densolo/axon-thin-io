package com.dc8.example.task

import com.dc8.axonthin.ProjectionMigrator
import com.dc8.axonthin.api.BulkCommandGateway
import com.dc8.example.task.ThinChunkPipelineTest.SqlRecorder
import com.dc8.example.task.api.CreateTaskCommand
import com.dc8.example.task.api.DeleteTaskCommand
import com.dc8.example.task.api.RenameTaskCommand
import com.dc8.example.task.contract.PostgresSupport
import com.dc8.example.task.contract.StoredEvents
import com.dc8.example.task.projection.CommentViewRepository
import com.dc8.example.task.projection.TaskActivityRepository
import com.dc8.example.task.projection.TaskSummaryRepository
import org.assertj.core.api.Assertions.assertThat
import org.axonframework.commandhandling.gateway.CommandGateway
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.util.UUID

/**
 * A real batch projection ([TaskTitleIndexProjection]) next to the per-event TaskSummaryProjection: same results,
 * a fraction of the round trips, and replayable page by page.
 */
@SpringBootTest(
    properties = [
        "spring.datasource.url=jdbc:h2:mem:batch-projection;DB_CLOSE_DELAY=-1",
        "spring.jpa.properties.hibernate.jdbc.batch_size=100",
        "spring.jpa.properties.hibernate.order_inserts=true",
        "spring.jpa.properties.hibernate.order_updates=true",
        "axon.thin.replay-page-size=250",
    ],
)
@Import(ThinChunkPipelineTest.Config::class)
class ThinBatchProjectionTest {

    @Autowired lateinit var bulk: BulkCommandGateway
    @Autowired lateinit var commandGateway: CommandGateway
    @Autowired lateinit var titles: TaskTitleIndexRepository
    @Autowired lateinit var projection: TaskTitleIndexProjection
    @Autowired lateinit var summaries: TaskSummaryRepository
    @Autowired lateinit var comments: CommentViewRepository
    @Autowired lateinit var activities: TaskActivityRepository
    @Autowired lateinit var migrator: ProjectionMigrator
    @Autowired lateinit var recorder: SqlRecorder
    @Autowired lateinit var jdbc: JdbcTemplate

    @BeforeEach
    fun clean() {
        StoredEvents(jdbc, "axon_domain_event_entry").deleteAll()
        listOf(titles, summaries, comments, activities).forEach { it.deleteAllInBatch() }
        projection.batches.clear()
    }

    private fun id() = UUID.randomUUID().toString()

    /** What the per-event projection shows, for comparison. */
    private fun summaryTitles() = summaries.findAll().associate { it.taskId to it.title }

    private fun indexTitles() = titles.findAll().associate { it.taskId to it.title }

    @Test
    fun `batch projection matches the per-event projection, for chunks and single commands`() {
        val a = id()
        val b = id()
        val c = id()
        bulk.sendAllAndWait(
            listOf(
                CreateTaskCommand(a, "a"), RenameTaskCommand(a, "a2"), RenameTaskCommand(a, "a3"),
                CreateTaskCommand(b, "b"), CreateTaskCommand(c, "c"), DeleteTaskCommand(c),
            ),
        )
        commandGateway.sendAndWait<Any>(RenameTaskCommand(b, "b2"))
        commandGateway.sendAndWait<Any>(DeleteTaskCommand(a))

        assertThat(indexTitles()).isEqualTo(summaryTitles()).isEqualTo(mapOf(b to "b2"))
        assertThat(projection.batches).containsExactly(6, 1, 1) // one call per chunk
    }

    @Test
    fun `a 1k chunk costs the batch projection one read and a few write batches`() {
        recorder.clear()
        val ids = bulk.sendAllAndWait(List(1_000) { CreateTaskCommand(id(), "t$it") }).map { it as String }
        val createTrips = trips()
        recorder.clear()

        bulk.sendAllAndWait(ids.map { RenameTaskCommand(it, "renamed") })
        val renameTrips = trips()

        println("round trips for 1k commands, per table: creates $createTrips, renames $renameTrips")
        // batch projection: one findAllById per chunk (no per-row SELECT thanks to isNew), writes in JDBC batches of 100
        assertThat(createTrips.getValue("task_title_index")).isEqualTo(1 + 10)
        assertThat(renameTrips.getValue("task_title_index")).isEqualTo(1 + 10)
        // per-event projection: about one round trip per event
        assertThat(renameTrips.getValue("task_summary")).isGreaterThanOrEqualTo(1_000)
        assertThat(indexTitles()).isEqualTo(summaryTitles())
    }

    @Test
    fun `migrator rebuilds the batch projection page by page`() {
        bulk.sendAllAndWait(List(600) { CreateTaskCommand(id(), "t$it") })
        val live = indexTitles()
        titles.deleteAllInBatch()
        projection.batches.clear()

        val result = migrator.migrate().single { it.projection == "task-title-index" }

        assertThat(result.outcome).isEqualTo(ProjectionMigrator.Outcome.REPLAYED)
        assertThat(projection.batches).containsExactly(250, 250, 100) // one call per replay page
        assertThat(indexTitles()).isEqualTo(live)
    }

    private fun trips(): Map<String, Int> = listOf("task_title_index", "task_summary").associateWith { table ->
        recorder.count { sql -> Regex("""\b$table\b""").containsMatchIn(sql) }
    }.also { recorder.clear() }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun database(registry: DynamicPropertyRegistry) = PostgresSupport.register(registry, "batch_projection")
    }
}
