package com.dc8.example.task

import com.dc8.axonthin.ProjectionMigrator
import com.dc8.axonthin.ProjectionMigrator.Outcome
import com.dc8.example.task.api.AddCommentCommand
import com.dc8.example.task.api.AssignTaskCommand
import com.dc8.example.task.api.ChangeTaskStatusCommand
import com.dc8.example.task.api.CreateTaskCommand
import com.dc8.example.task.api.DeleteCommentCommand
import com.dc8.example.task.api.DeleteTaskCommand
import com.dc8.example.task.api.RenameTaskCommand
import com.dc8.example.task.api.TaskStatus
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
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.util.UUID

/** ProjectionMigrator: refills emptied `@ReplayInto` projections from the event store (thin only). */
@SpringBootTest(
    properties = [
        "spring.datasource.url=jdbc:h2:mem:projection-migration;DB_CLOSE_DELAY=-1",
        "axon.thin.replay-page-size=7", // every scenario spans several pages
    ],
)
class ThinProjectionMigrationTest {

    @Autowired lateinit var migrator: ProjectionMigrator
    @Autowired lateinit var commandGateway: CommandGateway
    @Autowired lateinit var summaries: TaskSummaryRepository
    @Autowired lateinit var comments: CommentViewRepository
    @Autowired lateinit var activities: TaskActivityRepository
    @Autowired lateinit var jdbc: JdbcTemplate
    @Autowired lateinit var titles: TaskTitleIndexRepository // test-only batch projection, also @ReplayInto

    private val stored by lazy { StoredEvents(jdbc, "axon_domain_event_entry") }

    @BeforeEach
    fun clean() {
        stored.deleteAll()
        listOf(summaries, comments, activities, titles).forEach { it.deleteAllInBatch() }
    }

    private fun id() = UUID.randomUUID().toString()

    /** A bit of everything: renames, comments, status, assignment, deletion. */
    private fun history(): List<String> {
        val ids = List(3) { id() }
        ids.forEachIndexed { i, taskId ->
            commandGateway.sendAndWait<String>(CreateTaskCommand(taskId, "task $i"))
            repeat(10) { r -> commandGateway.sendAndWait<Any>(RenameTaskCommand(taskId, "task $i rename $r")) }
            commandGateway.sendAndWait<String>(AddCommentCommand(taskId, "c$i-1", "ann", "one"))
            commandGateway.sendAndWait<String>(AddCommentCommand(taskId, "c$i-2", "bob", "two"))
            commandGateway.sendAndWait<Any>(DeleteCommentCommand(taskId, "c$i-1"))
            commandGateway.sendAndWait<Any>(AssignTaskCommand(taskId, "user$i"))
        }
        commandGateway.sendAndWait<Any>(ChangeTaskStatusCommand(ids[0], TaskStatus.DONE))
        commandGateway.sendAndWait<Any>(DeleteTaskCommand(ids[2]))
        return ids
    }

    private fun summaryRows() = summaries.findAll()
        .associate { it.taskId to listOf(it.title, it.status, it.assignee, it.commentCount, it.createdBy) }

    private fun activityRows(taskId: String) = activities.findByTaskIdOrderById(taskId).map { it.type to it.sequenceNumber }

    private fun outcomes(results: List<ProjectionMigrator.Result>) = results.associate { it.projection to it.outcome }

    @Test
    fun `an emptied projection is rebuilt exactly as live handling built it`() {
        history()
        val live = summaryRows()
        summaries.deleteAllInBatch() // what a Liquibase change would do

        val results = migrator.migrate()

        assertThat(outcomes(results))
            .containsEntry("task-summary", Outcome.REPLAYED)
            .containsEntry("task-comments", Outcome.NOT_EMPTY)
            .containsEntry("task-activity", Outcome.NOT_EMPTY)
        assertThat(results.first { it.projection == "task-summary" }.events).isGreaterThan(7) // several pages
        assertThat(summaryRows()).isEqualTo(live)
    }

    @Test
    fun `several emptied projections are rebuilt in one pass, in per-aggregate order`() {
        val ids = history()
        val liveComments = comments.findAll().map { it.commentId to it.text }.toSet()
        val liveActivity = ids.associateWith(::activityRows)
        comments.deleteAllInBatch()
        activities.deleteAllInBatch()

        val results = migrator.migrate()

        assertThat(outcomes(results)).containsEntry("task-comments", Outcome.REPLAYED).containsEntry("task-activity", Outcome.REPLAYED)
        assertThat(comments.findAll().map { it.commentId to it.text }.toSet()).isEqualTo(liveComments)
        assertThat(ids.associateWith(::activityRows)).isEqualTo(liveActivity)
    }

    @Test
    fun `filled projections are left alone, so a second run does nothing`() {
        history()
        summaries.deleteAllInBatch()
        migrator.migrate()

        assertThat(outcomes(migrator.migrate()).values).containsOnly(Outcome.NOT_EMPTY)
    }

    @Test
    fun `empty projections with no relevant events are left alone`() {
        assertThat(outcomes(migrator.migrate()).values).containsOnly(Outcome.NO_EVENTS)
    }

    @Test
    fun `event types unknown to this code (another branch) are skipped`() {
        history()
        val live = summaryRows()
        jdbc.update(
            "insert into axon_domain_event_entry (global_index, event_identifier, payload_type, payload_revision, payload, " +
                "meta_data, time_stamp, aggregate_identifier, sequence_number, type) values (?,?,?,?,?,?,?,?,?,?)",
            99_000_000, id(), "com.example.other.branch.SomethingHappened", null, "{}".toByteArray(), "{}".toByteArray(),
            "2026-01-01T00:00:00.000Z", id(), 0, "Other",
        )
        summaries.deleteAllInBatch()

        migrator.migrate()

        assertThat(summaryRows()).isEqualTo(live)
    }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun database(registry: DynamicPropertyRegistry) = PostgresSupport.register(registry, "projection_migration")
    }
}
