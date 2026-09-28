package app.dc8.example.task

import app.dc8.axonthin.ProjectionMigrator
import app.dc8.axonthin.ProjectionMigrator.Outcome
import app.dc8.example.task.api.AddCommentCommand
import app.dc8.example.task.api.AssignTaskCommand
import app.dc8.example.task.api.ChangeTaskStatusCommand
import app.dc8.example.task.api.CreateTaskCommand
import app.dc8.example.task.api.DeleteCommentCommand
import app.dc8.example.task.api.DeleteTaskCommand
import app.dc8.example.task.api.RenameTaskCommand
import app.dc8.example.task.api.TaskRenamedEvent
import app.dc8.example.task.api.TaskStatus
import app.dc8.example.task.contract.PostgresSupport
import app.dc8.example.task.contract.StoredEvents
import app.dc8.example.task.projection.CommentViewRepository
import app.dc8.example.task.projection.TaskActivityRepository
import app.dc8.example.task.projection.TaskSummaryRepository
import org.assertj.core.api.Assertions.assertThat
import org.axonframework.commandhandling.gateway.CommandGateway
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.axonframework.serialization.Serializer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
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
    @Autowired lateinit var bulkRename: TaskBulkRenameService
    @Autowired @Qualifier("eventSerializer") lateinit var serializer: Serializer
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

    @Test
    fun `the bulk pattern (flagged singles + bulk event) is rebuilt as live handling built it`() {
        val ids = List(4) { id().also { taskId -> commandGateway.sendAndWait<String>(CreateTaskCommand(taskId, "t")) } }
        bulkRename.renameAll(ids.associateWith { "bulk $it" })
        commandGateway.sendAndWait<Any>(RenameTaskCommand(ids[0], "single after bulk"))
        bulkRename.renameAll(mapOf(ids[1] to "second bulk"))
        val live = summaryRows()
        summaries.deleteAllInBatch()

        val result = migrator.migrate().single { it.projection == "task-summary" }

        assertThat(result.outcome).isEqualTo(Outcome.REPLAYED)
        assertThat(result.inversions).isZero()
        assertThat(summaryRows()).isEqualTo(live) // needs append order: the bulk events have their own aggregate ids
    }

    @Test
    fun `an aggregate event stored with a lower global index than its predecessor is reported`() {
        val taskId = id()
        commandGateway.sendAndWait<String>(CreateTaskCommand(taskId, "created")) // seq 0
        val base = jdbc.queryForObject("select max(global_index) from axon_domain_event_entry", Long::class.java)!!
        // seq 1 and 2 as two processes with pooled blocks could store them: the later event gets the lower index
        rawRename(taskId, sequence = 1, globalIndex = base + 1_000_000, title = "older (seq 1)")
        rawRename(taskId, sequence = 2, globalIndex = base + 500_000, title = "newer (seq 2)")
        summaries.deleteAllInBatch()

        val result = migrator.migrate().single { it.projection == "task-summary" }

        assertThat(result.inversions).isEqualTo(1)
        // what the warning is about: replayed in index order, the older rename is applied last
        assertThat(summaries.findById(taskId).orElseThrow().title).isEqualTo("older (seq 1)")
    }

    private fun rawRename(taskId: String, sequence: Long, globalIndex: Long, title: String) {
        val payload = serializer.serialize(TaskRenamedEvent(taskId, title), ByteArray::class.java)
        jdbc.update(
            "insert into axon_domain_event_entry (global_index, event_identifier, payload_type, payload_revision, payload, " +
                "meta_data, time_stamp, aggregate_identifier, sequence_number, type) values (?,?,?,?,?,?,?,?,?,?)",
            globalIndex, id(), payload.type.name, payload.type.revision, payload.data, "{}".toByteArray(),
            "2026-01-01T00:00:00.000Z", taskId, sequence, "Task",
        )
    }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun database(registry: DynamicPropertyRegistry) = PostgresSupport.register(registry, "projection_migration")
    }
}
