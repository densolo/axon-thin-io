package app.dc8.example.task

import app.dc8.axonthin.api.BulkCommandGateway
import app.dc8.axonthin.api.BulkOptions
import app.dc8.example.task.api.CreateTaskCommand
import app.dc8.example.task.api.RenameTaskCommand
import app.dc8.example.task.contract.PostgresSupport
import app.dc8.example.task.contract.StoredEvents
import app.dc8.example.task.projection.TaskSummaryRepository
import org.assertj.core.api.Assertions.assertThat
import org.axonframework.commandhandling.gateway.CommandGateway
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.jdbc.core.JdbcTemplate
import java.util.Collections
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.random.Random

/**
 * Two application contexts on one PostgreSQL database — "backend" and "jobs", as two processes would be — writing to
 * the same aggregates at the same time. Checks the invariants afterwards instead of the interleaving.
 *
 * PostgreSQL only (`mvn test -Ppostgres`): H2 does not reproduce PostgreSQL's unique-index waits.
 *
 * Note: with zero think time (users renaming the same 50 aggregates back-to-back) a 50-aggregate chunk can lose every
 * race and exhaust its retries — large optimistic transactions starve under constant contention on their aggregates.
 */
@EnabledIfSystemProperty(named = "thin.test.db", matches = "postgres")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PostgresConcurrencyTest {

    private lateinit var backend: ConfigurableApplicationContext
    private lateinit var jobs: ConfigurableApplicationContext

    @BeforeAll
    fun startProcesses() {
        val db = PostgresSupport.properties("concurrency") + mapOf(
            "spring.jpa.hibernate.ddl-auto" to "update", // two "processes" share the schema: never drop it
            "axon.thin.event-handler-error-mode" to "propagate",
            "axon.thin.concurrency-retries" to "20",     // single commands (UI) retry too
        )
        backend = start(db + ("spring.application.name" to "backend"))
        jobs = start(db + ("spring.application.name" to "jobs"))
    }

    @AfterAll
    fun stopProcesses() {
        jobs.close()
        backend.close()
    }

    /** Command-line args: they override application.yml (builder "properties" would be defaults, overridden by it). */
    private fun start(properties: Map<String, String>): ConfigurableApplicationContext =
        SpringApplicationBuilder(ThinTaskApplication::class.java)
            .run(*properties.map { (key, value) -> "--$key=$value" }.toTypedArray())

    @Test
    fun `backend and jobs writing the same aggregates concurrently keep every stream and read model consistent`() {
        val jdbc = backend.getBean(JdbcTemplate::class.java)
        val stored = StoredEvents(jdbc, "axon_domain_event_entry")
        val taskIds = List(50) { UUID.randomUUID().toString() }
        backend.getBean(BulkCommandGateway::class.java).sendAllAndWait(taskIds.map { CreateTaskCommand(it, "created") })

        val errors = Collections.synchronizedList(ArrayList<Throwable>())
        val start = CountDownLatch(1)
        val pool = Executors.newFixedThreadPool(3)
        val jobRounds = 20
        val uiCommandsPerThread = 100

        // jobs: whole-chunk renames of every task, retried on conflict
        pool.submit {
            runCatching {
                start.await()
                val bulk = jobs.getBean(BulkCommandGateway::class.java)
                repeat(jobRounds) { round ->
                    bulk.sendAllAndWait(taskIds.map { RenameTaskCommand(it, "job-$round") }, BulkOptions(concurrencyRetries = 20))
                }
            }.onFailure(errors::add)
        }
        // backend: two "users" renaming random tasks, one command at a time
        repeat(2) { user ->
            pool.submit {
                runCatching {
                    start.await()
                    val gateway = backend.getBean(CommandGateway::class.java)
                    repeat(uiCommandsPerThread) { i ->
                        gateway.sendAndWait<Any>(RenameTaskCommand(taskIds[Random.nextInt(taskIds.size)], "ui-$user-$i"))
                        Thread.sleep(Random.nextLong(5, 15)) // "think time" — still far busier than real users
                    }
                }.onFailure(errors::add)
            }
        }
        start.countDown()
        pool.shutdown()
        assertThat(pool.awaitTermination(5, TimeUnit.MINUTES)).isTrue()

        assertThat(errors).isEmpty()
        val summaries = backend.getBean(TaskSummaryRepository::class.java)
        var events = 0
        var outOfOrder = 0
        for (id in taskIds) {
            val stream = stored.forAggregate(id)
            events += stream.size
            // no gaps, no duplicates: 0, 1, 2, … n
            assertThat(stream.map { it.sequenceNumber }).isEqualTo((0L until stream.size).toList())
            // the read model shows the latest event (projections ran in the same transaction, in order)
            assertThat(summaries.findById(id).orElseThrow().title).isEqualTo(stream.last().payload["title"])
            outOfOrder += stream.zipWithNext().count { (a, b) -> a.globalIndex!! > b.globalIndex!! }
        }
        // every command produced exactly one event (titles are unique per command), none lost or duplicated
        assertThat(events).isEqualTo(taskIds.size * (1 + jobRounds) + 2 * uiCommandsPerThread)
        // pooled global_index blocks per process: commit order ≠ index order across processes (relevant for trackers)
        println("global_index lower than the previous event of the same aggregate: $outOfOrder of $events events")
    }
}
