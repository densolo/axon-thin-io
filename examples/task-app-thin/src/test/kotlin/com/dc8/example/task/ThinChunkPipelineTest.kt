package com.dc8.example.task

import com.dc8.axonthin.api.BulkCommandGateway
import com.dc8.axonthin.api.BulkOptions
import com.dc8.example.task.api.CreateTaskCommand
import com.dc8.example.task.api.RenameTaskCommand
import com.dc8.example.task.api.TaskCreatedEvent
import com.dc8.example.task.api.TaskEvent
import com.dc8.example.task.api.TaskRenamedEvent
import com.dc8.example.task.contract.StoredEvents
import com.dc8.example.task.query.TaskQueryService
import net.ttddyy.dsproxy.ExecutionInfo
import net.ttddyy.dsproxy.QueryInfo
import net.ttddyy.dsproxy.listener.QueryExecutionListener
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.axonframework.commandhandling.CommandHandler
import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.eventhandling.EventHandler
import org.axonframework.eventhandling.EventMessage
import org.axonframework.modelling.command.ConcurrencyException
import org.axonframework.serialization.Serializer
import org.axonframework.serialization.SerializedObject
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.TransactionTemplate
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

/**
 * Thin-only chunk semantics (`sendAllAndWait`): set-based I/O, batch projections, end-of-chunk visibility and
 * retry on concurrent writers. Not part of the Axon contract suite — Axon 4 executes command by command.
 */
@SpringBootTest(
    properties = [
        "spring.datasource.url=jdbc:h2:mem:chunk-pipeline;DB_CLOSE_DELAY=-1",
        "task.snapshot-threshold=5",
    ],
)
@Import(ThinChunkPipelineTest.Config::class)
class ThinChunkPipelineTest {

    @Autowired lateinit var bulk: BulkCommandGateway
    @Autowired lateinit var commandGateway: CommandGateway
    @Autowired lateinit var queries: TaskQueryService
    @Autowired lateinit var jdbc: JdbcTemplate
    @Autowired lateinit var transactionManager: PlatformTransactionManager
    @Autowired lateinit var recorder: SqlRecorder
    @Autowired lateinit var batchProjection: BatchRecordingProjection
    @Autowired lateinit var interference: Interference
    @Autowired lateinit var visibility: VisibilityProbe

    private val stored by lazy { StoredEvents(jdbc, "axon_domain_event_entry") }

    @BeforeEach
    fun reset() {
        stored.deleteAll()
        batchProjection.calls.clear()
        interference.reset()
        visibility.seen.clear()
    }

    private fun id() = UUID.randomUUID().toString()

    private val eventReads = { sql: String -> sql.startsWith("select") && sql.contains("from axon_domain_event_entry") }
    private val snapshotReads = { sql: String -> sql.startsWith("select") && sql.contains("from axon_snapshot_event_entry") }
    private val eventInserts = { sql: String -> sql.startsWith("insert into axon_domain_event_entry") }
    private val sequenceFetches = { sql: String -> sql.contains("next value for axon_domain_event_entry_seq") }

    private fun createTasks(count: Int): List<String> =
        bulk.sendAllAndWait(List(count) { CreateTaskCommand(id(), "t$it") }).map { it as String }

    // ---- set-based I/O ------------------------------------------------------------------------------------------------

    @Test
    fun `a chunk loads, appends and allocates indexes in a constant number of queries`() {
        val ids = createTasks(100)
        recorder.clear()

        bulk.sendAllAndWait(ids.map { RenameTaskCommand(it, "renamed $it") })

        assertThat(recorder.count(snapshotReads)).isEqualTo(1)
        assertThat(recorder.count(eventReads)).isEqualTo(1)
        assertThat(recorder.count(eventInserts)).isEqualTo(1) // one JDBC batch …
        assertThat(recorder.rows(eventInserts)).isEqualTo(100) // … carrying all 100 events
        assertThat(recorder.count(sequenceFetches)).isLessThanOrEqualTo(1)
        assertThat(ids.map { queries.summary(it)!!.title }).allSatisfy { assertThat(it).startsWith("renamed") }
    }

    @Test
    fun `preload combines snapshots with the events after them`() {
        val withSnapshot = createTasks(3)
        repeat(4) { round -> bulk.sendAllAndWait(withSnapshot.map { RenameTaskCommand(it, "r$round") }) } // seq 4 → snapshot
        assertThat(withSnapshot).allSatisfy { assertThat(stored.snapshots(it)).hasSize(1) }
        bulk.sendAllAndWait(withSnapshot.map { RenameTaskCommand(it, "after snapshot") }) // seq 5, after the snapshot
        val plain = createTasks(3)
        recorder.clear()

        // same title → no event only if the state (snapshot + later events) was restored correctly
        val results = bulk.sendAllAndWait(
            withSnapshot.map { RenameTaskCommand(it, "after snapshot") } + plain.map { RenameTaskCommand(it, "t-new") },
        )

        assertThat(results).hasSize(6)
        assertThat(recorder.count(snapshotReads)).isEqualTo(1)
        assertThat(recorder.count(eventReads)).isEqualTo(1)
        assertThat(withSnapshot).allSatisfy { assertThat(stored.forAggregate(it).last().sequenceNumber).isEqualTo(5L) }
        assertThat(plain).allSatisfy { assertThat(queries.summary(it)!!.title).isEqualTo("t-new") }
    }

    // ---- projections --------------------------------------------------------------------------------------------------

    @Test
    fun `a batch projection receives the chunk's events as one ordered list`() {
        val a = id()
        val b = id()

        bulk.sendAllAndWait(listOf(CreateTaskCommand(a, "a"), RenameTaskCommand(a, "a2"), CreateTaskCommand(b, "b")))

        assertThat(batchProjection.calls).hasSize(1)
        assertThat(batchProjection.calls.single().map { it.payload::class.simpleName to it.payload.taskId }).containsExactly(
            "TaskCreatedEvent" to a,
            "TaskRenamedEvent" to a,
            "TaskCreatedEvent" to b,
        )
    }

    @Test
    fun `projections reflect a chunk only after its last command`() {
        val taskId = id()

        bulk.sendAllAndWait(listOf(CreateTaskCommand(taskId, "new"), ProbeVisibilityCommand(taskId)))

        assertThat(visibility.seen).containsExactly(false) // inside the chunk: summary not written yet
        assertThat(queries.summary(taskId)).isNotNull     // after the chunk
    }

    // ---- concurrency --------------------------------------------------------------------------------------------------

    @Test
    fun `a chunk that lost a race to another writer is re-run on top of the latest state`() {
        val taskId = createTasks(1).single()
        interference.armFor(taskId, times = 1)

        bulk.sendAllAndWait(
            listOf(RenameTaskCommand(taskId, "mine"), InterfereCommand(taskId)),
            BulkOptions(concurrencyRetries = 2),
        )

        assertThat(interference.writes.get()).isEqualTo(1)
        assertThat(stored.forAggregate(taskId).map { it.sequenceNumber to it.payload["title"] })
            .containsExactly(0L to "t0", 1L to "foreign", 2L to "mine")
        assertThat(queries.summary(taskId)!!.title).isEqualTo("mine")
    }

    @Test
    fun `without retries the conflict is rethrown and nothing of the chunk is stored`() {
        val taskId = createTasks(1).single()
        interference.armFor(taskId, times = 1)

        assertThatThrownBy {
            bulk.sendAllAndWait(listOf(RenameTaskCommand(taskId, "mine"), InterfereCommand(taskId)))
        }.isInstanceOf(ConcurrencyException::class.java)

        assertThat(stored.forAggregate(taskId).map { it.payload["title"] }).containsExactly("t0", "foreign")
    }

    @Test
    fun `no retry inside the caller's transaction`() {
        val taskId = createTasks(1).single()
        interference.armFor(taskId, times = 1)

        assertThatThrownBy {
            TransactionTemplate(transactionManager).executeWithoutResult {
                bulk.sendAllAndWait(
                    listOf(RenameTaskCommand(taskId, "mine"), InterfereCommand(taskId)),
                    BulkOptions(concurrencyRetries = 3),
                )
            }
        }.isInstanceOf(ConcurrencyException::class.java)
        assertThat(interference.writes.get()).isEqualTo(1)
    }

    // ---- test fixtures ------------------------------------------------------------------------------------------------

    data class InterfereCommand(val taskId: String)
    data class ProbeVisibilityCommand(val taskId: String)

    /** Batch projection: one call per chunk with every TaskEvent, in order. */
    class BatchRecordingProjection {
        val calls = CopyOnWriteArrayList<List<EventMessage<TaskEvent>>>()

        @EventHandler
        fun on(events: List<EventMessage<TaskEvent>>) {
            calls += events.toList()
        }
    }

    /** Checks, from inside a chunk, whether the summary of a task created earlier in the chunk is visible. */
    class VisibilityProbe(private val queries: TaskQueryService) {
        val seen = CopyOnWriteArrayList<Boolean>()

        @CommandHandler
        fun handle(command: ProbeVisibilityCommand) {
            seen += queries.summary(command.taskId) != null
        }
    }

    /** Simulates another process: commits a rename for the same aggregate while the chunk is running. */
    class Interference(
        private val jdbc: JdbcTemplate,
        transactionManager: PlatformTransactionManager,
        @Qualifier("eventSerializer") private val serializer: Serializer,
    ) {
        private val newTransaction = TransactionTemplate(transactionManager)
            .apply { propagationBehavior = TransactionDefinition.PROPAGATION_REQUIRES_NEW }
        private var target: String? = null
        private var remaining = 0
        private val index = AtomicInteger(10_000_000)
        val writes = AtomicInteger()

        fun armFor(taskId: String, times: Int) {
            target = taskId
            remaining = times
        }

        fun reset() {
            target = null
            remaining = 0
            writes.set(0)
        }

        @CommandHandler
        fun handle(command: InterfereCommand) {
            if (command.taskId != target || remaining == 0) return
            remaining--
            newTransaction.executeWithoutResult {
                val seq = jdbc.queryForObject(
                    "select max(sequence_number) from axon_domain_event_entry where aggregate_identifier = ?",
                    Long::class.java, command.taskId,
                )!! + 1
                val payload: SerializedObject<ByteArray> =
                    serializer.serialize(TaskRenamedEvent(command.taskId, "foreign"), ByteArray::class.java)
                jdbc.update(
                    "insert into axon_domain_event_entry (global_index, event_identifier, payload_type, payload_revision, " +
                        "payload, meta_data, time_stamp, aggregate_identifier, sequence_number, type) values (?,?,?,?,?,?,?,?,?,?)",
                    index.incrementAndGet(), UUID.randomUUID().toString(), payload.type.name, payload.type.revision,
                    payload.data, "{}".toByteArray(), java.time.Instant.now().toString(), command.taskId, seq, "Task",
                )
            }
            writes.incrementAndGet()
        }
    }

    /** Every statement the application sends, with its JDBC batch size. */
    class SqlRecorder : QueryExecutionListener {
        private val executions = CopyOnWriteArrayList<Pair<String, Int>>()

        override fun beforeQuery(execInfo: ExecutionInfo, queryInfoList: List<QueryInfo>) = Unit

        override fun afterQuery(execInfo: ExecutionInfo, queryInfoList: List<QueryInfo>) {
            queryInfoList.forEach { executions += it.query.trim().lowercase() to maxOf(1, execInfo.batchSize) }
        }

        fun clear() = executions.clear()

        /** Round trips whose SQL matches. */
        fun count(match: (String) -> Boolean): Int = executions.count { match(it.first) }

        /** Rows sent by matching statements (JDBC batch size). */
        fun rows(match: (String) -> Boolean): Int = executions.filter { match(it.first) }.sumOf { it.second }
    }

    @TestConfiguration(proxyBeanMethods = false)
    class Config {
        @Bean fun sqlRecorder() = SqlRecorder()
        @Bean fun batchRecordingProjection() = BatchRecordingProjection()
        @Bean fun visibilityProbe(queries: TaskQueryService) = VisibilityProbe(queries)

        @Bean
        fun interference(
            jdbc: JdbcTemplate,
            transactionManager: PlatformTransactionManager,
            @Qualifier("eventSerializer") serializer: Serializer,
        ) = Interference(jdbc, transactionManager, serializer)

        companion object {
            @JvmStatic
            @Bean
            fun countingDataSource(recorder: org.springframework.beans.factory.ObjectProvider<SqlRecorder>): BeanPostProcessor =
                object : BeanPostProcessor {
                    override fun postProcessAfterInitialization(bean: Any, beanName: String): Any =
                        if (bean is DataSource && beanName == "dataSource") {
                            ProxyDataSourceBuilder.create(bean).listener(object : QueryExecutionListener {
                                override fun beforeQuery(e: ExecutionInfo, q: List<QueryInfo>) = Unit
                                override fun afterQuery(e: ExecutionInfo, q: List<QueryInfo>) = recorder.getObject().afterQuery(e, q)
                            }).build()
                        } else bean
                }
        }
    }
}
