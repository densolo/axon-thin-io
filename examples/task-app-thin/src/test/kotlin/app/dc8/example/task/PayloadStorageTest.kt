package app.dc8.example.task

import app.dc8.axonthin.ProjectionMigrator
import app.dc8.axonthin.api.EventStoreBrowser
import app.dc8.axonthin.api.PayloadStorage
import app.dc8.axonthin.eventstore.ThinEventStore
import app.dc8.example.task.api.CreateTaskCommand
import app.dc8.example.task.api.RenameTaskCommand
import app.dc8.example.task.api.TaskCreatedEvent
import app.dc8.example.task.api.TaskRenamedEvent
import app.dc8.example.task.contract.PostgresSupport
import app.dc8.example.task.domain.Task
import app.dc8.example.task.projection.TaskSummaryRepository
import org.assertj.core.api.Assertions.assertThat
import org.axonframework.commandhandling.GenericCommandMessage
import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.common.jpa.SimpleEntityManagerProvider
import org.axonframework.eventhandling.DomainEventMessage
import org.axonframework.eventhandling.GenericDomainEventMessage
import org.axonframework.eventsourcing.eventstore.EventStore
import org.axonframework.eventsourcing.eventstore.jpa.JpaEventStorageEngine
import org.axonframework.serialization.Serializer
import org.axonframework.spring.messaging.unitofwork.SpringTransactionManager
import jakarta.persistence.EntityManager
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.util.UUID
import kotlin.streams.toList

/**
 * Thin on each way of storing `payload` / `meta_data`: detection, aggregates (+ snapshots), the EventStore bean,
 * EventStoreBrowser and projection replays — the same scenario per storage.
 */
@SpringBootTest(properties = ["task.snapshot-threshold=5"])
abstract class PayloadStorageTest(private val expected: PayloadStorage) {

    @Autowired lateinit var commandGateway: CommandGateway
    @Autowired lateinit var store: ThinEventStore
    @Autowired lateinit var eventStore: EventStore
    @Autowired lateinit var browser: EventStoreBrowser
    @Autowired lateinit var migrator: ProjectionMigrator
    @Autowired lateinit var summaries: TaskSummaryRepository
    @Autowired lateinit var jdbc: JdbcTemplate

    @BeforeEach
    fun clean() {
        listOf("axon_domain_event_entry", "axon_snapshot_event_entry").forEach { jdbc.update("delete from $it") }
        summaries.deleteAllInBatch()
    }

    protected fun id() = UUID.randomUUID().toString()

    @Test
    fun `the storage of both tables is detected`() {
        commandGateway.sendAndWait<String>(CreateTaskCommand(id(), "detect")) // detection happens on first use
        assertThat(store.eventColumns).isEqualTo(ThinEventStore.StoredColumns(expected, expected))
        assertThat(store.snapshotColumns).isEqualTo(ThinEventStore.StoredColumns(expected, expected))
    }

    @Test
    fun `aggregates, snapshots, EventStore and EventStoreBrowser work on this storage`() {
        val taskId = id()
        val create = GenericCommandMessage.asCommandMessage<CreateTaskCommand>(CreateTaskCommand(taskId, "r0"))
        commandGateway.sendAndWait<String>(create)
        (1..6).forEach { commandGateway.sendAndWait<Any>(RenameTaskCommand(taskId, "r$it")) } // loads + snapshot at 4

        assertThat(summaries.findById(taskId).orElseThrow().title).isEqualTo("r6")
        assertThat(eventStore.readEvents(taskId).asStream().toList().first().payload).isInstanceOf(Task::class.java)
        val decoded = browser.readEvents(taskId).toList()
        assertThat(decoded.map { it.payloadAs<TaskCreatedEvent>()?.title ?: it.payloadAs<TaskRenamedEvent>()?.title })
            .isEqualTo((0..6).map { "r$it" })
        assertThat(decoded.first().metaData).containsEntry("correlationId", create.identifier)
        assertThat(decoded.first().row.payload).startsWith("{").contains("\"title\":\"r0\"") // readable JSON text
    }

    @Test
    fun `projection replays read this storage`() {
        val ids = List(3) { id().also { taskId -> commandGateway.sendAndWait<String>(CreateTaskCommand(taskId, "t")) } }
        ids.forEach { commandGateway.sendAndWait<Any>(RenameTaskCommand(it, "renamed $it")) }
        summaries.deleteAllInBatch()

        migrator.migrate()

        assertThat(summaries.findAllById(ids).map { it.title }).containsExactlyInAnyOrderElementsOf(ids.map { "renamed $it" })
    }
}

/** `text` columns: readable JSON in the database (Axon 4 cannot map them). */
class TextPayloadStorageTest : PayloadStorageTest(PayloadStorage.TEXT) {
    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun database(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url") { "jdbc:h2:mem:payload-text;DB_CLOSE_DELAY=-1" }
            registry.add("spring.sql.init.schema-locations") { "classpath:schema/h2-text.sql" }
            if (PostgresSupport.enabled) {
                PostgresSupport.register(registry, "payload_text")
                registry.add("spring.sql.init.schema-locations") { "classpath:schema/postgresql-text.sql" }
            }
        }
    }
}

/**
 * PostgreSQL `oid` columns (large objects), created by Hibernate from Axon's own JPA mapping with the default
 * dialect — the schema of a project that never overrode `@Lob byte[]`. Axon's JpaEventStorageEngine and thin share it.
 */
@EnabledIfSystemProperty(named = "thin.test.db", matches = "postgres")
class OidPayloadStorageTest : PayloadStorageTest(PayloadStorage.OID) {

    @Autowired lateinit var entityManager: EntityManager
    @Autowired lateinit var transactionManager: PlatformTransactionManager
    @Autowired @Qualifier("eventSerializer") lateinit var serializer: Serializer

    private val axon by lazy {
        JpaEventStorageEngine.builder()
            .entityManagerProvider(SimpleEntityManagerProvider(entityManager))
            .transactionManager(SpringTransactionManager(transactionManager))
            .eventSerializer(serializer)
            .snapshotSerializer(serializer)
            .build()
    }

    private fun <T> tx(block: () -> T): T = TransactionTemplate(transactionManager).execute { block() }!!

    @Test
    fun `Axon and thin read and write each other's large objects`() {
        val taskId = id()
        tx { axon.appendEvents(listOf(GenericDomainEventMessage("Task", taskId, 0, TaskCreatedEvent(taskId, "by axon", null, null)))) }

        commandGateway.sendAndWait<Any>(RenameTaskCommand(taskId, "by thin")) // thin loads Axon's large object

        val events: List<DomainEventMessage<*>> = tx { axon.readEvents(taskId).asStream().toList() }
        assertThat(events.map { it.payload }).containsExactly(
            TaskCreatedEvent(taskId, "by axon", null, null),
            TaskRenamedEvent(taskId, "by thin"),
        )
    }

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun database(registry: DynamicPropertyRegistry) {
            PostgresSupport.register(registry, "payload_oid")
            registry.add("spring.sql.init.mode") { "never" }                          // Hibernate creates Axon's tables …
            registry.add("spring.jpa.mapping-resources") { "META-INF/axon-orm.xml" }  // … from Axon's entities
            registry.add("spring.jpa.properties.hibernate.dialect") { "org.hibernate.dialect.PostgreSQLDialect" } // oid
            // events appended through Axon's storage engine are never projected: log instead of failing on them
            registry.add("axon.thin.event-handler-error-mode") { "log" }
        }
    }
}
