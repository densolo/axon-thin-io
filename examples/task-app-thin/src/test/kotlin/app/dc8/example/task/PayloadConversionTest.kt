package app.dc8.example.task

import app.dc8.axonthin.api.EventStoreBrowser
import app.dc8.axonthin.api.PayloadStorage
import app.dc8.axonthin.eventstore.ThinEventStore
import app.dc8.example.task.api.CreateTaskCommand
import app.dc8.example.task.api.RenameTaskCommand
import app.dc8.example.task.api.TaskRenamedEvent
import app.dc8.example.task.contract.PostgresSupport
import app.dc8.example.task.domain.Task
import app.dc8.example.task.projection.TaskSummaryRepository
import org.assertj.core.api.Assertions.assertThat
import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.eventsourcing.eventstore.EventStore
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.jdbc.core.JdbcTemplate
import java.io.File
import java.sql.DriverManager
import java.util.UUID

/**
 * Runs the Liquibase changesets in `docs/liquibase` (formatted SQL, executed here as plain SQL) against data written by
 * a running application, then starts a fresh one: it must detect the new column type, read everything written before
 * (events and snapshots) and keep writing. PostgreSQL only.
 */
@EnabledIfSystemProperty(named = "thin.test.db", matches = "postgres")
class PayloadConversionTest {

    private val changesets = File("../../docs/liquibase")

    @Test
    fun `oid to bytea, and its rollback back to oid`() {
        val db = "conv_oid_bytea"
        val written = write(db, schema = "classpath:schema/postgresql-oid.sql", expected = PayloadStorage.OID)

        run(db, "oid-to-bytea.sql")
        assertThat(largeObjects(db)).isZero() // the changeset frees them
        verify(db, written, PayloadStorage.BINARY)

        run(db, "oid-to-bytea.sql", rollback = true)
        verify(db, written, PayloadStorage.OID)
    }

    @Test
    fun `oid to text`() {
        val db = "conv_oid_text"
        val written = write(db, schema = "classpath:schema/postgresql-oid.sql", expected = PayloadStorage.OID)

        run(db, "oid-to-text.sql")

        assertThat(largeObjects(db)).isZero()
        verify(db, written, PayloadStorage.TEXT)
    }

    @Test
    fun `bytea to text and back`() {
        val db = "conv_bytea_text"
        val written = write(db, schema = "optional:classpath:axon-thin/schema/postgresql.sql", expected = PayloadStorage.BINARY)

        run(db, "bytea-to-text.sql")
        verify(db, written, PayloadStorage.TEXT)

        run(db, "text-to-bytea.sql")
        verify(db, written, PayloadStorage.BINARY)
    }

    // ---- scenario -----------------------------------------------------------------------------------------------------

    /** Ids of tasks with snapshots (7 events each) written on the original column type. */
    private fun write(db: String, schema: String, expected: PayloadStorage): List<String> = app(db, schema).use { ctx ->
        val gateway = ctx.getBean(CommandGateway::class.java)
        val ids = List(3) { UUID.randomUUID().toString() }
        ids.forEach { id ->
            gateway.sendAndWait<String>(CreateTaskCommand(id, "r0"))
            (1..6).forEach { gateway.sendAndWait<Any>(RenameTaskCommand(id, "r$it")) } // snapshot at seq 4
        }
        assertThat(ctx.getBean(ThinEventStore::class.java).eventColumns.payload).isEqualTo(expected)
        ids
    }

    /** A fresh application on the converted tables: detects the new type, reads the old data, keeps writing. */
    private fun verify(db: String, ids: List<String>, expected: PayloadStorage) = app(db).use { ctx ->
        val gateway = ctx.getBean(CommandGateway::class.java)
        ids.forEach { id -> gateway.sendAndWait<Any>(RenameTaskCommand(id, "after $expected")) } // loads snapshot + events

        val store = ctx.getBean(ThinEventStore::class.java)
        assertThat(store.eventColumns).isEqualTo(ThinEventStore.StoredColumns(expected, expected))
        assertThat(store.snapshotColumns).isEqualTo(ThinEventStore.StoredColumns(expected, expected))
        assertThat(ctx.getBean(EventStore::class.java).readEvents(ids.first()).next().payload).isInstanceOf(Task::class.java)
        val titles = ctx.getBean(EventStoreBrowser::class.java).readEvents(ids.first()).mapNotNull { it.payloadAs<TaskRenamedEvent>()?.title }.toList()
        assertThat(titles).startsWith("r1", "r2", "r3", "r4", "r5", "r6").contains("after $expected")
        assertThat(ctx.getBean(TaskSummaryRepository::class.java).findAllById(ids).map { it.title }).containsOnly("after $expected")
    }

    private fun app(db: String, schema: String? = null): ConfigurableApplicationContext {
        val properties = PostgresSupport.properties(db) + mapOf(
            "spring.jpa.hibernate.ddl-auto" to "update", // read models survive between the two applications
            "task.snapshot-threshold" to "5",
        ) + (schema?.let { mapOf("spring.sql.init.schema-locations" to it) } ?: emptyMap())
        return SpringApplicationBuilder(ThinTaskApplication::class.java)
            .run(*properties.map { (k, v) -> "--$k=$v" }.toTypedArray())
    }

    // ---- Liquibase formatted SQL as plain SQL -------------------------------------------------------------------------

    /** Executes a changeset file (or its `--rollback` lines) with `axon_prefix = axon_`. */
    private fun run(db: String, file: String, rollback: Boolean = false) {
        val lines = File(changesets, file).readLines()
        val sql = if (rollback) lines.filter { it.startsWith("--rollback ") }.map { it.removePrefix("--rollback ") }
        else lines.filterNot { it.trimStart().startsWith("--") }
        jdbc(db) { jdbc ->
            sql.joinToString("\n").replace("\${axon_prefix}", "axon_")
                .split(';').map { it.trim() }.filter { it.isNotEmpty() }
                .forEach { statement -> jdbc.execute(statement) }
        }
    }

    private fun largeObjects(db: String): Long =
        jdbc(db) { it.queryForObject("select count(*) from pg_largeobject_metadata", Long::class.java)!! }

    private fun <T> jdbc(db: String, block: (JdbcTemplate) -> T): T {
        val p = PostgresSupport.properties(db)
        return DriverManager.getConnection(p.getValue("spring.datasource.url"), p.getValue("spring.datasource.username"),
            p.getValue("spring.datasource.password")).use { connection ->
            block(JdbcTemplate(org.springframework.jdbc.datasource.SingleConnectionDataSource(connection, true)))
        }
    }
}
