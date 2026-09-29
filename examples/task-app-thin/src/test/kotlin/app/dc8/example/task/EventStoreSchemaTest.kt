package app.dc8.example.task

import app.dc8.axonthin.api.PayloadStorage
import app.dc8.axonthin.eventstore.ThinEventStore
import app.dc8.example.task.api.CreateTaskCommand
import app.dc8.example.task.api.RenameTaskCommand
import app.dc8.example.task.contract.PostgresSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.axonframework.commandhandling.gateway.CommandGateway
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.SingleConnectionDataSource
import java.sql.DriverManager
import java.util.UUID

/**
 * `axon.thin.event-store.schema`: create-if-missing (dev/tests, no Liquibase needed) and validate (production).
 * Each scenario starts the application on a fresh database; H2 by default, PostgreSQL with -Ppostgres.
 */
class EventStoreSchemaTest {

    // ---- create-if-missing ----------------------------------------------------------------------------------------

    @Test
    fun `create-if-missing builds a working schema with text payloads`() = createsWorkingSchema("text", PayloadStorage.TEXT)

    @Test
    fun `create-if-missing builds a working schema with binary payloads`() = createsWorkingSchema("binary", PayloadStorage.BINARY)

    @Test
    @EnabledIfSystemProperty(named = "thin.test.db", matches = "postgres")
    fun `create-if-missing builds a working schema with oid payloads`() = createsWorkingSchema("oid", PayloadStorage.OID)

    private fun createsWorkingSchema(column: String, expected: PayloadStorage) {
        val db = database("create_$column")
        val props = db + mapOf("axon.thin.event-store.schema" to "create-if-missing", "axon.thin.event-store.payload-column" to column)
        val taskId = UUID.randomUUID().toString()

        app(props).use { ctx ->
            val gateway = ctx.getBean(CommandGateway::class.java)
            gateway.sendAndWait<String>(CreateTaskCommand(taskId, "created"))
            gateway.sendAndWait<Any>(RenameTaskCommand(taskId, "renamed"))
            assertThat(ctx.getBean(ThinEventStore::class.java).eventColumns.payload).isEqualTo(expected)
        }
        // a second start on the same database: nothing created again, nothing lost, still valid
        app(props).use { ctx ->
            ctx.getBean(CommandGateway::class.java).sendAndWait<Any>(RenameTaskCommand(taskId, "after restart"))
        }
        assertThat(sql(db) { it.queryForObject("select count(*) from axon_domain_event_entry", Long::class.java) }).isEqualTo(3)
    }

    @Test
    fun `create-if-missing leaves an existing schema alone`() {
        val db = database("existing")
        app(db + ("axon.thin.event-store.schema" to "create-if-missing")).close() // binary schema
        sql(db) { it.update("insert into axon_snapshot_event_entry (sequence_number, aggregate_identifier, event_identifier, payload_type, time_stamp, type, payload) values (0, 'x', 'marker', 't', 't', 'T', ?)", "{}".toByteArray()) }

        app(db + mapOf("axon.thin.event-store.schema" to "create-if-missing")).close()

        assertThat(sql(db) { it.queryForObject("select count(*) from axon_snapshot_event_entry where event_identifier = 'marker'", Long::class.java) }).isEqualTo(1)
    }

    // ---- validate -------------------------------------------------------------------------------------------------

    @Test
    fun `validate passes on a correct schema`() {
        val db = database("valid")
        app(db + ("axon.thin.event-store.schema" to "create-if-missing")).close()

        app(db + ("axon.thin.event-store.schema" to "validate")).close()
    }

    @Test
    fun `validate fails on an empty database, listing what is missing`() {
        val db = database("empty")

        assertThatThrownBy { app(db + ("axon.thin.event-store.schema" to "validate")) }
            .rootCause()
            .hasMessageContaining("table axon_domain_event_entry does not exist")
            .hasMessageContaining("table axon_snapshot_event_entry does not exist")
            .hasMessageContaining("sequence axon_domain_event_entry_seq does not exist")
    }

    @Test
    fun `validate fails without the unique index on aggregate id and sequence`() {
        val db = database("no_index")
        sql(db) { jdbc -> handmadeSchema(uniqueIndex = false, increment = 50).forEach(jdbc::execute) }

        assertThatThrownBy { app(db + ("axon.thin.event-store.schema" to "validate")) }
            .rootCause()
            .hasMessageContaining("has no unique index on (aggregate_identifier, sequence_number)")
    }

    @Test
    fun `validate fails when the sequence increment differs from allocation-size`() {
        val db = database("increment")
        sql(db) { jdbc -> handmadeSchema(uniqueIndex = true, increment = 1).forEach(jdbc::execute) }

        assertThatThrownBy { app(db + ("axon.thin.event-store.schema" to "validate")) }
            .rootCause()
            .hasMessageContaining("increments by 1, but axon.thin.event-store.global-index.allocation-size is 50")
        // and it starts once they agree
        app(db + mapOf("axon.thin.event-store.schema" to "validate", "axon.thin.event-store.global-index.allocation-size" to "1")).close()
    }

    // ---- helpers --------------------------------------------------------------------------------------------------

    /** A schema made by hand (as a legacy database might be), with `varchar` payloads. */
    private fun handmadeSchema(uniqueIndex: Boolean, increment: Int) = listOf(
        "create sequence axon_domain_event_entry_seq start with 1 increment by $increment",
        """create table axon_domain_event_entry (global_index bigint not null primary key, sequence_number bigint not null,
           aggregate_identifier varchar(255) not null, event_identifier varchar(255) not null unique, payload_revision varchar(255),
           payload_type varchar(255) not null, time_stamp varchar(255) not null, type varchar(255), meta_data varchar(4000),
           payload varchar(4000) not null${if (uniqueIndex) ", constraint ev_agg_seq unique (aggregate_identifier, sequence_number)" else ""})""",
        """create table axon_snapshot_event_entry (sequence_number bigint not null, aggregate_identifier varchar(255) not null,
           event_identifier varchar(255) not null unique, payload_revision varchar(255), payload_type varchar(255) not null,
           time_stamp varchar(255) not null, type varchar(255) not null, meta_data varchar(4000), payload varchar(4000) not null,
           primary key (sequence_number, aggregate_identifier, type))""",
    )

    /** A fresh database; the app's own schema script is off, so only thin (or the test) creates Axon's tables. */
    private fun database(name: String): Map<String, String> =
        (if (PostgresSupport.enabled) PostgresSupport.properties("schema_$name")
        else mapOf("spring.datasource.url" to "jdbc:h2:mem:schema-$name;DB_CLOSE_DELAY=-1")) +
            mapOf("spring.sql.init.mode" to "never", "spring.jpa.hibernate.ddl-auto" to "update")

    private fun app(properties: Map<String, String>): ConfigurableApplicationContext =
        SpringApplicationBuilder(ThinTaskApplication::class.java)
            .run(*(properties + ("axon.thin.event-store.table-prefix" to "axon_")).map { (k, v) -> "--$k=$v" }.toTypedArray())

    private fun <T> sql(db: Map<String, String>, block: (JdbcTemplate) -> T): T =
        DriverManager.getConnection(db.getValue("spring.datasource.url"), db["spring.datasource.username"] ?: "sa",
            db["spring.datasource.password"] ?: "").use { connection ->
            block(JdbcTemplate(SingleConnectionDataSource(connection, true)))
        }
}
