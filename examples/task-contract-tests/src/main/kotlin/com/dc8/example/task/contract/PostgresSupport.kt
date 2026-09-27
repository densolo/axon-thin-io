package com.dc8.example.task.contract

import org.hibernate.boot.model.TypeContributions
import org.hibernate.dialect.PostgreSQLDialect
import org.hibernate.service.ServiceRegistry
import org.hibernate.type.SqlTypes
import org.hibernate.type.descriptor.jdbc.BinaryJdbcType
import org.springframework.test.context.DynamicPropertyRegistry
import org.testcontainers.containers.PostgreSQLContainer
import java.sql.DriverManager
import java.sql.Types

/**
 * Switches a test context to PostgreSQL when the build runs with `-Ppostgres` (system property
 * `thin.test.db=postgres`); otherwise does nothing and the context keeps its H2 settings.
 *
 * One container per test JVM (started on first use, removed by Testcontainers at exit), one database per test class
 * — the PostgreSQL counterpart of the per-context H2 databases.
 *
 * Usage, in a test class (or an abstract base):
 * ```
 * companion object {
 *     @JvmStatic @DynamicPropertySource
 *     fun db(registry: DynamicPropertyRegistry) = PostgresSupport.register(registry, "contract")
 * }
 * ```
 */
object PostgresSupport {

    val enabled: Boolean = System.getProperty("thin.test.db") == "postgres"

    private val container: PostgreSQLContainer<*> by lazy {
        PostgreSQLContainer("postgres:17-alpine")
            // a 1k all-or-nothing chunk must fit in the lock table; production should use the same setting
            .withCommand("postgres", "-c", "max_locks_per_transaction=256", "-c", "fsync=off")
            .also { it.start() }
    }

    private val created = HashSet<String>()

    fun register(registry: DynamicPropertyRegistry, database: String) {
        if (!enabled) return
        properties(database).forEach { (key, value) -> registry.add(key) { value } }
    }

    /** Spring properties pointing a context at [database] (created on first use) in the shared container. */
    fun properties(database: String): Map<String, String> = mapOf(
        "spring.datasource.url" to "${jdbcUrl(createDatabase(database))}?reWriteBatchedInserts=true",
        "spring.datasource.username" to container.username,
        "spring.datasource.password" to container.password,
        "spring.datasource.driver-class-name" to "org.postgresql.Driver",
        // thin apps: Axon tables from the reference DDL (absent in the Axon 4 app: Hibernate creates them there)
        "spring.sql.init.mode" to "always",
        "spring.sql.init.schema-locations" to "optional:classpath:axon-thin/schema/postgresql.sql",
        // Axon's @Lob byte[] as bytea (not oid) — what thin reads and writes
        "spring.jpa.properties.hibernate.dialect" to ByteaEnforcedPostgresSQLDialect::class.java.name,
        "spring.jpa.properties.hibernate.jdbc.batch_size" to "100",
        "spring.jpa.properties.hibernate.order_inserts" to "true",
        "spring.jpa.properties.hibernate.order_updates" to "true",
    )

    /** JDBC URL of [database] in the shared container, for tests that start extra contexts themselves. */
    fun jdbcUrl(database: String): String =
        "jdbc:postgresql://${container.host}:${container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)}/$database"

    @Synchronized
    private fun createDatabase(name: String): String {
        val database = "thin_${name.lowercase().replace(Regex("[^a-z0-9_]"), "_")}"
        if (created.add(database)) {
            DriverManager.getConnection(container.jdbcUrl, container.username, container.password).use { connection ->
                val exists = connection.prepareStatement("select 1 from pg_database where datname = ?").use {
                    it.setString(1, database)
                    it.executeQuery().use { rs -> rs.next() }
                }
                if (!exists) connection.createStatement().use { it.execute("create database $database") }
            }
        }
        return database
    }
}

/**
 * PostgreSQL dialect storing `@Lob byte[]` as `bytea` instead of `oid` — the variant recommended in Axon's reference
 * guide for Hibernate 6. Your project's axon-orm.xml / dialect must end up with bytea payload columns for thin.
 */
class ByteaEnforcedPostgresSQLDialect : PostgreSQLDialect() {

    override fun columnType(sqlTypeCode: Int): String =
        if (sqlTypeCode == SqlTypes.BLOB) "bytea" else super.columnType(sqlTypeCode)

    override fun castType(sqlTypeCode: Int): String =
        if (sqlTypeCode == SqlTypes.BLOB) "bytea" else super.castType(sqlTypeCode)

    override fun contributeTypes(typeContributions: TypeContributions, serviceRegistry: ServiceRegistry) {
        super.contributeTypes(typeContributions, serviceRegistry)
        typeContributions.typeConfiguration.jdbcTypeRegistry.addDescriptor(Types.BLOB, BinaryJdbcType.INSTANCE)
    }
}
