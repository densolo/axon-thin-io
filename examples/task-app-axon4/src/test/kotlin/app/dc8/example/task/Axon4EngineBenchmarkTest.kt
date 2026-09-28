package app.dc8.example.task

import app.dc8.example.task.contract.EngineBenchmark
import app.dc8.example.task.contract.PostgresSupport
import app.dc8.example.task.contract.SqlCountingConfiguration
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource

/** `mvn test -Ppostgres -pl examples/task-app-axon4 -Dtest=Axon4EngineBenchmarkTest` */
@EnabledIfSystemProperty(named = "thin.test.db", matches = "postgres")
@SpringBootTest(properties = ["task.snapshot-threshold=100"])
@Import(SqlCountingConfiguration::class)
class Axon4EngineBenchmarkTest : EngineBenchmark("Axon 4") {
    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun database(registry: DynamicPropertyRegistry) = PostgresSupport.register(registry, "engine_benchmark")
    }
}
