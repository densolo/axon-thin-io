package app.dc8.axonthin.v4

import app.dc8.axonthin.api.BulkCommandGateway
import app.dc8.axonthin.api.ChunkContext
import app.dc8.axonthin.api.EventStoreBrowser
import org.axonframework.commandhandling.gateway.CommandGateway
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.axonframework.serialization.Serializer
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.transaction.PlatformTransactionManager
import javax.sql.DataSource

@AutoConfiguration(
    afterName = [
        "org.axonframework.springboot.autoconfig.AxonAutoConfiguration",
        "org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration",
        "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
        "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
    ],
)
class Axon4BulkAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(ChunkContext::class)
    fun axon4ChunkContext(): ChunkContext = Axon4ChunkContext()

    /**
     * The same event-table browser thin provides, over the table Axon's JPA engine writes, decoding with Axon's
     * `eventSerializer`. Configure the table with thin's keys (`axon.thin.event-store.table-prefix`,
     * `domain-event-table`) so nothing changes at the engine switch.
     */
    @Bean
    @ConditionalOnBean(DataSource::class)
    @ConditionalOnMissingBean
    fun axon4EventStoreBrowser(
        dataSource: DataSource,
        @Qualifier("eventSerializer") serializer: Serializer,
        @Value("\${axon.thin.event-store.table-prefix:}") tablePrefix: String,
        @Value("\${axon.thin.event-store.domain-event-table:domain_event_entry}") table: String,
    ): EventStoreBrowser = EventStoreBrowser(dataSource, tablePrefix + table, SerializerEventDecoder(serializer))

    @Bean
    @ConditionalOnMissingBean(BulkCommandGateway::class)
    @ConditionalOnBean(CommandGateway::class, PlatformTransactionManager::class)
    fun axon4BulkCommandGateway(
        commandGateway: CommandGateway,
        transactionManager: PlatformTransactionManager,
    ): BulkCommandGateway = Axon4BulkCommandGateway(commandGateway, transactionManager)
}
