package com.dc8.axonthin

import com.dc8.axonthin.aggregate.ThinSnapshotter
import com.dc8.axonthin.api.ChunkContext
import com.dc8.axonthin.eventstore.GlobalIndexAllocator
import com.dc8.axonthin.eventstore.PooledSequenceAllocator
import com.dc8.axonthin.eventstore.ThinEventStore
import com.fasterxml.jackson.databind.ObjectMapper
import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.eventsourcing.Snapshotter
import org.axonframework.serialization.AnnotationRevisionResolver
import org.axonframework.serialization.ChainingConverter
import org.axonframework.serialization.Serializer
import org.axonframework.serialization.json.JacksonSerializer
import org.springframework.beans.factory.ListableBeanFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import javax.sql.DataSource

@AutoConfiguration(
    afterName = [
        "org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration",
        "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
        "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
        "org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration",
        "org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration",
    ],
)
@EnableConfigurationProperties(ThinAxonProperties::class)
class ThinAxonAutoConfiguration {

    @Bean
    fun thinHandlerRegistry(): ThinHandlerRegistry = ThinHandlerRegistry()

    @Bean
    @ConditionalOnMissingBean(ChunkContext::class)
    fun thinChunkContext(): ChunkContext = ThinChunkContext()

    /**
     * Same serializer Axon's Spring Boot autoconfig builds for `axon.serializer.events=jackson`:
     * JacksonSerializer on the `defaultAxonObjectMapper` bean, else the application's ObjectMapper,
     * with AnnotationRevisionResolver (`@Revision`) and a ChainingConverter — so stored bytes are identical.
     */
    @Bean
    @Qualifier("eventSerializer")
    @ConditionalOnMissingBean(name = ["eventSerializer"])
    fun eventSerializer(beanFactory: ListableBeanFactory): Serializer {
        val mappers = beanFactory.getBeansOfType(ObjectMapper::class.java)
        val objectMapper = mappers["defaultAxonObjectMapper"] ?: mappers.values.firstOrNull() ?: ObjectMapper().findAndRegisterModules()
        return JacksonSerializer.builder()
            .revisionResolver(AnnotationRevisionResolver())
            .converter(ChainingConverter(javaClass.classLoader))
            .objectMapper(objectMapper)
            .build()
    }

    @Bean
    @ConditionalOnBean(DataSource::class)
    @ConditionalOnProperty(prefix = "axon.thin.event-store", name = ["enabled"], matchIfMissing = true)
    fun thinEventStore(
        dataSource: DataSource,
        @Qualifier("eventSerializer") serializer: Serializer,
        properties: ThinAxonProperties,
    ): ThinEventStore {
        val config = properties.eventStore
        val jdbc = JdbcTemplate(dataSource)
        val allocator = when (config.globalIndex.strategy) {
            ThinAxonProperties.GlobalIndex.Strategy.IDENTITY -> GlobalIndexAllocator.IDENTITY
            ThinAxonProperties.GlobalIndex.Strategy.SEQUENCE -> PooledSequenceAllocator(
                jdbc, dataSource, config.globalIndex.sequenceName ?: "${config.domainEventTableName}_seq", config.globalIndex.allocationSize,
            )
        }
        return ThinEventStore(
            jdbc, serializer, config.domainEventTableName, config.snapshotEventTableName, allocator,
            config.storeNonAggregateEvents,
        )
    }

    /** Target of the application's `SnapshotTriggerDefinition` beans (they take a `Snapshotter`). */
    @Bean
    @ConditionalOnBean(ThinEventStore::class)
    @ConditionalOnMissingBean(Snapshotter::class)
    fun thinSnapshotter(
        registry: ThinHandlerRegistry,
        eventStore: ThinEventStore,
        transactionManager: ObjectProvider<PlatformTransactionManager>,
    ): ThinSnapshotter = ThinSnapshotter(registry, eventStore, transactionManager.ifAvailable)

    @Bean
    fun thinEventGateway(
        registry: ThinHandlerRegistry,
        transactionManager: ObjectProvider<PlatformTransactionManager>,
        eventStore: ObjectProvider<ThinEventStore>,
        properties: ThinAxonProperties,
    ): ThinEventGateway = ThinEventGateway(
        registry,
        ThinTransactions(transactionManager.ifAvailable),
        properties.eventHandlerErrorMode,
        eventStore.ifAvailable,
    )

    @Bean
    @ConditionalOnMissingBean(CommandGateway::class)
    fun thinCommandGateway(
        registry: ThinHandlerRegistry,
        eventGateway: ThinEventGateway,
        transactionManager: ObjectProvider<PlatformTransactionManager>,
        eventStore: ObjectProvider<ThinEventStore>,
        properties: ThinAxonProperties,
    ): ThinCommandGateway = ThinCommandGateway(
        registry,
        eventGateway,
        ThinTransactions(transactionManager.ifAvailable),
        eventStore.ifAvailable,
        properties.concurrencyRetries,
    )
}

/** ProjectionMigrator — only with JPA (emptiness checks go through the EntityManagerFactory) and the event store. */
@AutoConfiguration(after = [ThinAxonAutoConfiguration::class])
@ConditionalOnClass(name = ["jakarta.persistence.EntityManagerFactory"])
class ThinProjectionMigratorAutoConfiguration {

    @Bean
    @ConditionalOnBean(ThinEventStore::class, jakarta.persistence.EntityManagerFactory::class)
    @ConditionalOnMissingBean
    fun projectionMigrator(
        registry: ThinHandlerRegistry,
        eventGateway: ThinEventGateway,
        eventStore: ThinEventStore,
        entityManagerFactory: jakarta.persistence.EntityManagerFactory,
        transactionManager: ObjectProvider<PlatformTransactionManager>,
        properties: ThinAxonProperties,
    ): ProjectionMigrator = ProjectionMigrator(
        registry, eventGateway, eventStore, entityManagerFactory,
        ThinTransactions(transactionManager.ifAvailable), properties.replayPageSize,
    )
}
