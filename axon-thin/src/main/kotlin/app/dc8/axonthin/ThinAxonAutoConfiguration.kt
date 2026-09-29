package app.dc8.axonthin

import app.dc8.axonthin.aggregate.ThinSnapshotter
import app.dc8.axonthin.api.ChunkContext
import app.dc8.axonthin.api.EventStoreBrowser
import app.dc8.axonthin.eventstore.SerializerEventDecoder
import app.dc8.axonthin.eventstore.EventStoreSchema
import app.dc8.axonthin.eventstore.GlobalIndexAllocator
import app.dc8.axonthin.eventstore.ThinAxonEventStore
import app.dc8.axonthin.eventstore.PooledSequenceAllocator
import app.dc8.axonthin.eventstore.ThinEventStore
import com.fasterxml.jackson.databind.ObjectMapper
import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.eventsourcing.Snapshotter
import org.axonframework.eventsourcing.eventstore.EventStore
import org.axonframework.serialization.AnnotationRevisionResolver
import org.axonframework.serialization.ChainingConverter
import org.axonframework.serialization.Serializer
import org.axonframework.serialization.json.JacksonSerializer
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization
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
     * The event serializer, resolved like Axon 4's Spring Boot autoconfig so stored bytes are identical:
     * 1. a `Serializer` bean named or `@Qualifier`-ed `eventSerializer` (then this method is skipped);
     * 2. else one named/qualified `messageSerializer`;
     * 3. else the application's general `Serializer` bean (e.g. `@Bean @Qualifier("serializer") fun axonJacksonSerializer()`),
     *    the `@Primary` one if there are several;
     * 4. else what Axon builds for `jackson`: JacksonSerializer on the `defaultAxonObjectMapper` bean, else the primary
     *    ObjectMapper, with AnnotationRevisionResolver and a ChainingConverter.
     */
    @Bean
    @Qualifier("eventSerializer")
    @ConditionalOnMissingBean(name = ["eventSerializer"])
    fun eventSerializer(beanFactory: ConfigurableListableBeanFactory): Serializer =
        applicationSerializer(beanFactory) ?: defaultSerializer(beanFactory)

    private fun applicationSerializer(beanFactory: ConfigurableListableBeanFactory): Serializer? {
        val candidates = beanFactory.getBeanNamesForType(Serializer::class.java, true, false)
            .filter { it != "eventSerializer" } // this method's own bean
            .associateWith { name -> setOfNotNull(name, beanFactory.findAnnotationOnBean(name, Qualifier::class.java)?.value) }
        fun qualified(qualifier: String) = candidates.filterValues { qualifier in it }.keys.firstOrNull()
        val name = qualified("eventSerializer") ?: qualified("messageSerializer") ?: run {
            val general = candidates.keys.toList()
            when {
                general.size <= 1 -> general.firstOrNull()
                else -> general.firstOrNull { beanFactory.getMergedBeanDefinition(it).isPrimary }
                    ?: qualified("serializer")
                    ?: error("Several Serializer beans $general: mark the one for events @Primary or name it eventSerializer")
            }
        }
        return name?.let { beanFactory.getBean(it, Serializer::class.java) }
    }

    private fun defaultSerializer(beanFactory: ConfigurableListableBeanFactory): Serializer {
        val objectMapper = beanFactory.getBeanProvider(ObjectMapper::class.java).let { mappers ->
            (if (beanFactory.containsBean("defaultAxonObjectMapper")) beanFactory.getBean("defaultAxonObjectMapper", ObjectMapper::class.java) else null)
                ?: mappers.ifUnique // the primary one when there are several
                ?: ObjectMapper().findAndRegisterModules()
        }
        return JacksonSerializer.builder()
            .revisionResolver(AnnotationRevisionResolver())
            .converter(ChainingConverter(javaClass.classLoader))
            .objectMapper(objectMapper)
            .build()
    }

    @Bean
    @ConditionalOnBean(DataSource::class)
    @ConditionalOnProperty(prefix = "axon.thin.event-store", name = ["enabled"], matchIfMissing = true)
    @DependsOnDatabaseInitialization // Liquibase / Flyway / spring.sql.init run first, so `schema` sees the final schema
    fun thinEventStore(
        dataSource: DataSource,
        @Qualifier("eventSerializer") serializer: Serializer,
        properties: ThinAxonProperties,
    ): ThinEventStore {
        val config = properties.eventStore
        EventStoreSchema(dataSource, config).apply(config.schema)
        val jdbc = JdbcTemplate(dataSource)
        val allocator = when (config.globalIndex.strategy) {
            ThinAxonProperties.GlobalIndex.Strategy.IDENTITY -> GlobalIndexAllocator.IDENTITY
            ThinAxonProperties.GlobalIndex.Strategy.SEQUENCE -> PooledSequenceAllocator(
                jdbc, dataSource, config.globalIndex.sequenceName ?: "${config.domainEventTableName}_seq", config.globalIndex.allocationSize,
            )
        }
        return ThinEventStore(
            jdbc, serializer, config.domainEventTableName, config.snapshotEventTableName, allocator,
            config.storeNonAggregateEvents, config.payloadColumn.storage,
        )
    }

    /** Raw, paged read access to the event table (debug/admin pages). */
    @Bean
    @ConditionalOnBean(DataSource::class)
    @ConditionalOnProperty(prefix = "axon.thin.event-store", name = ["enabled"], matchIfMissing = true)
    @ConditionalOnMissingBean
    fun eventStoreBrowser(
        dataSource: DataSource,
        properties: ThinAxonProperties,
        @Qualifier("eventSerializer") serializer: Serializer,
    ): EventStoreBrowser =
        EventStoreBrowser(
            dataSource, properties.eventStore.domainEventTableName, SerializerEventDecoder(serializer),
            properties.eventStore.payloadColumn.storage,
        )

    /** Axon's EventStore interface for application code that reads streams or publishes domain events directly. */
    @Bean
    @ConditionalOnBean(ThinEventStore::class)
    @ConditionalOnMissingBean(EventStore::class)
    fun thinAxonEventStore(eventStore: ThinEventStore, eventGateway: ThinEventGateway): EventStore =
        ThinAxonEventStore(eventStore, eventGateway)

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
        ThinTransactions(transactionManager.ifAvailable), properties.replayPageSize, properties.replayOrder,
    )
}
