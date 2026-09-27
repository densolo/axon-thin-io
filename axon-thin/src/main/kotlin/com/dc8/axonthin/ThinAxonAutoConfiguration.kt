package com.dc8.axonthin

import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.eventhandling.gateway.EventGateway
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.transaction.PlatformTransactionManager

@ConfigurationProperties("axon.thin")
class ThinAxonProperties {

    /**
     * What happens when an `@EventHandler` throws. Axon 4 defaults to logging (LoggingErrorHandler);
     * PROPAGATE matches `PropagatingErrorHandler` and rolls the command's transaction back.
     */
    var eventHandlerErrorMode: EventHandlerErrorMode = EventHandlerErrorMode.LOG

    enum class EventHandlerErrorMode { LOG, PROPAGATE }
}

@AutoConfiguration(
    afterName = [
        "org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration",
        "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
        "org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration",
    ],
)
@EnableConfigurationProperties(ThinAxonProperties::class)
class ThinAxonAutoConfiguration {

    @Bean
    fun thinHandlerRegistry(): ThinHandlerRegistry = ThinHandlerRegistry()

    @Bean
    @ConditionalOnMissingBean(EventGateway::class)
    fun thinEventGateway(
        registry: ThinHandlerRegistry,
        transactionManager: ObjectProvider<PlatformTransactionManager>,
        properties: ThinAxonProperties,
    ): ThinEventGateway =
        ThinEventGateway(registry, ThinTransactions(transactionManager.ifAvailable), properties.eventHandlerErrorMode)

    @Bean
    @ConditionalOnMissingBean(CommandGateway::class)
    fun thinCommandGateway(
        registry: ThinHandlerRegistry,
        eventGateway: ThinEventGateway,
        transactionManager: ObjectProvider<PlatformTransactionManager>,
    ): ThinCommandGateway = ThinCommandGateway(registry, eventGateway, ThinTransactions(transactionManager.ifAvailable))
}
