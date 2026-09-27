package com.dc8.axonthin.v4

import com.dc8.axonthin.api.BulkCommandGateway
import org.axonframework.commandhandling.gateway.CommandGateway
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.transaction.PlatformTransactionManager

@AutoConfiguration(
    afterName = [
        "org.axonframework.springboot.autoconfig.AxonAutoConfiguration",
        "org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration",
        "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration",
    ],
)
class Axon4BulkAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(BulkCommandGateway::class)
    @ConditionalOnBean(CommandGateway::class, PlatformTransactionManager::class)
    fun axon4BulkCommandGateway(
        commandGateway: CommandGateway,
        transactionManager: PlatformTransactionManager,
    ): BulkCommandGateway = Axon4BulkCommandGateway(commandGateway, transactionManager)
}
