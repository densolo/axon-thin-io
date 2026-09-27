package com.dc8.axonthin.v4

import com.dc8.axonthin.api.BulkCommandGateway
import org.axonframework.commandhandling.gateway.CommandGateway
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate

/**
 * [BulkCommandGateway] on top of the real Axon 4 engine: wraps sequential `sendAndWait` calls in one Spring
 * transaction. Axon's `SpringTransactionManager` uses PROPAGATION_REQUIRED, so every command's unit of work
 * (and its subscribing event processors) joins the outer transaction.
 *
 * Requires a local, synchronous command bus (SimpleCommandBus) — a distributed/async bus would escape the transaction.
 */
class Axon4BulkCommandGateway(
    private val commandGateway: CommandGateway,
    transactionManager: PlatformTransactionManager,
) : BulkCommandGateway {

    private val transaction = TransactionTemplate(transactionManager)

    override fun sendAllAndWait(commands: List<Any>): List<Any?> =
        transaction.execute { commands.map { commandGateway.sendAndWait<Any?>(it) } }!!
}
