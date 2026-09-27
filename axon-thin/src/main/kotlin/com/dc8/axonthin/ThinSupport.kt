package com.dc8.axonthin

import org.axonframework.messaging.Message
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate

/**
 * Spring transaction boundary with PROPAGATION_REQUIRED, like Axon's SpringTransactionManager:
 * joins the caller's transaction when there is one. Without a transaction manager it just runs the block.
 */
internal class ThinTransactions(transactionManager: PlatformTransactionManager?) {

    private val template = transactionManager?.let(::TransactionTemplate)

    fun <T> inTransaction(block: () -> T): T =
        if (template == null) block()
        else {
            @Suppress("UNCHECKED_CAST")
            template.execute { block() } as T
        }
}

/**
 * Correlation data copied onto messages dispatched while [source] is handled — Axon 4's default
 * `MessageOriginProvider`: `correlationId` = id of the causing message, `traceId` = its trace id (or its id).
 */
internal fun correlationData(source: Message<*>): Map<String, Any> = mapOf(
    "correlationId" to source.identifier,
    "traceId" to (source.metaData["traceId"] ?: source.identifier),
)
