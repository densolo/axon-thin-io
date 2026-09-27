package app.dc8.axonthin.aggregate

import app.dc8.axonthin.ThinHandlerRegistry
import app.dc8.axonthin.ThinUnitOfWork
import app.dc8.axonthin.correlationData
import app.dc8.axonthin.eventstore.ThinEventStore
import org.axonframework.eventhandling.GenericDomainEventMessage
import org.axonframework.eventsourcing.NoSnapshotTriggerDefinition
import org.axonframework.eventsourcing.Snapshotter
import org.axonframework.modelling.command.ConcurrencyException
import org.slf4j.LoggerFactory
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import java.util.concurrent.ConcurrentHashMap

/**
 * Axon [Snapshotter] replacement (for SpringAggregateSnapshotter / AggregateSnapshotter), so the application's
 * own `SnapshotTriggerDefinition` beans — e.g. `EventCountSnapshotTriggerDefinition(snapshotter, 100)` — work
 * unchanged.
 *
 * Like AbstractSnapshotter with the default direct executor:
 * - scheduled while a transaction runs → executed after it commits (once per aggregate per transaction);
 * - runs synchronously, in its own transaction, rebuilding the aggregate from the store (latest snapshot + events);
 * - stored only if it replaces more than one event; deleted aggregates are not snapshotted;
 * - failures are logged, never propagated (the command already committed);
 * - the snapshot carries the correlation data of the command that triggered it (Axon builds it in that
 *   command's unit of work).
 */
class ThinSnapshotter internal constructor(
    private val registry: ThinHandlerRegistry,
    private val store: ThinEventStore,
    transactionManager: PlatformTransactionManager?,
) : Snapshotter {

    private val log = LoggerFactory.getLogger(ThinSnapshotter::class.java)
    private val newTransaction = transactionManager?.let {
        TransactionTemplate(it).apply { propagationBehavior = TransactionDefinition.PROPAGATION_REQUIRES_NEW }
    }
    private val inProgress = ConcurrentHashMap.newKeySet<Pair<Class<*>, String>>()

    override fun scheduleSnapshot(aggregateType: Class<*>, aggregateIdentifier: String) {
        val key = aggregateType to aggregateIdentifier
        val metaData = ThinUnitOfWork.currentOrNull()?.currentMessage?.let(::correlationData) ?: emptyMap()
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            snapshot(key, metaData)
            return
        }
        @Suppress("UNCHECKED_CAST")
        val scheduled = TransactionSynchronizationManager.getResource(this) as MutableMap<Pair<Class<*>, String>, Map<String, Any>>?
            ?: LinkedHashMap<Pair<Class<*>, String>, Map<String, Any>>().also { map ->
                TransactionSynchronizationManager.bindResource(this, map)
                TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
                    override fun afterCommit() = map.forEach(::snapshot)
                    override fun afterCompletion(status: Int) {
                        TransactionSynchronizationManager.unbindResourceIfPossible(this@ThinSnapshotter)
                    }
                })
            }
        scheduled[key] = metaData // latest trigger wins within one transaction
    }

    private fun snapshot(key: Pair<Class<*>, String>, metaData: Map<String, Any>) {
        if (!inProgress.add(key)) return
        try {
            if (newTransaction != null) newTransaction.executeWithoutResult { createAndStore(key, metaData) }
            else createAndStore(key, metaData)
        } catch (e: ConcurrencyException) {
            log.info("An up-to-date snapshot entry already exists, ignoring this attempt.")
        } catch (e: Exception) {
            log.warn("An attempt to create and store a snapshot resulted in an exception. Exception summary: {}", e.message, e)
        } finally {
            inProgress.remove(key)
        }
    }

    private fun createAndStore(key: Pair<Class<*>, String>, metaData: Map<String, Any>) {
        val (type, id) = key
        val model = registry.aggregateModel(type) ?: error("No aggregate registered for ${type.name}")
        val stream = store.readAggregateStream(model, id)
        if (stream.isEmpty()) return
        val aggregate = model.rebuild(stream, NoSnapshotTriggerDefinition.TRIGGER)
        if (aggregate.deleted) return
        val version = aggregate.lastSequence ?: return
        // a snapshot should only be stored if it replaces more than one event
        if (version <= stream.first().sequenceNumber) return
        store.storeSnapshot(GenericDomainEventMessage(model.typeName, id, version, aggregate.root, metaData))
    }
}
