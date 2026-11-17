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
 *   everything one transaction scheduled is built and stored together (two reads, two JDBC batches);
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
            snapshot(mapOf(key to metaData))
            return
        }
        @Suppress("UNCHECKED_CAST")
        val scheduled = TransactionSynchronizationManager.getResource(this) as MutableMap<Pair<Class<*>, String>, Map<String, Any>>?
            ?: LinkedHashMap<Pair<Class<*>, String>, Map<String, Any>>().also { map ->
                TransactionSynchronizationManager.bindResource(this, map)
                TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
                    override fun afterCommit() = snapshot(map)
                    override fun afterCompletion(status: Int) {
                        TransactionSynchronizationManager.unbindResourceIfPossible(this@ThinSnapshotter)
                    }
                })
            }
        scheduled[key] = metaData // latest trigger wins within one transaction
    }

    /**
     * Snapshots of everything one transaction scheduled, together: two reads, one batch of deletes, one of inserts.
     * If that fails, each aggregate is retried on its own, so one failing snapshot does not cost the others.
     */
    private fun snapshot(scheduled: Map<Pair<Class<*>, String>, Map<String, Any>>) {
        val keys = scheduled.keys.filter(inProgress::add) // skip aggregates another thread is snapshotting
        if (keys.isEmpty()) return
        try {
            val stored = keys.size > 1 && try {
                inNewTransaction { createAndStore(keys, scheduled) }
                true
            } catch (e: Exception) {
                log.info("Storing {} snapshots together failed ({}); storing them one by one.", keys.size, e.message)
                false
            }
            if (!stored) keys.forEach { key -> snapshotOne(key, scheduled.getValue(key)) }
        } finally {
            inProgress.removeAll(keys.toSet())
        }
    }

    private fun snapshotOne(key: Pair<Class<*>, String>, metaData: Map<String, Any>) {
        try {
            inNewTransaction { createAndStore(listOf(key), mapOf(key to metaData)) }
        } catch (e: ConcurrencyException) {
            log.info("An up-to-date snapshot entry already exists, ignoring this attempt.")
        } catch (e: Exception) {
            log.warn("An attempt to create and store a snapshot resulted in an exception. Exception summary: {}", e.message, e)
        }
    }

    private fun inNewTransaction(block: () -> Unit) {
        if (newTransaction != null) newTransaction.executeWithoutResult { block() } else block()
    }

    /** Aggregate ids are unique across types (the event table's key is id + sequence), so streams are keyed by id. */
    private fun createAndStore(keys: List<Pair<Class<*>, String>>, metaData: Map<Pair<Class<*>, String>, Map<String, Any>>) {
        val models = keys.associate { (type, id) -> id to (registry.aggregateModel(type) ?: error("No aggregate registered for ${type.name}")) }
        val streams = store.readAggregateStreams(models)
        val snapshots = keys.mapNotNull { key ->
            val id = key.second
            val model = models.getValue(id)
            val stream = streams.getValue(id)
            if (stream.isEmpty()) return@mapNotNull null
            val aggregate = model.rebuild(stream, NoSnapshotTriggerDefinition.TRIGGER)
            if (aggregate.deleted) return@mapNotNull null
            val version = aggregate.lastSequence ?: return@mapNotNull null
            // a snapshot should only be stored if it replaces more than one event
            if (version <= stream.first().sequenceNumber) return@mapNotNull null
            GenericDomainEventMessage(model.typeName, id, version, aggregate.root, metaData.getValue(key))
        }
        store.storeSnapshots(snapshots)
    }
}
