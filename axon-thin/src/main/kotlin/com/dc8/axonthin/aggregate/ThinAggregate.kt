package com.dc8.axonthin.aggregate

import com.dc8.axonthin.ThinUnitOfWork
import com.dc8.axonthin.correlationData
import org.axonframework.eventhandling.DomainEventMessage
import org.axonframework.eventhandling.GenericDomainEventMessage
import org.axonframework.eventsourcing.SnapshotTrigger
import org.axonframework.messaging.MetaData
import org.axonframework.modelling.command.Aggregate
import org.axonframework.modelling.command.AggregateLifecycle
import org.axonframework.modelling.command.ApplyMore
import java.util.concurrent.Callable
import java.util.function.Supplier

/**
 * One event-sourced aggregate instance plus its [AggregateLifecycle] scope, so model code calling
 * `AggregateLifecycle.apply(...)` works unchanged.
 *
 * Apply semantics follow Axon's AnnotatedAggregate / EventSourcedAggregate:
 * - apply → the `@EventSourcingHandler` runs immediately, then the event is queued for storage/dispatch;
 * - apply from inside an `@EventSourcingHandler`, or from a constructor (no root yet) → delayed until the current
 *   handler / the constructor finished;
 * - while sourcing from the store (`isLive() == false`) apply is ignored — those events are already recorded.
 *
 * Holds no reference to a unit of work: inside `sendAllAndWait` the same instance serves several commands.
 */
internal class ThinAggregate(
    val model: AggregateModel,
    root: Any?,
    /** Axon's SnapshotTrigger: sees every domain event this instance handles (sourced, snapshot included, or applied). */
    private val snapshotTrigger: SnapshotTrigger = model.newSnapshotTrigger(),
) : AggregateLifecycle() {

    var root: Any? = root
        private set
    var lastSequence: Long? = null
        private set
    var deleted = false
        private set

    private var live = true
    private var applying = false
    private val delayed = ArrayDeque<() -> Unit>()

    val identifierValue: Any? get() = root?.let(model::identifierOf)
    val identifierAsString: String? get() = identifierValue?.toString()

    /** Runs [block] with this aggregate as the current `AggregateLifecycle` scope. */
    fun <T> inScope(block: () -> T): T = executeWithResult(Callable { block() })

    /** Rebuilds state from stored events. */
    fun initializeState(events: List<DomainEventMessage<*>>) {
        live = false
        try {
            inScope {
                events.forEach { event ->
                    lastSequence = event.sequenceNumber
                    snapshotTrigger.eventHandled(event)
                    source(event)
                }
            }
        } finally {
            live = true
        }
        snapshotTrigger.initializationFinished()
    }

    /** After a creation handler constructed the root: attach it and apply what the constructor applied. */
    fun attachRoot(instance: Any) {
        root = instance
        inScope { drainDelayed() }
    }

    // ---- AggregateLifecycle ---------------------------------------------------------------------------------------

    override fun <T> doApply(payload: T, metaData: MetaData): ApplyMore {
        if (!live) return IgnoredApplyMore
        if (applying || root == null) {
            delayed.addLast { publishApplying(payload, metaData) }
        } else {
            publishApplying(payload, metaData)
            drainDelayed()
        }
        return LiveApplyMore()
    }

    override fun getIsLive(): Boolean = live

    override fun version(): Long? = lastSequence

    override fun doMarkDeleted() {
        deleted = true
    }

    override fun <T : Any?> doCreateNew(aggregateType: Class<T>, factoryMethod: Callable<T>): Aggregate<T> =
        throw UnsupportedOperationException("AggregateLifecycle.createNew is not supported by axon-thin yet")

    override fun type(): String = model.typeName

    override fun identifier(): Any? = root?.let(model::identifierOf)

    // ---- internals ------------------------------------------------------------------------------------------------

    /**
     * Runs delayed tasks in order. Only publishing marks the aggregate as "applying"; an `andThen` task runs
     * normally, so an apply() inside it takes effect right away (Axon: andThenApply precedes applies nested in the
     * sourcing handler of the preceding event).
     */
    private fun drainDelayed() {
        while (true) {
            val task = delayed.removeFirstOrNull() ?: return
            task()
        }
    }

    private fun publishApplying(payload: Any?, metaData: MetaData) {
        applying = true
        try {
            publish(payload, metaData)
        } finally {
            applying = false
        }
    }

    private fun publish(payload: Any?, metaData: MetaData) {
        // the unit of work of the command being handled now — a batch reuses this aggregate across commands
        val uow = checkNotNull(ThinUnitOfWork.currentOrNull()) { "apply() outside of command handling" }
        val sequence = (lastSequence ?: -1L) + 1
        val correlated = uow.currentMessage?.let { metaData.mergedWith(correlationData(it)) } ?: metaData
        val before = identifierAsString
        var event: DomainEventMessage<*> = GenericDomainEventMessage(model.typeName, before, sequence, payload, correlated)
        lastSequence = sequence // getVersion() inside the sourcing handler already reflects this event (as in Axon)
        snapshotTrigger.eventHandled(event)
        source(event)
        val after = identifierAsString ?: throw IllegalStateException(
            "Aggregate identifier must be non-null after applying an event. Make sure the aggregate identifier " +
                "is initialized at the latest when handling the creation event.",
        )
        if (before != after) {
            val original = event
            event = GenericDomainEventMessage(model.typeName, after, sequence, original, Supplier { original.timestamp })
        }
        uow.aggregates.putIfAbsent(model.typeName to after, this)
        uow.queue(event)
    }

    private fun source(event: DomainEventMessage<*>) {
        model.sourcingHandler(event.payloadType)?.invokeOn(root!!, event)
    }

    private object IgnoredApplyMore : ApplyMore {
        override fun andThenApply(payloadOrMessageSupplier: Supplier<*>): ApplyMore = this
        override fun andThen(runnable: Runnable): ApplyMore = this
    }

    private inner class LiveApplyMore : ApplyMore {
        override fun andThenApply(payloadOrMessageSupplier: Supplier<*>): ApplyMore = andThen {
            AggregateLifecycle.apply(payloadOrMessageSupplier.get())
        }

        override fun andThen(runnable: Runnable): ApplyMore {
            if (applying || root == null) delayed.addLast { runnable.run() } else runnable.run()
            return this
        }
    }
}
