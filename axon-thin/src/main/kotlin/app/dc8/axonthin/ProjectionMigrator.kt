package app.dc8.axonthin

import app.dc8.axonthin.api.ReplayInto
import app.dc8.axonthin.eventstore.ReplayOrder
import app.dc8.axonthin.eventstore.ThinEventStore
import jakarta.persistence.EntityManagerFactory
import org.slf4j.LoggerFactory
import org.springframework.util.ClassUtils

/**
 * Fills new, empty projections from the event store. Call [migrate] from the migration step that runs before the
 * applications start (they wait for it); nothing else writes while it runs.
 *
 * A projection is an event-handling bean annotated [ReplayInto]. For each of them:
 * - any of its entities not empty → left alone (it is live; events reach it in the command's transaction);
 * - all empty, but the store holds no event it handles → nothing to do;
 * - all empty and relevant events exist → replayed.
 *
 * All projections to replay are filled in one pass over the store, in pages of [pageSize] events — one transaction per
 * page, delivered like live chunks (runs of events for one batch handler arrive as one list). Handler errors fail the
 * migration. Events published by handlers during the replay are dropped. Resetting a projection is up to the
 * application (e.g. a Liquibase change emptying or renaming its table).
 *
 * Order ([order]):
 * - [ReplayOrder.GLOBAL] (default): `global_index`, the order events were appended — like live handling and Axon's
 *   tracking replays; keeps patterns spanning aggregates (single events + a bulk event) in order. With pooled
 *   sequence blocks (increment 50) and several writing processes, a later event of an aggregate can carry a lower
 *   index: such inversions are detected, logged and counted in [Result.inversions]. A sequence increment of 1 avoids
 *   them.
 * - [ReplayOrder.PER_AGGREGATE]: strict per-aggregate order, aggregates one after another.
 */
class ProjectionMigrator internal constructor(
    private val registry: ThinHandlerRegistry,
    private val eventGateway: ThinEventGateway,
    private val store: ThinEventStore,
    private val entityManagerFactory: EntityManagerFactory,
    private val transactions: ThinTransactions,
    private val pageSize: Int,
    private val order: ReplayOrder = ReplayOrder.GLOBAL,
) {
    private val log = LoggerFactory.getLogger(ProjectionMigrator::class.java)

    enum class Outcome { REPLAYED, NOT_EMPTY, PARTIALLY_EMPTY, NO_EVENTS }

    /**
     * @property inversions events that arrived after a later event of the same aggregate (GLOBAL order only): the
     *   projection may hold an older state for those aggregates — the log names them.
     */
    data class Result(
        val projection: String,
        val outcome: Outcome,
        val events: Long = 0,
        val millis: Long = 0,
        val inversions: Long = 0,
    )

    fun migrate(): List<Result> {
        check(ThinUnitOfWork.currentOrNull() == null) { "migrate() must not run inside command or event handling" }
        val projections = registry.eventHandlingBeans.mapNotNull { bean ->
            ClassUtils.getUserClass(bean.bean).getAnnotation(ReplayInto::class.java)
                ?.let { annotation -> bean to annotation.value.map { it.java } }
        }
        if (projections.isEmpty()) return emptyList()

        val results = LinkedHashMap<String, Result>()
        val empty = projections.filter { (bean, entities) ->
            val emptiness = entities.map(::isEmpty)
            when {
                emptiness.all { it } -> true
                emptiness.any { it } -> {
                    log.warn("Projection [{}] is partially empty ({}); not replayed", bean.projectionName, entities.map { it.simpleName })
                    results[bean.projectionName] = Result(bean.projectionName, Outcome.PARTIALLY_EMPTY)
                    false
                }
                else -> {
                    results[bean.projectionName] = Result(bean.projectionName, Outcome.NOT_EMPTY)
                    false
                }
            }
        }.map { it.first }

        if (empty.isNotEmpty()) {
            // stored types this code can load; unknown types (e.g. from another branch) are skipped
            val storedTypes = store.distinctPayloadTypes().mapNotNull { name -> resolve(name)?.let { name to it } }
            val relevant = empty.associateWith { bean -> storedTypes.filter { (_, type) -> bean.handles(type) }.map { it.first } }
            relevant.filterValues { it.isEmpty() }.keys.forEach {
                results[it.projectionName] = Result(it.projectionName, Outcome.NO_EVENTS)
            }
            val toReplay = relevant.filterValues { it.isNotEmpty() }
            if (toReplay.isNotEmpty()) replay(toReplay.keys.toList(), toReplay.values.flatten().toSet()).forEach { results[it.projection] = it }
        }
        results.values.forEach { log.info("Projection [{}]: {} ({} events, {} ms)", it.projection, it.outcome, it.events, it.millis) }
        return projections.map { results.getValue(it.first.projectionName) }
    }

    private fun replay(beans: List<ThinHandlerRegistry.EventHandlingBean>, payloadTypes: Set<String>): List<Result> {
        log.info(
            "Replaying {} projection(s) {} from {} event type(s) in {} order",
            beans.size, beans.map { it.projectionName }, payloadTypes.size, order,
        )
        val started = System.currentTimeMillis()
        val counts = beans.associateWith { 0L }.toMutableMap()
        val inversions = beans.associateWith { 0L }.toMutableMap()
        val lastSequence = HashMap<String, Long>() // per aggregate, to detect out-of-order events (GLOBAL order)
        var after: ThinEventStore.ReplayEvent? = null
        while (true) {
            val page = store.readReplayPage(payloadTypes, order, after, pageSize)
            if (page.isEmpty()) break
            // detect inversions first: logged even if a handler then fails because of one
            for (event in page) {
                val message = event.message
                val handledBy = beans.filter { it.handles(message.payloadType) }
                handledBy.forEach { counts[it] = counts.getValue(it) + 1 }
                if (message.type == null) continue // non-aggregate events have no sequence to violate
                val previous = lastSequence.put(message.aggregateIdentifier, maxOf(message.sequenceNumber, lastSequence[message.aggregateIdentifier] ?: -1))
                if (previous != null && previous > message.sequenceNumber) {
                    handledBy.forEach { inversions[it] = inversions.getValue(it) + 1 }
                    log.warn(
                        "Replay out of order: aggregate [{}] event {} (global index {}) arrives after event {}; " +
                            "projections {} may end with an older state for it",
                        message.aggregateIdentifier, message.sequenceNumber, event.globalIndex, previous,
                        handledBy.map { it.projectionName },
                    )
                }
            }
            val messages = page.map { it.message }
            transactions.inTransaction {
                // a unit of work so handlers behave as in live dispatch; whatever they publish is never flushed
                ThinUnitOfWork.joinOrStart { uow, _ ->
                    eventGateway.dispatch(beans, messages, uow, ThinAxonProperties.EventHandlerErrorMode.PROPAGATE)
                }
            }
            after = page.last()
        }
        val millis = System.currentTimeMillis() - started
        return beans.map { Result(it.projectionName, Outcome.REPLAYED, counts.getValue(it), millis, inversions.getValue(it)) }
    }

    private fun isEmpty(entity: Class<*>): Boolean = entityManagerFactory.createEntityManager().use { em ->
        val name = runCatching { em.metamodel.entity(entity).name }
            .getOrElse { throw IllegalStateException("@ReplayInto(${entity.name}): not a JPA entity", it) }
        em.createQuery("select 1 from $name e").setMaxResults(1).resultList.isEmpty()
    }

    private fun resolve(typeName: String): Class<*>? =
        runCatching { ClassUtils.forName(typeName, javaClass.classLoader) }.getOrNull()
}
