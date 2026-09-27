package app.dc8.axonthin

import app.dc8.axonthin.api.ReplayInto
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
 * All projections to replay are filled in one pass over the store, in `(aggregate_identifier, sequence_number)` order
 * (per-aggregate order guaranteed, same order on every run), in pages of [pageSize] events — one transaction per
 * page, delivered like live chunks (batch handlers receive the page's events as one list). Handler errors fail the
 * migration. Events published by handlers during the replay are dropped. Resetting a projection is up to the
 * application (e.g. a Liquibase change emptying or renaming its table).
 */
class ProjectionMigrator internal constructor(
    private val registry: ThinHandlerRegistry,
    private val eventGateway: ThinEventGateway,
    private val store: ThinEventStore,
    private val entityManagerFactory: EntityManagerFactory,
    private val transactions: ThinTransactions,
    private val pageSize: Int,
) {
    private val log = LoggerFactory.getLogger(ProjectionMigrator::class.java)

    enum class Outcome { REPLAYED, NOT_EMPTY, PARTIALLY_EMPTY, NO_EVENTS }

    data class Result(val projection: String, val outcome: Outcome, val events: Long = 0, val millis: Long = 0)

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
        log.info("Replaying {} projection(s) {} from {} event type(s)", beans.size, beans.map { it.projectionName }, payloadTypes.size)
        val started = System.currentTimeMillis()
        val counts = beans.associateWith { 0L }.toMutableMap()
        var after: Pair<String, Long>? = null
        while (true) {
            val page = store.readReplayPage(payloadTypes, after, pageSize)
            if (page.isEmpty()) break
            transactions.inTransaction {
                // a unit of work so handlers behave as in live dispatch; whatever they publish is never flushed
                ThinUnitOfWork.joinOrStart { uow, _ ->
                    eventGateway.dispatch(beans, page, uow, ThinAxonProperties.EventHandlerErrorMode.PROPAGATE)
                }
            }
            beans.forEach { bean -> counts[bean] = counts.getValue(bean) + page.count { bean.handles(it.payloadType) } }
            after = page.last().let { it.aggregateIdentifier to it.sequenceNumber }
        }
        val millis = System.currentTimeMillis() - started
        return beans.map { Result(it.projectionName, Outcome.REPLAYED, counts.getValue(it), millis) }
    }

    private fun isEmpty(entity: Class<*>): Boolean = entityManagerFactory.createEntityManager().use { em ->
        val name = runCatching { em.metamodel.entity(entity).name }
            .getOrElse { throw IllegalStateException("@ReplayInto(${entity.name}): not a JPA entity", it) }
        em.createQuery("select 1 from $name e").setMaxResults(1).resultList.isEmpty()
    }

    private fun resolve(typeName: String): Class<*>? =
        runCatching { ClassUtils.forName(typeName, javaClass.classLoader) }.getOrNull()
}
