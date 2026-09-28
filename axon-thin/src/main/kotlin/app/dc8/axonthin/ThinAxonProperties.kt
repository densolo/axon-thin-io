package app.dc8.axonthin

import app.dc8.axonthin.eventstore.ReplayOrder
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("axon.thin")
class ThinAxonProperties {

    /**
     * What happens when an `@EventHandler` throws. Axon 4 defaults to logging (LoggingErrorHandler);
     * PROPAGATE matches `PropagatingErrorHandler` and rolls the command's transaction back.
     */
    var eventHandlerErrorMode: EventHandlerErrorMode = EventHandlerErrorMode.LOG

    /**
     * Default re-runs of a chunk (`sendAndWait` / `sendAllAndWait` without options) on ConcurrencyException;
     * per call via `BulkOptions(concurrencyRetries = …)`.
     */
    var concurrencyRetries: Int = 0

    val eventStore = EventStore()

    /** ProjectionMigrator: events per page (one transaction per page). */
    var replayPageSize: Int = 1000

    /** ProjectionMigrator: `global` (append order, default) or `per-aggregate`. */
    var replayOrder: ReplayOrder = ReplayOrder.GLOBAL

    enum class EventHandlerErrorMode { LOG, PROPAGATE }

    /** Axon-compatible JDBC event store (same tables/columns as Axon's JpaEventStorageEngine). */
    class EventStore {
        /** Persist events. Required for event-sourced aggregates; needs a DataSource. */
        var enabled: Boolean = true

        /** Prefix for all Axon tables, e.g. `axon_` → `axon_domain_event_entry`. */
        var tablePrefix: String = ""

        var domainEventTable: String = "domain_event_entry"

        var snapshotEventTable: String = "snapshot_event_entry"

        /** Also store events published via EventGateway (not applied by an aggregate), as Axon's event store does. */
        var storeNonAggregateEvents: Boolean = true

        val globalIndex = GlobalIndex()

        val domainEventTableName: String get() = tablePrefix + domainEventTable

        val snapshotEventTableName: String get() = tablePrefix + snapshotEventTable
    }

    class GlobalIndex {
        /**
         * SEQUENCE: pooled allocation from a database sequence, compatible with Hibernate's pooled optimizer
         * (Hibernate 6 default for Axon's `@GeneratedValue globalIndex`). IDENTITY: the column generates it.
         */
        var strategy: Strategy = Strategy.SEQUENCE

        /** Defaults to `<domain event table>_seq` — what Hibernate 6 generates for Axon's DomainEventEntry. */
        var sequenceName: String? = null

        /** Must equal the sequence's INCREMENT BY (Hibernate default allocationSize = 50). */
        var allocationSize: Int = 50

        enum class Strategy { SEQUENCE, IDENTITY }
    }
}
