# axon-thin

A thin, drop-in replacement for the subset of **Axon Framework 4.13** we actually use, written against Axon's own
annotations and types:
- `@CommandHandler` / `@EventHandler` on Spring beans;
- event-sourced `@Aggregate`s (`AggregateLifecycle.apply`, `@EventSourcingHandler`, `@CreationPolicy`, `markDeleted`);
- `CommandGateway` and `EventGateway`;
- an event store that reads and writes **Axon's own tables** (`domain_event_entry`, with an optional `axon_` prefix)
  through Axon's `JacksonSerializer`.

On top of that it adds `BulkCommandGateway.sendAllAndWait`, which sends many commands in one transaction.

Runtime model: commands are handled synchronously on the caller's thread. Events are stored and then passed to event
handlers **in the same transaction**, which matches Axon's subscribing processors on an `EmbeddedEventStore`.
There is no Axon Server, no tracking processors, and no `QueryGateway`: queries go directly to services and
repositories.

## Modules

```
axon-thin-api            BulkCommandGateway (no dependencies): the only API that is not an Axon type
axon-thin                the thin runtime + Spring Boot auto-configuration (depends on axon-messaging for types only)
axon-thin-v4-adapter     BulkCommandGateway on real Axon 4 (a TransactionTemplate around sendAndWait)

examples/task-model           Task aggregate (event-sourced, comments inside), an external command handler,
                              JPA read models + projections, query service (Axon types are `provided`)
examples/task-contract-tests  abstract JUnit suite (src/main) + raw event-row reader + a lifecycle probe aggregate
examples/task-app-axon4       task-model + Axon 4 starter, JPA event store, tables renamed via META-INF/axon-orm.xml
examples/task-app-thin        task-model + axon-thin, same tables via axon.thin.event-store.table-prefix
                              + AxonStorageInteropTest: Axon's JpaEventStorageEngine and thin on one database

scripts/axon-usage-scan  inventory of Axon usage in a real project (see its README)
```

### Changes from the proposed 4-module layout

* **Contract tests in their own module.** To run the same tests against Axon 4 and thin, they can't live in
  either app. They are abstract classes in `src/main` of `task-contract-tests`. Each app module adds a one-line
  `@SpringBootTest` subclass. This avoids `test-jar` classifiers and scope problems.
* **`axon-thin-api` split out.** The model's `TaskService` uses `sendAllAndWait`, and both engines have to
  provide it. If the interface lived in `axon-thin`, the Axon 4 app would pull the thin runtime onto its classpath.
* **`axon-thin-v4-adapter`.** This implements `sendAllAndWait` on real Axon 4. It is needed for the compatibility
  suite. It also lets the real project start using the bulk API before it switches engines.
* **Model depends on Axon types only.** `axon-messaging` is `provided` in `task-model`, so each app module
  decides which engine is on the classpath.

## Semantics (verified by the contract suite on both engines)

| | Axon 4 (as configured in `task-app-axon4`) | axon-thin |
|---|---|---|
| command bus | `SimpleCommandBus`, caller thread | same |
| events | `EmbeddedEventStore` + `JpaEventStorageEngine`, subscribing processors | JDBC store on the same tables, synchronous dispatch |
| transaction | one per command (PROPAGATION_REQUIRED, joins the caller's transaction) | same |
| order | store events, then run event handlers, before commit | same |
| aggregates | `AggregateAnnotationCommandHandler` + `EventSourcingRepository` | same routing, creation policies, results and exceptions |
| `AggregateLifecycle` | apply / andThenApply / isLive / getVersion / markDeleted | same ordering (see `LifecycleProbe`) |
| handler exception | original runtime exception rethrown; checked exceptions → `CommandExecutionException` | same |
| duplicate aggregate / stale write | `AggregateStreamCreationException` / `ConcurrencyException` | same (unique index on aggregate id + sequence) |
| event-handler exception | `PropagatingErrorHandler` configured | `axon.thin.event-handler-error-mode=propagate` (default `log`, like Axon) |
| correlation metadata | `MessageOriginProvider` (`correlationId`, `traceId`) | same |
| handler parameters | payload, `Message` types, `MetaData`, `@MetaDataValue`, `@MessageIdentifier`, `@Timestamp`, `@SequenceNumber`, `@SourceId`, `@AggregateType`, Spring beans | same |
| `sendAllAndWait` | adapter: one transaction around sequential `sendAndWait` | one transaction; set-based chunk pipeline (see below) |

### Chunks: set-based `sendAllAndWait` (thin only)

Every top-level dispatch is a *chunk*: `sendAndWait` is a chunk of one, `sendAllAndWait(commands)` a chunk of N.
A chunk runs in one transaction:

| Stage | Round trips |
|---|---|
| preload: latest snapshots of all target aggregates | 1 |
| preload: events after each snapshot (`join (values (id, after_seq), …)`) | 1 |
| handle commands in order against the in-memory aggregates | 0 |
| allocate `global_index` values for all events | 1 (one `nextval` per 50 events, in one query) |
| append all events (one JDBC batch) | 1 |
| event handlers: batch handlers get one ordered list | depends on the projection |

`ThinChunkPipelineTest` asserts these counts.

What that means for your code:
- **Aggregates:** a later command in the chunk sees the aggregate state produced by earlier ones, because they
  share the in-memory instances.
- **Projections** receive the chunk's events only after its last command. **Validation inside a chunk** must
  therefore consider the chunk's own changes, not only the projections.
- **Batch handlers:** `@EventHandler fun on(events: List<EventMessage<ContainerEvent>>)` (or `List<ContainerEvent>`)
  receives every event it handles, in publication order, once per chunk. Existing single-event handlers are called
  once per event. Each handler bean receives the whole chunk before the next bean does.
- **All-or-nothing:** the first failure rolls the chunk back. Jobs size their chunks (for example 100–500).
- **Concurrent writers:** if another writer appended to one of the chunk's aggregates first, the unique index
  raises `ConcurrencyException`. `BulkOptions(concurrencyRetries = n)` (or `axon.thin.concurrency-retries`) re-runs
  the chunk with freshly loaded aggregates, so your deltas land on the latest state. There is no retry inside a
  caller's transaction, and no retry on `AggregateStreamCreationException`.

On Axon 4 (the v4 adapter), a chunk runs command by command. Storage and single-command behaviour stay covered by the
shared contract suite; chunk semantics are thin-only.

Read-model writes from JPA projections are only batched if Hibernate is told to:
`spring.jpa.properties.hibernate.jdbc.batch_size`, `order_inserts` and `order_updates`, plus `reWriteBatchedInserts=true`
on the PostgreSQL URL.

### Storage compatibility

Rows are written exactly like Axon's `JpaEventStorageEngine` writes them:
- **Payload and metadata:** JSON bytes from Axon's `JacksonSerializer`. Thin looks up the `ObjectMapper` the same
  way Axon's autoconfig does: `defaultAxonObjectMapper`, else the application's `ObjectMapper`.
- **Type and revision:** `payload_type` is the class name, and `payload_revision` comes from `@Revision`.
- **Timestamp:** `time_stamp` is ISO-8601 with milliseconds.
- **Aggregate columns:** `type` is the aggregate type, and `sequence_number` the aggregate sequence.
- **Non-aggregate events** (published through `EventGateway`) are stored with `aggregate_identifier = event id`,
  `sequence 0` and `type null`.

`global_index` comes from the sequence Hibernate creates for Axon's entity (`<table>_seq`, `INCREMENT BY 50`).
Thin allocates from it with the same *pooled* semantics, so Axon/Hibernate and thin can write to the same table at
the same time (tested). The contract suite checks the raw rows on both engines. `AxonStorageInteropTest` also
checks, on one database, that Axon reads thin's streams and that thin continues Axon's streams.

```yaml
axon.thin:
  event-handler-error-mode: propagate      # or log (Axon's default)
  event-store:
    table-prefix: axon_                    # -> axon_domain_event_entry (instead of an orm.xml override)
    global-index:
      strategy: sequence                   # or identity, if your orm.xml maps global_index as IDENTITY
      sequence-name: axon_domain_event_entry_seq   # default: <table>_seq
      allocation-size: 50                  # must equal the sequence's INCREMENT BY
```

If the tables don't exist yet, reference DDL is in `axon-thin/src/main/resources/axon-thin/schema/` (H2 and PostgreSQL).

### Snapshots

Thin uses your existing Axon snapshot configuration unchanged:

```kotlin
@Aggregate(snapshotTriggerDefinition = "taskSnapshotTrigger")
class Task { ... }

@Bean
fun taskSnapshotTrigger(snapshotter: Snapshotter) = EventCountSnapshotTriggerDefinition(snapshotter, 100)
```

The trigger is Axon's own class, so it counts exactly as Axon does: every event handled while loading, the snapshot
message included, plus every event applied. Thin supplies the `Snapshotter` bean (`ThinSnapshotter`) in place of
Axon's `SpringAggregateSnapshotter`. It works like Axon's `AbstractSnapshotter` with the default direct executor:
- it runs after the command's transaction commits, in its own transaction;
- it rebuilds the aggregate from the store, and only stores the snapshot if it replaces more than one event;
- it replaces older snapshots of the same aggregate;
- the snapshot carries the triggering command's `correlationId`/`traceId`.

On load, thin reads the latest snapshot plus the events after it, as Axon's `AbstractEventStore` does. Snapshots
serialize the aggregate itself with the event serializer (Jackson), so aggregate state must be visible to Jackson.
The example uses `@JsonAutoDetect(fieldVisibility = ANY)`.

Verified on both engines: the same snapshot points (threshold 5 gives snapshots at seq 4, then seq 8), the same rows,
and restore after the history is purged. `AxonStorageInteropTest` checks that Axon reads thin's snapshots and that
thin restores from Axon's.

**One deliberate difference.** If a snapshot can't be read (its class is gone, or the payload is malformed), thin
skips it and replays the full stream. Axon 4.13 fails the command instead (`IncompatibleAggregateException` /
`SerializationException`), because it deserializes the payload lazily, after its own fallback has already run.
`ThinSnapshotFallbackTest` covers thin's behaviour.

### Processing groups

Thin accepts `@ProcessingGroup` and ignores it: every event handler bean receives every event, as one subscribing
group. The group name never reaches the event rows. Axon only uses it for tracking tokens (`token_entry`) and
sagas, and thin uses neither.

### Why there is still a (small) unit of work

The transaction gives atomicity. Thin still keeps a small internal scope per command. It is not Axon's
`UnitOfWork` API. It provides:
1. **Deferred events.** Events are stored and dispatched after the handler returns, which is Axon's order.
2. **An aggregate identity map.** An aggregate is loaded once and shared by nested commands, so sequence numbers
   stay consistent. Inside `sendAllAndWait` the map spans the whole batch.
3. **The thread-bound `AggregateLifecycle` scope** that `apply()` needs.
4. **Correlation metadata.**
5. **Discarding events of a failed nested command.**

### Not supported (yet)

- **Aggregates:** `@AggregateMember` entities, `AggregateLifecycle.createNew`, `@TargetAggregateVersion`, and
  state-stored (JPA) aggregates.
- **Snapshots:** `@Aggregate(snapshotFilter)` is ignored.
- **Upcasters.**
- **Other Axon features:** sagas, deadlines, queries, handler interceptors, `UnitOfWork` parameters, custom
  correlation providers, and tracking processors.
- **PostgreSQL `oid` payload columns.** Hibernate's default for Axon's `@Lob byte[]` is `oid`; thin reads and
  writes `bytea` (the usual orm.xml override).

Run the scanner on the real project to decide which of these are needed. It also lists your `axon-orm.xml`
overrides.

## Build & test

```bash
mvn install                               # everything: engine unit tests, the 33-test contract suite on both engines, interop and thin-only tests
mvn test -pl examples/task-app-axon4      # contract suite on Axon 4
mvn test -pl examples/task-app-thin       # contract suite on axon-thin
```

JDK 21+ (bytecode target 21), Kotlin 2.4, Spring Boot 3.5, H2. Postgres is planned. The event store uses plain
JDBC (`nextval(...)` on PostgreSQL, `next value for` elsewhere). Postgres support is mainly a Testcontainers
profile, plus the `bytea`/`oid` question above.
