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
axon-thin-api            the only non-Axon API, dependency-free: BulkCommandGateway + BulkOptions, ChunkContext, @ReplayInto,
                         EventStoreBrowser
axon-thin                the thin runtime + Spring Boot auto-configuration (uses Axon jars for their types only)
axon-thin-v4-adapter     the same API on real Axon 4: sendAllAndWait (+ retry), ChunkContext, EventStoreBrowser

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

`sendAllAndWait` returns `List<R>`, typed like Axon's `<R> R sendAndWait(command)`: an unchecked cast, with `R` taken
from the expected type or given explicitly.
```kotlin
val ids: List<String> = bulk.sendAllAndWait(creates)                     // R from the expected type
val previous = bulk.sendAllAndWait<TaskStatus>(statusChanges)             // explicit
bulk.sendAllAndWait<Any?>(listOf(create, rename, assign))                 // mixed results, or results not needed
```
A wrong `R` fails where an element is used (`ClassCastException`), not in the gateway.
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
  receives the events it handles in **runs**: consecutive events for the same batch handler arrive as one call.
  Single-event handlers are called once per event, and a bean mixing both still sees everything in event order.
  Bulk operations produce long runs, so they arrive as large batches. Each handler bean receives the whole chunk
  before the next bean does.
- **All-or-nothing:** the first failure rolls the chunk back. Jobs size their chunks (for example 100–500).
- **Concurrent writers:** if another writer appended to one of the chunk's aggregates first, the unique index
  raises `ConcurrencyException`. `BulkOptions(concurrencyRetries = n)` (or `axon.thin.concurrency-retries`) re-runs
  the chunk with freshly loaded aggregates, so your deltas land on the latest state. There is no retry inside a
  caller's transaction, and no retry on `AggregateStreamCreationException`.

**Validating within a chunk: `ChunkContext`** (in `axon-thin-api`; inject it, or declare it as a handler parameter):

```kotlin
@CommandHandler
fun handle(command: ReserveTitleCommand, chunk: ChunkContext) {
    val taken = summaries.existsByTitle(command.title) ||                       // committed + earlier chunks
        chunk.pendingEvents<TaskCreatedEvent>().any { it.title == command.title } // this chunk so far
    if (taken) throw TitleTakenException(command.title)
}
```

| `ChunkContext` | axon-thin | Axon 4 (v4 adapter) |
|---|---|---|
| `pendingEvents()` | events applied or published in this chunk, not yet seen by projections, in order | always empty (projections are updated per command) |
| `commands()` / `currentCommandIndex()` | the chunk's top-level commands and the position of the current one | same |
| `aggregate<T>(id)` | the chunk's in-memory aggregate root (never queries); read-only | `null` |

The same validation code is therefore correct on both engines. The contract suite checks this
(`validation sees changes made earlier in the same chunk`).

On Axon 4 (the v4 adapter), a chunk runs command by command. Storage and single-command behaviour stay covered by the
shared contract suite; chunk semantics are thin-only.

Read-model writes from JPA projections are only batched if Hibernate is told to:
`spring.jpa.properties.hibernate.jdbc.batch_size`, `order_inserts` and `order_updates`, plus `reWriteBatchedInserts=true`
on the PostgreSQL URL.

### Storage compatibility

Rows are written exactly like Axon's `JpaEventStorageEngine` writes them:
- **Payload and metadata:** JSON bytes from the same serializer Axon 4 would use. Thin resolves it like Axon's
  Spring Boot autoconfig:
  1. a `Serializer` bean named or qualified `eventSerializer`;
  2. else `messageSerializer`;
  3. else **your general `Serializer` bean**, for example
     `@Bean @Primary @Qualifier("serializer") fun axonJacksonSerializer(objectMapper: ObjectMapper) = JacksonSerializer.builder().objectMapper(objectMapper.copy()…)`
     (the `@Primary` one if there are several);
  4. else a `JacksonSerializer` on `defaultAxonObjectMapper` or the primary `ObjectMapper`.

  The contract suite checks this with such a bean, whose extra Jackson module changes the JSON: both engines store
  identical bytes.
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

### Filling new projections: `ProjectionMigrator`

Mark a projection with the JPA entities it writes, and call `migrate()` in the migration step that runs before the
apps start:

```kotlin
@Component
@ProcessingGroup("task-summary")
@ReplayInto(TaskSummary::class)                 // entity classes; the table name comes from @Table
class TaskSummaryProjection(...) { ... }

@Component
class Migrator(private val projections: ProjectionMigrator) : ApplicationRunner {
    override fun run(args: ApplicationArguments) { projections.migrate() }   // returns one result per projection
}
```

For each `@ReplayInto` projection, `migrate()` does this:

| State | Result |
|---|---|
| any of its entities has rows | `NOT_EMPTY`: left alone; it is live |
| some entities empty, others not | `PARTIALLY_EMPTY`: left alone, with a warning |
| all empty, but no stored event it handles | `NO_EVENTS`: nothing to do |
| all empty and relevant events exist | `REPLAYED` |

- **Emptiness:** one JPQL query per entity (milliseconds).
- **Relevant event types:** found with one `select distinct payload_type` (an index on `payload_type` makes it
  instant), and matched against the projection's handlers, including supertype handlers. Types this code can't load,
  for example ones from another branch, are skipped.
- **One pass for all:** every projection to replay is filled in a single pass, in pages of
  `axon.thin.replay-page-size` events (default 1000), one transaction per page. Pages are delivered like live chunks
  (runs per batch handler).
- **Order (`axon.thin.replay-order`):**
  - `global` (the default) is `global_index` order: the order events were appended, like live handling and Axon's
    tracking replays. It keeps patterns that span aggregates in order, such as flagged single events plus a bulk
    event with its own aggregate id.
  - `per-aggregate` is `(aggregate_identifier, sequence_number)` order: strict per-aggregate order, aggregates one
    after another.
- **Inversions (`global` only):** with pooled sequence blocks and several writing processes, an aggregate's later
  event can carry a lower index. The replay detects this *before* dispatching each page, logs the aggregate id and
  both positions, and reports it in `Result.inversions`. **A sequence increment of 1 prevents it:** a writer can
  only write an aggregate's next event after its previous one is committed, so it always draws a higher index.
  Once every process runs thin, use `alter sequence … increment by 1` plus
  `axon.thin.event-store.global-index.allocation-size: 1`. It costs no extra round trips, because thin fetches all
  values a chunk needs in one query.
- **Handler failures** fail the migration. Events published by handlers during a replay are dropped.
- **Resetting is up to you.** To rebuild a projection, empty or rename its table (for example a Liquibase change that
  switches to `task_summary_v3`); the next `migrate()` fills it. Beans without `@ReplayInto` are never replayed.

### Using the event store directly

For code that talks to the event store itself:
- **`EventStore` (Axon's interface)** is provided as a bean:
  - `readEvents(id)` works like Axon's: the latest snapshot first, then the events after it, then events of that
    aggregate still pending in the running chunk. It reads lazily, 100 rows at a time.
  - `readEvents(id, firstSequence)` returns the events from that sequence, without the snapshot.
  - `publish(…)` stores a `DomainEventMessage` exactly as given (aggregate id, sequence number, type) and dispatches
    it like any other event.
  - `lastSequenceNumberFor` and `storeSnapshot` work as in Axon.
  - The tracking methods (`openStream`, tokens) and `subscribe` throw `UnsupportedOperationException`.
- **`EventStoreBrowser`** (in `axon-thin-api`, provided as a bean by **both** `axon-thin` and `axon-thin-v4-adapter`,
  so debug pages built on it survive the engine switch) reads the event table directly:
  - **Rows are raw:** payload and metadata as text (your serializer's JSON), nothing deserialized, so it also lists
    event types the running code no longer knows.
  - **Paging is by keyset;** reads by id return rows in the requested order.
  - **`decode(...)`** uses the engine's event serializer. Metadata is decoded right away (plain values, never fails
    on a missing class). The payload is decoded only when first accessed, and the outcome is `Payload.Decoded(value)`,
    `Payload.Unknown(type)` (class not on the classpath) or `Payload.Failed(error)` (unreadable). One bad row never
    breaks a list.

  ```kotlin
  // list: metadata only, payloads never deserialized
  val rows = browser.latest(limit = 50, beforeGlobalIndex = lastSeenIndex, filter = EventStoreBrowser.Filter(aggregateType = "Task"))
  val list = browser.decode(rows).map { it.row.eventIdentifier to it.metaData["correlationId"] }
  browser.aggregate(taskId, afterSequence = lastSeenSequence, limit = 50)       // one stream, page by page

  // details: fetch the clicked events by id, then read the payloads
  for (event in browser.decode(browser.events(listOf(id1, id2)))) {
      when (val p = event.payload) {
          is Payload.Decoded -> render(p.value)             // e.g. `when (p.value) { is TaskCreatedEvent -> … }`
          is Payload.Unknown -> renderRaw(event.row.payload)
          is Payload.Failed  -> renderRaw(event.row.payload, p.error)
      }
  }
  browser.decode(browser.event(id)!!).payloadAs<TaskCreatedEvent>()             // typed, or null for anything else

  // like eventStore.readEvents(id).asSequence(): lazy, page by page — but every event from 0, no snapshot, no failures
  browser.readEvents(taskId).map { it.metaData["correlationId"] to it.payloadAs<TaskRenamedEvent>() }
  ```

  Existing `eventStore.readEvents(id).asSequence().map { … }` code keeps working unchanged on both engines.
  `DomainEventStream` is an `Iterator`, and thin's `EventStore` bean keeps Axon's semantics, including the snapshot as
  the first element.

  Configure the table with `axon.thin.event-store.table-prefix` / `domain-event-table` on both engines. The v4
  adapter reads the same keys, so the configuration doesn't change at the switch.

**The pre-thin bulk pattern keeps working unchanged**, verified on both engines (`TaskBulkRenameService`):
- rename commands flagged `bulk = true`, recorded on the events so the aggregates stay correct;
- then one `GenericDomainEventMessage("BulkTaskRename", "BulkTaskRename-<uuid>", 0, TasksBulkRenamedEvent(…))`
  published to the `EventStore`;
- the summary projection skips flagged singles and applies the bulk event in one batch.

Creating an aggregate by publishing its first event directly also works. Replays use `global` order, so the bulk
events stay in place.

On thin the pattern is **no longer needed**: a batch handler gets the same effect (the benchmark shows 11 round trips
either way) without the flag and the extra event. Convert projections at your own pace. Converted projections
replay correctly in either order.

### Payload column types

Thin reads and writes `payload` / `meta_data` stored in any of three ways. It detects each column's type on first
use per process (`axon.thin.event-store.payload-column: auto | binary | oid | text`):

| Column type | How thin reads / writes | Notes |
|---|---|---|
| `bytea` (`blob` on H2) | bytes | Axon-compatible with the bytea dialect or orm override |
| `oid` | `lo_get(col)` / `lo_from_bytea(0, ?)` | Hibernate's default for Axon's `@Lob byte[]`; each payload is a separate large object (slower); thin frees replaced snapshots' large objects |
| `text` (`clob` on H2) | UTF-8 strings | readable JSON in SQL; Axon 4 cannot map it |

This covers aggregates, snapshots, replays, the `EventStore` bean and `EventStoreBrowser`.
- `PayloadStorageTest` runs the same scenario per storage (`text` on H2 and PostgreSQL, `oid` on PostgreSQL). The `oid`
  run uses tables Hibernate creates from Axon's own mapping, and Axon's `JpaEventStorageEngine` and thin read each
  other's large objects.
- **Converting:** Liquibase changesets for `oid → bytea`, `oid → text`, `bytea ↔ text` are in `docs/liquibase`, with a
  procedure. `PayloadConversionTest` runs them against real data and restarts the application on the new type.

### Event store schema: `axon.thin.event-store.schema`

Thin can create or check its own two tables and the `global_index` sequence. It uses your configured prefix and
table names, the sequence settings, and the payload column type:

```yaml
axon.thin.event-store:
  schema: create-if-missing   # none (default) | validate | create-if-missing
  payload-column: text        # the type used when creating: bytea/blob (binary), text/clob (text), oid (PostgreSQL)
```

| Mode | Use | What it does |
|---|---|---|
| `none` | default | nothing |
| `create-if-missing` | dev, H2 tests (no Liquibase needed) | creates the event and snapshot tables (with the unique index on aggregate id + sequence) and the sequence (increment = `allocation-size`) **only if missing**; never alters or drops; then validates |
| `validate` | production | checks tables and columns, **the unique index** (Hibernate's `validate` can't), the sequence's existence and **increment = `allocation-size`**, and a forced `payload-column` type; fails startup listing every problem |

- **Ordering:** it runs after Liquibase, Flyway and `spring.sql.init` (the event store bean is marked
  `@DependsOnDatabaseInitialization`), so `validate` checks the final schema.
- **Hibernate:** your read models stay with Hibernate as before. Thin's tables are not JPA entities, so
  Hibernate's `validate` ignores them.
- **Databases:** creation supports H2 and PostgreSQL; validation works on any database with `information_schema`.
- **Tests:** `EventStoreSchemaTest` checks create-if-missing (per payload type, idempotent, leaves existing
  schemas alone) and validate (empty database, missing unique index, wrong increment, correct schema).

### Processing groups

Thin accepts `@ProcessingGroup` and ignores it: every event handler bean receives every event, as one subscribing
group. The group name never reaches the event rows. Axon only uses it for tracking tokens (`token_entry`) and
sagas, and thin uses neither.

### Why there is still a (small) unit of work

The transaction gives atomicity. Thin still keeps a small internal scope per command. It is not Axon's
`UnitOfWork` API. It provides:
1. **Deferred events.** Events are stored and dispatched after the handler returns, which is Axon's order.
2. **An aggregate identity map.** An aggregate is loaded once and shared by nested commands, so sequence numbers
   stay consistent. It spans the whole chunk, so each aggregate is loaded once per `sendAllAndWait`.
3. **The thread-bound `AggregateLifecycle` scope** that `apply()` needs.
4. **Correlation metadata.**
5. **Discarding events of a failed nested command.**

## Assumptions

Thin is built for one deployment shape. These are the things it relies on; breaking one of them is where surprises
come from.

**Architecture**
- **Everything is synchronous and local.** Commands are handled on the caller's thread, and projections run in
  the command's transaction (subscribing). There is no command routing and no distributed command bus.
- **Processes share only the database.** The backend and the jobs communicate through the event store and the read
  models, nothing else.
- **No tracking processors.** Nothing reads the event store by `global_index` position, and `token_entry` is not
  used. A stale row left there by an old Axon processor is harmless.
- **Queries go directly to repositories or services.** There is no `QueryGateway`.
- **Projections tolerate replays in append order.** Replays use `global_index` order, which matches live handling
  unless several processes wrote with pooled sequence blocks (see *Inversions*); `per-aggregate` order is available
  for projections that are independent across aggregates.

**Data and schema**
- **Unique index on `(aggregate_identifier, sequence_number)`.** It is the only concurrency guard. An orm.xml
  `<table>` override drops Axon's own `@Table` index. `axon.thin.event-store.schema: validate` checks it at startup.
- **Payload columns on PostgreSQL** may be `bytea`, `oid` (large objects) or `text`. Each column's type is detected on
  first use per process, so restart after converting. See *Payload column types*.
- **The `global_index` sequence's `INCREMENT BY` equals `allocation-size`** (50 by default, as Hibernate creates
  it).
- **One serializer configuration in every process:** the same Jackson `ObjectMapper` setup and the same Axon
  serializer settings.
- **Aggregates are Jackson-serializable**, because snapshots serialize the aggregate itself.

**Evolution and deployment**
- **Event classes evolve additively:** new fields are optional or have defaults, and deserialization tolerates
  unknown fields. There are no upcasters.
- **Readers deploy before writers.** Backend and jobs deploy the same domain module version (events, aggregates,
  projections, serializer config).
- **An aggregate ignores event types it can't load.** That's the same as Axon, but it means aggregate state can
  be silently wrong after an out-of-order deploy.
- **New aggregates get random ids.** Duplicate creates are not expected; if one happens, it fails with
  `AggregateStreamCreationException`.
- **Resetting projections is manual** (Liquibase), and `ProjectionMigrator.migrate()` runs before the apps start.
  Different branches never run against the same database at the same time.

## Running several processes (backend + jobs)

Both processes run thin against one database. There is **no locking**: concurrency is optimistic, and the database
is the arbiter.

| Concern | What happens | What to do |
|---|---|---|
| Two writers change the same aggregate | The second append violates the unique index → `ConcurrencyException`; its whole chunk rolls back (events and projections) | `BulkOptions(concurrencyRetries = n)` or `axon.thin.concurrency-retries` re-runs the chunk with a fresh load, so the delta lands on the latest state. Retries use exponential backoff with jitter (5 ms … 1 s). |
| Same field changed by two users or processes | Last writer wins; both changes are in the event store | Accepted by design; the history shows who changed what |
| Large job chunk vs frequent UI edits on the same aggregates | Optimistic chunks can **starve**: every retry loses to another writer | Keep job chunks small (100–500) when they overlap with interactive work. The concurrency test shows a 50-aggregate chunk under users with no think time losing 20 retries in a row. |
| Validation across aggregates (done in code) | Each process only sees the other's *committed* data, so two processes can both pass the same rule | Put a database constraint behind rules that must hold |
| Read-model rows shared by several aggregates (totals, counters) | Updates from two transactions can overwrite each other | `@Version` on the entity, or atomic `update … set x = x + 1` |
| Per-aggregate read-model rows | Safe: they're written in the writer's transaction, in sequence order | — |
| Event-handler side effects (email, messages) | They run inside the transaction; after a rollback they have still happened, and after a retry they happen twice | Outbox table, or send after commit |
| `global_index` order | Each process draws its own blocks of 50, so an aggregate's later event can get a lower index (≈5 % inversions measured) | Matters for `global`-order replays, which detect and report it. Use a sequence increment of 1 once all processes run thin. |
| Snapshots from both processes | Duplicates are ignored, older snapshots replaced | — |

`PostgresConcurrencyTest` covers this. Two application contexts (a "backend" with two users, and "jobs" with chunks of
50) write concurrently to the same 50 aggregates. That produces 1,250 events, with 48 retries logged in one run.
Afterwards every stream has contiguous sequence numbers, every read model equals the aggregate's last event, and no
event is lost or duplicated.

## Benchmark

`mvn test -Ppostgres -pl examples/task-app-thin -Dtest=PostgresChunkBenchmarkTest`

Setup:
- MacBook Air M1 (16 GB), Docker Desktop, `postgres:17-alpine` started with `fsync=off`.
- Hibernate batch size 100, `reWriteBatchedInserts=true`.
- Four projections: per-event `task_summary`, `task_comment` and `task_activity`, plus the batch `task_title_index`.

**Read the round-trip counts, not the milliseconds.** Absolute times are optimistic: local Docker, no disk sync, no
network latency. On a real network every round trip adds latency, which makes the round-trip column matter more.

| scenario | chunks | commands | ms | ms/command | event store | `task_summary` (per-event) | `task_title_index` (batch) | other projections |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| create 100, one chunk | 1 | 100 | 41 | 0.41 | 2 | 101 | 2 | 3 |
| create 1k, one chunk | 1 | 1,000 | 341 | 0.34 | 2 | 1,010 | 11 | 30 |
| rename 100, one chunk | 1 | 100 | 60 | 0.60 | 4 | 101 | 2 | 3 |
| rename 1k, one chunk | 1 | 1,000 | 329 | 0.33 | 4 | 1,010 | 11 | 30 |
| create 10k, chunks of 500 | 20 | 10,000 | 2,330 | 0.23 | 40 | 10,100 | 120 | 300 |

**Axon 4 vs axon-thin, same scenarios** (`EngineBenchmark`, run by both apps:
`mvn test -Ppostgres -pl examples/task-app-axon4,examples/task-app-thin -Dtest='*EngineBenchmarkTest'`):

| engine | scenario | ms | event store | `task_summary` | `task_title_index` (batch, thin only) | other |
|---|---|---:|---:|---:|---:|---:|
| Axon 4 | create 1k: commands, one chunk | 2,435 | 1,020 | 2,000 | — | 1,020 |
| Axon 4 | rename 1k: commands, one chunk | 4,358 | 3,020 | 2,000 | — | 1,020 |
| Axon 4 | rename 1k: bulk pattern (flagged + bulk event) | 3,021 | 3,021 | 11 | — | 1,020 |
| axon-thin | create 1k: commands, one chunk | 409 | 2 | 1,010 | 11 | 30 |
| axon-thin | rename 1k: commands, one chunk | 542 | 4 | 1,010 | 11 | 30 |
| axon-thin | rename 1k: bulk pattern (flagged + bulk event) | 269 | 5 | 11 | 11 | 30 |

- **Axon 4 pays per command:** a snapshot read, an event read and an insert for each command, and projection writes
  flushed per command (no JDBC batching).
- **Thin pays per chunk:** 6–8× faster on the same code.
- **The bulk pattern and a batch projection cost the same on thin** (11 round trips): the pattern is no longer
  needed.

What it shows (thin-only table above):
- **The event store costs a constant number of round trips per chunk:** 2 for creates (index + insert), 4 for
  updates (+ snapshots + events), whatever the chunk size.
- **A per-event projection costs about one round trip per command.** One `findById` or merge per event dominates
  everything else.
- **A batch projection costs 1 read plus one write per 100 rows:** 11 instead of 1,010 for 1k commands.
  `TaskTitleIndexProjection` (test sources) is the pattern: `findAllById` per chunk, `Persistable.isNew` so new rows
  are INSERTed without a SELECT first, JDBC-batched writes, and one `delete … where id in (…)`. `ThinBatchProjectionTest`
  asserts exactly 1 + 10 round trips. Converting hot projections is the main performance lever.

## Limitations

**Not supported (yet)**
- **Aggregates:** `@AggregateMember` entities, `AggregateLifecycle.createNew`, `@TargetAggregateVersion`, and
  state-stored (JPA) aggregates.
- **Snapshots:** `@Aggregate(snapshotFilter)` is ignored.
- **Upcasters** (events must evolve additively).
- **Other Axon features:** sagas, deadlines, queries (`QueryGateway`, `@QueryHandler`), handler interceptors,
  `UnitOfWork` parameters, custom correlation providers, and tracking processors.
- **Async snapshot executor:** snapshots are built synchronously after commit.

**Deliberate differences from Axon 4**
- **Chunk visibility:** projections see a chunk's events only after its last command. Validation uses `ChunkContext`
  for what the chunk did so far.
- **Dispatch order:** each handler bean receives a whole chunk (in event order, runs batched per batch handler) before
  the next bean does. Axon dispatches event by event across beans.
- **Batch `@EventHandler(List<…>)` handlers** exist only in thin.
- **Unreadable snapshots** are skipped (full replay) instead of failing the command.
- **Duplicate command handlers** fail at startup instead of being logged.
- **`@ProcessingGroup` is ignored**, and there are no tracking tokens.
- **Replays** run only through `ProjectionMigrator` (no tracking processors). They use `global_index` order by
  default, with inversion detection.

**Operational notes**
- **No locking:** heavy overlap between jobs and users on the same aggregates means more retries (see above).
- **`ChunkContext.aggregate()` is read-only.** Change aggregates through commands only.
- **Replays read all history**, so every old event version must still deserialize.

## Migrating a project from Axon 4

### Checklist

1. **Run the scanner** (`scripts/axon-usage-scan`) and check its MISSING and PARTIAL rows against the limitations
   above.
2. **Database:** check that the unique index on `(aggregate_identifier, sequence_number)` exists. Payload columns
   may stay `oid`; see `docs/liquibase` for converting them to `bytea` or `text`.
3. **Swap engines in both processes together:** backend and jobs switch in the same release. Do the code migration
   below in both.
4. **Before switching:** run your suite against thin on PostgreSQL (`-Ppostgres`), not just H2.
5. **Afterwards, use the new API where it pays off:**
   - `sendAllAndWait` for bulk work, with `concurrencyRetries` for jobs;
   - `ChunkContext` in validation;
   - batch handlers for hot projections;
   - `@ReplayInto` plus `migrate()` for projections you want to be able to rebuild.

### Code migration

The example apps are the reference diff: `examples/task-app-axon4` and `examples/task-app-thin` run the same model,
and the steps below are exactly what separates them.

**Optional step 0: adopt the new API while still on Axon.** Add `axon-thin-api` and `axon-thin-v4-adapter` next to the
Axon starter. `BulkCommandGateway`, `BulkOptions` and `ChunkContext` then work on Axon 4, so code using them doesn't
change when the engine switches. `@ReplayInto` can be added at any time; Axon ignores it.

#### 1. POM: application modules (the deployables)

Before:
```xml
<dependency>
    <groupId>org.axonframework</groupId>
    <artifactId>axon-spring-boot-starter</artifactId>
    <exclusions>
        <exclusion>
            <groupId>org.axonframework</groupId>
            <artifactId>axon-server-connector</artifactId>
        </exclusion>
    </exclusions>
</dependency>
<!-- if you used step 0 -->
<dependency>
    <groupId>app.dc8</groupId>
    <artifactId>axon-thin-v4-adapter</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

After:
```xml
<dependency>
    <groupId>app.dc8</groupId>
    <artifactId>axon-thin</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

**Remove every Axon engine artifact** from the deployables:
- `axon-spring-boot-starter` / `axon-spring-boot-autoconfigure`;
- `axon-server-connector`;
- `axon-micrometer` / `axon-metrics` and `axon-tracing-*`;
- distributed command bus extensions (JGroups, Spring Cloud).

**This one is not optional.** If Axon's auto-configuration is still on the classpath, you get a mix of both engines:
- depending on the order the auto-configurations run in, Axon's `CommandGateway` can win (thin's backs off when
  one exists), so Axon keeps handling commands;
- there are then two `EventGateway` beans, so injecting one fails with "expected single matching bean".

**You don't add Axon jars back.** `axon-thin` brings the same ones the starter did, for their *types*:
`axon-messaging`, `axon-modelling`, `axon-eventsourcing`, `axon-spring`, `axon-configuration` and `axon-disruptor`.
What it leaves out:
- the engine, `axon-spring-boot-autoconfigure`;
- `xstream`: only Jackson is used, so code referencing `XStreamSerializer` stops compiling.

The starter brought nothing non-Axon beyond `slf4j`, so removing it drops no Spring or JPA libraries. Also not
included, so keep them if you use them:
- Axon extensions (`axon-kotlin`, `axon-micrometer`, `axon-tracing-*`);
- `axon-test`, in test scope.

If `mvn dependency:analyze` should be clean, declare the Axon jars you use explicitly, without a version.

**Versions:** if you import `axon-bom` in `dependencyManagement`, it overrides the Axon versions thin brings. Keep it
at the version thin is built against (4.13.2), or remove the import.

#### 2. POM: model and library modules

Modules that only contain aggregates, events, handlers and projections don't change. They keep their Axon
dependencies for the annotations, typically `provided`. Add `axon-thin-api` wherever you use `BulkCommandGateway`,
`ChunkContext` or `@ReplayInto`:
```xml
<dependency>
    <groupId>app.dc8</groupId>
    <artifactId>axon-thin-api</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

`axon-test` can stay in test scope: `AggregateTestFixture` unit-tests aggregates without any engine.

#### 3. `application.yml`

Before:
```yaml
spring:
  jpa:
    mapping-resources: META-INF/axon-orm.xml      # renames Axon's JPA entities (axon_ prefix)
axon:
  axonserver:
    enabled: false
  serializer:
    general: jackson
    events: jackson
    messages: jackson
  eventhandling:
    processors:
      task-summary:
        mode: subscribing
```

After:
```yaml
axon:
  thin:
    event-handler-error-mode: propagate          # if you registered PropagatingErrorHandler; Axon's default is log
    concurrency-retries: 0                       # default for sendAndWait; jobs pass BulkOptions per call
    event-store:
      table-prefix: axon_                        # same tables your axon-orm.xml pointed to
      # global-index: { strategy: identity }     # only if your orm.xml maps global_index as IDENTITY
      # global-index: { sequence-name: my_seq, allocation-size: 50 }   # if your orm.xml defines its own generator
```

- **`axon.*` keys are no longer read.** Thin ignores `axon.serializer.*`: it always builds Axon's `JacksonSerializer`
  from your `ObjectMapper` (a `defaultAxonObjectMapper` bean if you have one). The `axon.eventhandling.processors.*`
  keys are ignored too, because everything is subscribing.
- **`axon-orm.xml`** is only needed if something still relies on Hibernate knowing Axon's entities, for example
  `ddl-auto` creating those tables. Thin writes the tables with JDBC. If Liquibase owns the schema, drop the file from
  `spring.jpa.mapping-resources`.

#### 4. Code that must change

| Axon 4 code | On thin | Change to |
|---|---|---|
| `EventProcessingConfigurer` / `Configurer` / `ConfigurerModule` customization (`registerSubscribingEventProcessor`, error handlers, …) | the beans don't exist: **startup fails** on an injection point | delete it; use `axon.thin.event-handler-error-mode` for the error handler |
| injecting `EventStore` | provided: `readEvents`, `publish`, `lastSequenceNumberFor`, `storeSnapshot` | nothing, unless you use tracking methods |
| injecting `EventBus` | not provided | `EventStore` or `EventGateway.publish(…)` |
| injecting `CommandBus` | not provided | `CommandGateway` |
| `Repository<T>` (`load(id).execute { … }`) | not provided | send a command to the aggregate |
| `QueryGateway` / `@QueryHandler` | not supported | call the service or repository directly |
| injecting `Configuration`, `TokenStore`, `EventProcessingModule` | not provided | remove |
| sagas, deadlines, `@AggregateMember`, upcasters | not supported | see *Limitations* |

#### 5. Code that stays as it is

- `@Aggregate` (including `snapshotTriggerDefinition`), `@AggregateIdentifier`, `@CommandHandler` (constructors
  too), `@CreationPolicy`, `@EventSourcingHandler`, and `AggregateLifecycle.apply` / `markDeleted`.
- `@EventHandler` projections (`@ProcessingGroup` is accepted and ignored), `@MetaDataValue`, `@Timestamp`,
  `@SequenceNumber`, and Spring-bean handler parameters.
- `CommandGateway` (`send`, `sendAndWait`, callbacks), `EventGateway`, and dispatch interceptors.
- `SnapshotTriggerDefinition` beans (for example `EventCountSnapshotTriggerDefinition(snapshotter, 100)`); thin
  provides the `Snapshotter` they take.
- Exceptions you catch: `AggregateNotFoundException`, `AggregateDeletedException`, `ConcurrencyException`,
  `AggregateStreamCreationException` and `CommandExecutionException` are the same classes, thrown in the same
  situations.

#### 6. Verify

- **Startup log:** `axon-thin registered N command handler(s), M aggregate(s) [Task, …] and K event handling
  bean(s)`. Compare the counts with your code.
- **No Axon engine left:** Axon's startup lines (`AxonAutoConfiguration`, `EventProcessor … started`) must not
  appear, and no rows may appear in `token_entry`.
- **Tests:** run them on PostgreSQL (`-Ppostgres`); H2 hides locking and column-type differences. Re-run the scanner:
  anything still marked MISSING is either unused or needs one of the changes above.

## Build & test

```bash
mvn install                                  # H2: engine unit tests, the contract suite on both engines, thin-only suites
mvn test -Ppostgres -pl examples/task-app-axon4,examples/task-app-thin   # the same, on PostgreSQL 17 in Docker
mvn test -Ppostgres -pl examples/task-app-thin -Dtest=PostgresChunkBenchmarkTest   # chunk timings (prints a table)
```

**PostgreSQL via Testcontainers (`-Ppostgres`)**
- Needs Docker (Docker Desktop is fine, including on Apple Silicon: `postgres:17-alpine` is multi-arch).
- There is one container per test JVM and one database per test class (`PostgresSupport`), so no local setup is needed.
- If Testcontainers can't find Docker, enable *Docker Desktop → Settings → Advanced → "Allow the default Docker
  socket to be used"*, or put `docker.host=unix:///Users/<you>/.docker/run/docker.sock` in
  `~/.testcontainers.properties`.
- **PostgreSQL-only tests:**
  - `PostgresConcurrencyTest`: two application contexts ("backend" and "jobs") on one database write to the same
    aggregates at the same time. It checks for contiguous sequences and read models equal to the last event, and
    reports `global_index` order inversions.
  - `PostgresChunkBenchmarkTest`: timings and round trips per chunk.
- **Axon 4 on PostgreSQL:** the tests use `bytea` payload columns through `ByteaEnforcedPostgresSQLDialect` (from Axon's
  reference guide), except `OidPayloadStorageTest`, which uses Hibernate's default `oid`.
- **PostgreSQL-only tests** also include `OidPayloadStorageTest` and `PayloadConversionTest`.

JDK 21+ (bytecode target 21), Kotlin 2.4, Spring Boot 3.5, H2 and PostgreSQL 17.
