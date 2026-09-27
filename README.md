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
axon-thin-api            the only non-Axon API, dependency-free: BulkCommandGateway + BulkOptions, ChunkContext, @ReplayInto
axon-thin                the thin runtime + Spring Boot auto-configuration (uses Axon jars for their types only)
axon-thin-v4-adapter     the same API on real Axon 4: sendAllAndWait (+ retry) and ChunkContext

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
- **One pass for all:** every projection to replay is filled in a single pass. The pass goes in
  `(aggregate_identifier, sequence_number)` order (per-aggregate order guaranteed, the same order on every run), in
  pages of `axon.thin.replay-page-size` events (default 1000), one transaction per page. Pages are delivered like live
  chunks: batch handlers get each page as one list.
- **Handler failures** fail the migration. Events published by handlers during a replay are dropped.
- **Resetting is up to you.** To rebuild a projection, empty or rename its table (for example a Liquibase change that
  switches to `task_summary_v3`); the next `migrate()` fills it. Beans without `@ReplayInto` are never replayed.

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
- **Projections are independent across aggregates.** A projection only needs per-aggregate event order (replays
  rely on this).

**Data and schema**
- **Unique index on `(aggregate_identifier, sequence_number)`.** It is the only concurrency guard. An orm.xml
  `<table>` override drops Axon's own `@Table` index, so check that your database still has it.
- **`bytea` payload columns on PostgreSQL** (not `oid`).
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
| `global_index` order | Each process draws its own blocks of 50, so the index is **not** in commit order (≈5 % inversions measured) | Irrelevant without tracking readers; replays use per-aggregate order |
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

What it shows:
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
- **PostgreSQL `oid` payload columns:** thin reads and writes `bytea`.
- **Async snapshot executor:** snapshots are built synchronously after commit.

**Deliberate differences from Axon 4**
- **Chunk visibility:** projections see a chunk's events only after its last command. Validation uses `ChunkContext`
  for what the chunk did so far.
- **Dispatch order:** each handler bean receives a whole chunk before the next bean does (Axon dispatches event by
  event across beans).
- **Batch `@EventHandler(List<…>)` handlers** exist only in thin.
- **Unreadable snapshots** are skipped (full replay) instead of failing the command.
- **Duplicate command handlers** fail at startup instead of being logged.
- **`@ProcessingGroup` is ignored**, and there are no tracking tokens.
- **Replay order** is per aggregate, not `global_index`.

**Operational notes**
- **No locking:** heavy overlap between jobs and users on the same aggregates means more retries (see above).
- **`ChunkContext.aggregate()` is read-only.** Change aggregates through commands only.
- **Replays read all history**, so every old event version must still deserialize.

## Migrating a project from Axon 4

1. **Run the scanner** (`scripts/axon-usage-scan`) and check its MISSING and PARTIAL rows against the limitations
   above.
2. **Database:** check that `bytea` payload columns and the unique index on `(aggregate_identifier, sequence_number)`
   exist. Set `axon.thin.event-store.table-prefix` (or the table names) to match your `axon-orm.xml`.
3. **Serializer:** keep `axon.serializer.*=jackson` semantics; thin builds the same `JacksonSerializer` from your
   `ObjectMapper`.
4. **Snapshots:** keep your `SnapshotTriggerDefinition` beans; thin supplies the `Snapshotter`.
5. **Swap engines in both processes together:** replace the Axon starter with `axon-thin`. Backend and jobs switch in
   the same release.
6. **Use the new API where it pays off:**
   - `sendAllAndWait` for bulk work, with `concurrencyRetries` for jobs;
   - `ChunkContext` in validation;
   - batch handlers for hot projections;
   - `@ReplayInto` plus `migrate()` for projections you want to be able to rebuild.
7. **Before switching:** run your suite against thin on PostgreSQL (`-Ppostgres`), not just H2.

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
- **Axon 4 on PostgreSQL** needs `bytea` payload columns (Hibernate's default for `@Lob byte[]` is `oid`). The tests use
  `ByteaEnforcedPostgresSQLDialect`, as recommended in Axon's reference guide. Your production mapping must also
  produce `bytea`.

JDK 21+ (bytecode target 21), Kotlin 2.4, Spring Boot 3.5, H2 and PostgreSQL 17.
