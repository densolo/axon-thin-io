# axon-thin

A thin, drop-in replacement for the subset of **Axon Framework 4.13** we actually use:
`@CommandHandler` / `@EventHandler` on Spring beans (using Axon's own annotations and types), `CommandGateway`
and `EventGateway`. On top of that it adds `BulkCommandGateway.sendAllAndWait`, which sends many commands in one
transaction.

Runtime model: commands are handled synchronously on the caller's thread. Events go to event handlers
**in the same transaction**, which matches Axon's subscribing processors on a `SimpleEventBus`. There is no event
store and no Axon Server. Queries go directly to services and repositories (no `QueryGateway`).

## Modules

```
axon-thin-api            BulkCommandGateway (no dependencies): the only API that is not an Axon type
axon-thin                the thin runtime + Spring Boot auto-configuration (depends on axon-messaging for types only)
axon-thin-v4-adapter     BulkCommandGateway on real Axon 4 (a TransactionTemplate around sendAndWait)

examples/task-model           tasks & comments: commands, events, JPA entities, handlers, projections, query service
                              (engine-agnostic: axon-messaging is `provided`, so no engine is wired here)
examples/task-contract-tests  abstract JUnit suite (src/main) describing the expected behaviour
examples/task-app-axon4       task-model + Axon 4 Spring Boot starter  -> runs the contract suite
examples/task-app-thin        task-model + axon-thin                   -> runs the same contract suite

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
| event bus | `SimpleEventBus`, subscribing processors | in-memory dispatch |
| transaction | one per command (PROPAGATION_REQUIRED, joins the caller's transaction) | same |
| events dispatched | after the handler returns, before commit (UoW prepare-commit) | same |
| handler exception | original runtime exception rethrown; checked exceptions → `CommandExecutionException` | same |
| event-handler exception | `PropagatingErrorHandler` configured | `axon.thin.event-handler-error-mode=propagate` (default `log`, like Axon) |
| correlation metadata | `MessageOriginProvider` (`correlationId`, `traceId`) | same |
| handler parameters | payload, `Message` types, `MetaData`, `@MetaDataValue`, `@MessageIdentifier`, `@Timestamp`, Spring beans | same |
| `sendAllAndWait` | adapter: one TX around sequential `sendAndWait` | native: one TX, one unit of work per command |

**Not supported yet:** aggregates (`@Aggregate`, `AggregateLifecycle`), sagas, deadlines, queries, handler
interceptors, `UnitOfWork` parameters, custom correlation providers, and `@ProcessingGroup` (the annotation lives in
`axon-configuration`). Run the scanner on the real project to decide which of these are needed.

## Build & test

```bash
mvn install                               # everything: 7 engine unit tests + the 20-test contract suite, run twice
mvn test -pl examples/task-app-axon4      # contract suite on Axon 4
mvn test -pl examples/task-app-thin       # contract suite on axon-thin
```

JDK 21+ (bytecode target 21), Kotlin 2.4, Spring Boot 3.5, H2. Postgres is planned. The model only uses
standard JPA, so switching databases should need only a datasource change and a Testcontainers profile.
