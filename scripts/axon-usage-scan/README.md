# axon-usage-scan

Makes an inventory of every Axon Framework usage in a Spring / Kotlin / Java multi-module project. For each
feature it shows whether **axon-thin** supports it, so you can decide what to build next.

Needs Python 3.9+ and nothing else (stdlib only). It matches source text with regexes (it does not compile the
code): good for an inventory, so check the reported locations before acting on them.

```bash
python3 scripts/axon-usage-scan/axon_usage_scan.py ~/work/my-project -o axon-usage-report
# several roots at once, example locations per item, extra excluded dirs:
python3 scripts/axon-usage-scan/axon_usage_scan.py ~/work/svc-a ~/work/svc-b --examples 10 --exclude testdata
```

Outputs `axon-usage.md` (summary, also printed) and `axon-usage.json` (full detail, including every handler signature).

## What it collects

* **Features by thin support**: every Axon type referenced (imports, annotations, fully-qualified names), with
  the `SUPPORTED / PARTIAL / MISSING / N/A` status from `CATALOG`, reference counts per module, and, for
  annotations, where they are applied (class / method / constructor / property / parameter).
* **Handler shapes**: every `@CommandHandler`, `@EventHandler`, `@EventSourcingHandler`, `@QueryHandler`,
  `@SagaEventHandler`, `@DeadlineHandler`, interceptor and reset handler. For each: whether it sits inside an
  `@Aggregate` / `@Saga` or on a constructor, and which parameter kinds it uses beyond the payload (Axon
  parameter annotations, `Message` types, `UnitOfWork`, Spring beans…). Also the return types of command handlers.
* **Calls on Axon types**: `CommandGateway.sendAndWait`, `EventGateway.publish`, `AggregateLifecycle.apply`,
  `QueryGateway.query`, axon-kotlin extension functions, …
* Classes that implement or extend Axon types (interceptors, custom resolvers, …) and `@Bean` methods that return
  Axon infrastructure (engine customization).
* `axon.*` keys from `application*.yml|properties` / `bootstrap*`.
* Axon dependencies and versions from `pom.xml`, `build.gradle(.kts)` and `libs.versions.toml`.
* **Unclassified**: Axon types not yet in `CATALOG`. Add them there with a status.

When axon-thin gains a feature, update its entry in `CATALOG` so the report stays accurate.
