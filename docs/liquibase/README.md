# Converting Axon's payload columns (PostgreSQL)

axon-thin reads and writes `payload` / `meta_data` in any of three storages. It detects each column's type on first
use per process (`axon.thin.event-store.payload-column: auto`):

| Column type | Storage | Readable in SQL | Axon 4 can map it |
|---|---|---|---|
| `oid` | large objects: Hibernate's default for Axon's `@Lob byte[]` | no (a number) | yes (default mapping) |
| `bytea` | binary in the row | via `convert_from(payload, 'UTF8')` | yes, with the bytea dialect / orm override |
| `text` | UTF-8 text in the row | yes | no (it would need a custom Hibernate type) |

These changesets move between them. They are **Liquibase formatted SQL**: include one in your changelog (see
`db.changelog-example.xml`) and set the `axon_prefix` parameter to your table prefix (`axon_`, or empty).

| File | From → to | Frees large objects | Rollback |
|---|---|---|---|
| `oid-to-bytea.sql` | `oid` → `bytea` | yes | back to `oid` (new large objects) |
| `oid-to-text.sql` | `oid` → `text` | yes | back to `oid` |
| `bytea-to-text.sql` | `bytea` → `text` | — | back to `bytea` |
| `text-to-bytea.sql` | `text` → `bytea` | — | back to `text` |

All four (and the `oid-to-bytea` rollback) are executed against real data by `PayloadConversionTest`
(`mvn test -Ppostgres -pl examples/task-app-thin -Dtest=PayloadConversionTest`). Each scenario:
1. an application writes events and snapshots on the old type;
2. it stops, and the changeset runs;
3. a fresh application detects the new type, reads everything, and keeps writing.

## Procedure
1. **Test on a copy of production first.** `ALTER … TYPE` rewrites and locks each table. Time it, and plan a
   maintenance window for big tables.
2. **Stop every application** writing the tables. A running axon-thin process keeps the type it detected at its
   start, and would fail against the converted columns.
3. **Run the changeset.** For `oid-*`, the large objects are unlinked in the same transaction. They are not
   removed with their rows, and would otherwise stay behind.
4. **Start the applications:**
   - **axon-thin** detects the new type; no configuration change. If you forced `payload-column`, update it.
   - **An Axon 4 process** on these tables needs its mapping changed **in the same release**:
     - to `bytea`: the `ByteaEnforcedPostgresSQLDialect` (in Axon's reference guide; a copy is in
       `examples/task-contract-tests/…/PostgresSupport.kt`), or a column override in `axon-orm.xml`;
     - to `text`: not supported by Axon. Only convert to `text` once no process needs Axon (including as a
       rollback path).

## Choosing
- **`bytea`:** keeps Axon 4 (and a rollback to it) possible. For readable SQL, add a view:
  ```sql
  create view axon_event_json as
  select global_index, aggregate_identifier, sequence_number, type, payload_type, time_stamp,
         convert_from(payload, 'UTF8') as payload, convert_from(meta_data, 'UTF8') as meta_data
  from axon_domain_event_entry;
  ```
- **`text`:** plain readable JSON everywhere, for projects that run axon-thin only and serialize to JSON (or other
  text).

Axon's other tables that may use `oid` (`token_entry.token`, saga tables) are not used by axon-thin and are left
as they are.
