<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# PostgreSQL Extension Surfaces

pgiceberg touches three PostgreSQL extension surfaces: foreign data wrappers,
table access methods, and logical decoding. They overlap in the user-visible
ability to read or write rows, but they enter PostgreSQL at different layers
and should be used for different product goals.

## Independence

The three surfaces are product-independent. Each has its own PostgreSQL entry
point, control catalog, and supported operations. None of them calls into
another surface's module:

| Surface | Source | Entry point | Own control state | Can stand alone for |
| --- | --- | --- | --- | --- |
| FDW | `src/fdw/` | `FOREIGN DATA WRAPPER pgiceberg` | Foreign server / foreign table options | Mapping and querying existing Iceberg tables |
| Table AM | `src/tableam/` | `ACCESS METHOD iceberg` | `pgiceberg.table_bindings` | `CREATE TABLE ... USING iceberg` scans and appends |
| Logical | `src/logical/` | output plugin + background worker | `pgiceberg.logical_mirrors` | Heap → Iceberg CDC mirrors |

They share lower-level Iceberg work through `src/engine/` (options, scans,
appends, pending transaction commits) and `src/common/` (catalog loading,
type mapping, Status helpers). That sharing is intentional: one iceberg-cpp
integration, three PostgreSQL contracts.

```text
  fdw/ ────────┐
  tableam/ ────┼──► engine/ ──► common/ ──► iceberg-cpp
  logical/ ────┘
```

What independence does *not* mean today:

- The extension still ships as one shared library and one `CREATE EXTENSION`.
  Operators enable all three surfaces together; there is no compile-time
  "FDW-only" or "logical-only" build.
- Capability depth differs. FDW is the most complete read/write path. Table AM
  is intentionally narrow (append/scan focused). Logical mirrors are
  INSERT-only CDC. Independence of *role* is not equality of *maturity*.

Do not merge the surfaces. A foreign table, a `USING iceberg` relation, and a
logical mirror answer different ownership questions (external mapping, native
storage, downstream copy) even when they all end in Iceberg files.

## Summary

Use the FDW path when Iceberg, Parquet, or Avro data is external to
PostgreSQL and should be mapped into SQL. Use the table access method path
when the goal is a native PostgreSQL table whose storage is Iceberg. Use the
logical decoding path when PostgreSQL heap tables are the source of truth and
Iceberg is an asynchronous downstream copy.

| Goal | Preferred surface |
| --- | --- |
| Query an existing Iceberg table through PostgreSQL | FDW |
| Import a foreign table definition from Iceberg metadata | FDW |
| Write directly to an Iceberg table from SQL with foreign table semantics | FDW |
| Expose `CREATE TABLE ... USING iceberg` | Table access method |
| Drive Iceberg schema changes from PostgreSQL DDL | Table access method |
| Mirror committed PostgreSQL heap inserts into Iceberg | Logical decoding |
| Build a CDC pipeline from PostgreSQL to a lakehouse table | Logical decoding |

## FDW

PostgreSQL calls an FDW through an `FdwRoutine`. The planner asks the FDW for
size estimates, paths, and plans. The executor later calls scan and modify
callbacks such as `BeginForeignScan`, `IterateForeignScan`,
`ExecForeignInsert`, `ExecForeignUpdate`, and `ExecForeignDelete`.

In PostgreSQL 18.4, the relevant core paths are:

- `src/backend/optimizer/path/allpaths.c`: calls `GetForeignRelSize`.
- `src/backend/executor/nodeForeignscan.c`: initializes the FDW routine and
  calls `BeginForeignScan` and `IterateForeignScan`.
- `src/backend/executor/nodeModifyTable.c`: calls foreign modify callbacks for
  writes.
- `src/backend/commands/foreigncmds.c`: calls `ImportForeignSchema`.

In pgiceberg, the main Iceberg FDW lives under `src/fdw/`. It supports scans,
`IMPORT FOREIGN SCHEMA`, and DML callbacks, delegating the actual Iceberg
reads and writes to the shared engine under `src/engine/`. The generic Parquet
and Avro FDWs live under `src/utilities/` and are read-only file wrappers.

The FDW path matches Iceberg well because Iceberg tables live outside
PostgreSQL storage. The PostgreSQL relation is a mapping over an external
catalog table. This keeps the implementation focused on catalog loading,
schema mapping, scan planning, and Iceberg commits rather than pretending to
be heap storage.

The main tradeoff is that a foreign table is not a normal PostgreSQL heap
table. PostgreSQL indexes, heap MVCC, table storage, VACUUM, and most physical
table maintenance are not owned by the FDW.

## Table Access Method

A table access method is PostgreSQL's storage interface for a normal relation.
PostgreSQL stores the relation in `pg_class` and `pg_attribute`, then calls
the relation's `TableAmRoutine` through `rel->rd_tableam`.

This is a lower-level and broader contract than FDW. PostgreSQL requires
callbacks for scans, tuple insertion, multi-insert, update, delete, row
locking, index fetch, relation storage operations, relation size estimates,
VACUUM, ANALYZE, and index validation/build support.

In PostgreSQL 18.4, the relevant core paths are:

- `src/backend/access/table/tableamapi.c`: validates required
  `TableAmRoutine` callbacks.
- `src/backend/executor/nodeSeqscan.c`: scans normal relations through
  `table_beginscan` and `table_scan_getnextslot`.
- `src/backend/commands/copyfrom.c`: inserts into normal relations through
  `table_multi_insert` or `table_tuple_insert`.
- `src/include/access/tableam.h`: defines the table AM callback contract and
  inline wrappers.

In pgiceberg, the table AM registers `CREATE ACCESS METHOD iceberg TYPE TABLE`
and supports `CREATE TABLE ... USING iceberg`. It stores a PostgreSQL relation
OID to Iceberg catalog/table binding in `pgiceberg.table_bindings`.

The current table AM implementation is intentionally narrow. It calls the
shared engine under `src/engine/` for scans and inserts, and rejects many
native table operations, including `ALTER TABLE`, `UPDATE`, `DELETE`,
`TRUNCATE`, parallel scans, row locking, index tuple fetches, `CREATE INDEX`,
`VACUUM`, and `TABLESAMPLE`.

This path is the right long-term surface if pgiceberg wants Iceberg to behave
like a native PostgreSQL table storage engine. It is also the more natural
place for PostgreSQL-initiated schema evolution, because user DDL can update
`pg_attribute` and then the extension can apply the matching Iceberg schema
change. The cost is that pgiceberg must honor much more of PostgreSQL's table
storage contract.

## Logical Decoding

Logical decoding is not a query surface and not a table storage surface. It
decodes committed WAL changes into an output plugin stream. A consumer can
read that stream through replication protocol or SQL slot functions.

In PostgreSQL 18.4, the relevant core paths are:

- `src/backend/replication/logical/logical.c`: loads `_PG_output_plugin_init`
  and connects reorder buffer callbacks to the output plugin.
- `src/include/replication/output_plugin.h`: defines output plugin callbacks.
- `src/include/replication/reorderbuffer.h`: defines decoded change records
  such as insert, update, delete, and truncate.
- `src/backend/replication/logical/logicalfuncs.c`: backs SQL functions such
  as `pg_logical_slot_peek_changes` and `pg_logical_slot_get_changes`.

In pgiceberg, logical decoding is implemented as an append-only mirror from a
PostgreSQL heap table to an Iceberg table. `pgiceberg.create_logical_mirror`
creates a logical replication slot and, by default, copies the existing source
rows under a write-conflicting handoff lock. A background worker, or
`pgiceberg.process_logical_mirrors()`, peeks slot changes, converts decoded
inserts into tuple slots, appends them to Iceberg, commits Iceberg, and then
advances the slot.

pgiceberg commits Iceberg before advancing the replication slot so it does not
lose changes if the Iceberg commit fails. Each bounded slot prefix has a stable
batch identifier persisted in Iceberg snapshot metadata and table properties.
A crash after the Iceberg commit and before slot advancement can therefore be
recognized and consumed without appending the same rows twice. Current logical
mirrors support only `INSERT`; update, delete, and truncate records are parsed
but disable the mirror. The complete rationale and follow-up architecture are
in [Real-time PostgreSQL-to-Iceberg Mirrors](realtime-iceberg-mirrors.md).

Use this path when PostgreSQL heap is the source of truth and Iceberg is an
asynchronous downstream copy. Do not use it to expose existing Iceberg data to
queries or to make Iceberg a native table storage engine.

## Overlap

FDW and table AM can both return rows to PostgreSQL and can both write rows to
Iceberg. In pgiceberg they share the engine scan and append APIs, not each
other's PostgreSQL callback layers.

FDW and logical decoding can both write Iceberg files and commit Iceberg
metadata. Their transaction models differ: FDW writes are part of the user's
SQL statement and PostgreSQL transaction boundary, while logical decoding
writes happen later from committed WAL.

Table AM and logical decoding can both present a normal PostgreSQL table as
the user-facing object, but they put the source of truth in different places.
With table AM, Iceberg is the table storage. With logical decoding, heap is
the source table and Iceberg is the downstream mirror.

## Source Layout

| Directory | Responsibility | May depend on |
| --- | --- | --- |
| `src/fdw/` | Iceberg FDW planner/executor callbacks | `engine/`, `common/` |
| `src/tableam/` | `TableAmRoutine` and binding catalog | `engine/`, `common/` |
| `src/logical/` | Output plugin, worker, mirror SQL | `engine/`, `common/` |
| `src/engine/` | Shared Iceberg options, scan, DML, xact | `common/`, iceberg-cpp |
| `src/common/` | Catalog, types, Status, PG helpers | iceberg-cpp, PostgreSQL |
| `src/functions/` | SQL helpers (create/register/metadata) | `common/` |
| `src/utilities/` | Read-only Parquet/Avro FDWs | `common/`, shared FDW helpers in `fdw/` |
| `src/copy/` | Reserved; no custom COPY format API yet | — |

Surface modules must not include each other. If a helper is needed by more
than one surface, it belongs in `engine/` or `common/`.

## Schema Evolution

FDW can support schema evolution, but PostgreSQL does not automatically update
a foreign table's local `pg_attribute` rows when the remote schema changes.
The FDW can infer schema during `IMPORT FOREIGN SCHEMA`, map local columns to
remote columns at scan time, and provide a refresh helper, but the local
foreign table definition remains a PostgreSQL catalog object.

For pgiceberg's current FDW:

- Iceberg field ids are stored on foreign columns (`OPTIONS (field_id 'N')`)
  and on native table AM bindings (`pgiceberg.column_bindings`).
- Scans and writes match by field id first, then fall back to column name.
- Added Iceberg columns are invisible until `pgiceberg.refresh_schema` (or a
  local `ALTER`) adds them. Writes fill `write-default` or NULL for optional
  fields that are missing locally.
- Dropped Iceberg columns remain as local NULLs until refresh drops them.
- Renamed Iceberg columns keep working through the stored field id.
- Type changes that PostgreSQL cannot hold raise an error; call
  `pgiceberg.refresh_schema` or `pgiceberg.schema_diff` to inspect and repair.
- `pgiceberg.update_schema` applies Iceberg add/drop/rename for operators and
  tests.

A practical FDW design is to add a schema refresh helper that compares the
current Iceberg schema with the PostgreSQL foreign table definition and either
emits or applies `ALTER FOREIGN TABLE` statements. `pgiceberg.schema_diff` and
`pgiceberg.refresh_schema` implement that helper.

Table AM is more natural for PostgreSQL-initiated schema evolution. The user
can run `ALTER TABLE`, PostgreSQL can update `pg_attribute`, and pgiceberg can
apply the corresponding Iceberg schema update. That direction keeps
PostgreSQL's catalog and Iceberg metadata synchronized through one DDL path.

Table AM does not automatically solve externally initiated Iceberg changes. If
Spark, Flink, or another writer changes the Iceberg schema, pgiceberg still
needs detection and reconciliation before PostgreSQL can safely use the new
shape.

The current pgiceberg table AM does not yet support this flow because it
rejects `ALTER TABLE`. Adding schema evolution through table AM should start
by deciding which PostgreSQL DDL forms are supported, how they map to Iceberg
schema updates, and how to handle external Iceberg metadata changes.

## Design Guidance

Keep the FDW as the primary path for reading and writing existing Iceberg
tables from PostgreSQL. It matches the external table model and has the most
complete implementation today.

Treat the table AM as the native table path. It is the right surface for
`CREATE TABLE ... USING iceberg` and PostgreSQL-driven schema evolution, but
it should not be described as heap-equivalent until more of PostgreSQL's table
contract is implemented.

Treat logical decoding as a CDC sink from PostgreSQL heap into Iceberg. It is
append-only and asynchronous today, with at-least-once delivery.

For schema evolution, separate the direction of change:

- External Iceberg schema changes: prefer FDW refresh/reconcile tooling.
- PostgreSQL DDL changes: prefer table AM support for `ALTER TABLE`.
- Mixed writers: require version/schema checks and a clear conflict policy.
