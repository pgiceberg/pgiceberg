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

# PostgreSQL extension surfaces

pgiceberg has three independent PostgreSQL entry points that share Iceberg
read/write primitives. Choose the surface by who owns the data:

| Surface | Data ownership and purpose | Current support | Entry point |
| --- | --- | --- | --- |
| FDW | Map existing external Iceberg tables into SQL. | Scans, import, and DML; most complete path. | `FOREIGN DATA WRAPPER pgiceberg` |
| Table AM | Use Iceberg as storage for a PostgreSQL relation. | Scans and appends; broader native operations remain unsupported. | `ACCESS METHOD iceberg` |
| Logical decoding | Copy committed PostgreSQL heap changes to Iceberg asynchronously. | Consistent initial copy and INSERT-only CDC. | Output plugin and mirror worker |

Each surface has its own callbacks and control state. Foreign tables use
server/table options; Table AM uses `pgiceberg.table_bindings`; mirrors use
`pgiceberg.logical_mirrors`. They ship in one shared library and one extension,
but do not call each other's surface modules.

```text
  fdw/ ────────┐
  tableam/ ────┼──► engine/ ──► common/ ──► iceberg-cpp
  logical/ ────┘
```

Shared engine code handles options, scans, writes, pending commits, and recovery.
Common code handles catalogs, types, schema binding, and PostgreSQL error
boundaries. A helper needed by several surfaces belongs in these lower layers.

## FDW

An `FdwRoutine` supplies planner, scan, and modify callbacks such as
`GetForeignRelSize`, `BeginForeignScan`, `IterateForeignScan`, and
`ExecForeignInsert/Update/Delete`. The local foreign relation maps to an external
catalog table; PostgreSQL heap indexes, MVCC, VACUUM, and physical storage are
outside the FDW's ownership.

`src/fdw/` implements Iceberg scans, `IMPORT FOREIGN SCHEMA`, and DML through the
shared engine. `src/utilities/` provides separate read-only Parquet/Avro file
wrappers. Use FDW for querying and modifying existing external tables.

## Table access method

A normal relation stores its schema in `pg_class`/`pg_attribute` and uses
`TableAmRoutine` through `rel->rd_tableam`. This contract covers scans, writes,
locking, index access/builds, storage, size estimates, VACUUM, and ANALYZE.

`CREATE TABLE ... USING iceberg` binds a relation OID to an Iceberg table and
uses the shared engine for scans and inserts. It currently rejects `ALTER TABLE`,
`UPDATE`, `DELETE`, `TRUNCATE`, parallel scans, row locking, index tuple fetches,
`CREATE INDEX`, `VACUUM`, and `TABLESAMPLE`.

Table AM is the native-storage path and the natural place for future
PostgreSQL-driven schema evolution. Supporting that role requires implementing
the corresponding storage and DDL contracts; current support is not heap-equivalent.

## Logical decoding

Logical decoding turns committed WAL into an output-plugin stream. It does not
serve queries or replace table storage. In pgiceberg,
`pgiceberg.create_logical_mirror` creates a slot and, by default, copies existing
heap rows under a write-conflicting handoff lock. A worker, or
`pgiceberg.process_logical_mirrors()`, then peeks changes, appends INSERTs, commits
Iceberg, and advances the slot.

A stable batch ID in snapshot metadata and table properties recognizes a commit
whose slot advancement was interrupted. UPDATE, DELETE, and TRUNCATE records
disable the current mirror. See [logical mirror design](realtime-iceberg-mirrors.md)
for the handoff proof, replay contract, and follow-up phases.

FDW/Table AM writes occur within the user's transaction; mirror writes consume
WAL later. Both use the engine, but neither PostgreSQL/Iceberg nor multi-table
Iceberg publication is atomic. See the
[commit protocol](postgres-iceberg-commit.md) for visibility and repair limits.

## PostgreSQL call paths

Relevant core locations, as reviewed in PostgreSQL 18.4:

| Surface | Core code |
| --- | --- |
| FDW planning | `optimizer/path/allpaths.c`: `GetForeignRelSize`. |
| FDW execution | `executor/nodeForeignscan.c`, `executor/nodeModifyTable.c`: scan and modify callbacks. |
| Foreign schema import | `commands/foreigncmds.c`: `ImportForeignSchema`. |
| Table AM registration | `access/table/tableamapi.c`, `access/tableam.h`: required callbacks and wrappers. |
| Table scans and COPY | `executor/nodeSeqscan.c`, `commands/copyfrom.c`: table scan and insert APIs. |
| Logical decoding | `replication/logical/logical.c`, `replication/output_plugin.h`, `replication/reorderbuffer.h`: plugin callbacks and decoded records. |
| SQL slot consumers | `replication/logical/logicalfuncs.c`: peek/get functions. |

Paths are under `src/backend/`, except headers under `src/include/`.

## Schema evolution

External Iceberg changes do not automatically update PostgreSQL `pg_attribute`.
For current FDW bindings:

- Store Iceberg field IDs in foreign-column options and native
  `pgiceberg.column_bindings`; match by ID before falling back to name.
- Added remote columns stay invisible until refresh or local ALTER. Writes
  supply `write-default` or NULL for missing optional fields.
- Dropped remote columns read as NULL until refresh removes local columns.
- Renames continue working through field IDs; incompatible type changes fail.
- `pgiceberg.schema_diff` inspects differences and `pgiceberg.refresh_schema`
  reconciles the local definition. `pgiceberg.update_schema` applies remote
  add/drop/rename operations.

Future Table AM schema evolution should define supported PostgreSQL DDL,
Iceberg mappings, and conflict handling before enabling `ALTER TABLE`.
PostgreSQL-initiated changes can then update both catalogs through that path.
External writers still require detection and reconciliation in either surface.

## Source boundaries

| Directory | Responsibility | Dependencies |
| --- | --- | --- |
| `src/fdw/` | Iceberg planner/executor callbacks. | `engine/`, `common/` |
| `src/tableam/` | Table AM callbacks and binding catalog. | `engine/`, `common/` |
| `src/logical/` | Output plugin, worker, mirror SQL. | `engine/`, `common/` |
| `src/engine/` | Shared scans, DML, transactions, recovery. | `common/`, iceberg-cpp |
| `src/common/` | Catalogs, types, status and PostgreSQL helpers. | iceberg-cpp, PostgreSQL |
| `src/functions/` | SQL management helpers. | `common/`, `engine/` |
| `src/utilities/` | Read-only file FDWs. | `common/`, shared FDW helpers |
| `src/copy/` | Reserved; no custom COPY format API yet. | — |

Keep surface modules independent. Use FDW for external mappings, Table AM for
native Iceberg storage, and logical decoding for heap-to-Iceberg CDC.
