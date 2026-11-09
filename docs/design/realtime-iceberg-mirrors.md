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

# Real-time PostgreSQL-to-Iceberg mirrors

The first implementation provides a lock-based consistent initial copy and
idempotent replay of INSERT batches. UPDATE/DELETE replication, shared streams,
and sub-second in-memory reads are future work.

Heap remains the source of truth. Logical decoding supplies ordered changes;
pgiceberg owns replication progress and uses the shared iceberg-cpp engine to
write files and publish metadata. This CDC path stays separate from FDW and
Table AM query/storage callbacks.

## Current scope

- Copy pre-existing rows without gaps or duplicates at the replication handoff.
- Publish Iceberg durably before acknowledging source WAL.
- Recognize a batch published before a crash and avoid appending it twice.
- Persist configuration and progress across PostgreSQL crashes.

The initial copy blocks source writes for its duration. Each mirror owns one
logical slot. UPDATE, DELETE, and TRUNCATE disable the mirror instead of silently
leaving an incorrect target. Visibility follows Iceberg commit boundaries; there
is no hot Arrow overlay or LSN-bounded union read. INSERT-only mirrors produce
no v3 deletion vectors, although FDW readers/writers support that separate path.

## Persistent state

The logged `pgiceberg.logical_mirrors` table stores lifecycle state
(`backfilling`, `ready`, `error`), slot creation LSN, copied-row count, last
consumed LSN, and batch ID. The output plugin excludes the `pgiceberg` schema
so progress writes do not feed the same slot. Other non-source changes may be
decoded and consumed without applying them.

Each Iceberg target belongs to one mirror: its batch property checkpoints one
ordered stream. External appends are allowed if writers preserve table
properties, including all `pgiceberg.logical.*` checkpoints.

## Consistent initial copy

Mirror creation runs in one PostgreSQL transaction:

1. Acquire `SHARE ROW EXCLUSIVE` on the heap source, blocking INSERT, UPDATE,
   DELETE, and TRUNCATE.
2. Create the logical slot and record its consistent point.
3. Create or validate the target Iceberg table.
4. Under `READ COMMITTED`, take a fresh snapshot after acquiring the lock and
   append source rows in bounded batches.
5. Mark the mirror ready and publish the pending append at PRE_COMMIT.
6. Commit PostgreSQL and release the source lock.

The lock is the handoff barrier. Pre-lock committed rows appear in the copy.
Blocked writes resume after mirror creation commits and appear after the slot's
consistent point. There is no gap or overlap.

Reject `REPEATABLE READ` and `SERIALIZABLE` for consistent backfill: their reused
transaction snapshot may miss rows committed before the lock. Use a short
`READ COMMITTED` transaction. With `backfill=false`, preserve from-now behavior.
An existing target must be empty when backfill is requested.

A later concurrent backfill can use replication-protocol
`CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT`, imported snapshots, and parallel
CTID-range readers. It must preserve the same handoff invariants.

## Idempotent batches

The worker peeks a bounded slot prefix, publishes Iceberg, then consumes exactly
that prefix. PostgreSQL cannot atomically combine external publication and slot
consumption.

For a prefix containing source INSERTs, compute a SHA-256 ID over the slot name
and exact ordered `(LSN, payload)` prefix. Stage append plus these properties in
one iceberg-cpp transaction, also recording them in the FastAppend summary:

- `pgiceberg.logical.last-batch-id`
- `pgiceberg.logical.last-source-lsn`

Before replay, load the target. A matching last-batch property means publication
already succeeded: consume the prefix and update progress without appending.
This handles the normal publish-before-consume crash window. LSN alone is not a
batch identity because several bounded batches from one PostgreSQL transaction
may share it.

The ID proves identity, not order. Consume WAL in slot order; a numerically newer
target LSN never authorizes skipping a source prefix.

## Failure behavior

| Failure point | Result |
| --- | --- |
| Before Iceberg publication | PostgreSQL aborts; slot prefix remains; clean new files best-effort. |
| Definite Iceberg failure | Keep the slot position and retry. |
| Unknown Iceberg outcome | Retain files and inspect the target checkpoint on retry. |
| Iceberg published, prefix not consumed | Match the batch property and consume without reappending. |
| Prefix consumed, local progress not committed | Slot rollback/durability determines replay; batch identity protects an already published prefix. |
| Initial copy fails | Abort creation and drop the new slot best-effort; external catalog changes may need operator cleanup. |

## Follow-up work

### UPDATE and DELETE

Emit transaction boundaries and old/new replica identity. For a primary key or
suitable replica identity, represent UPDATE as old-key equality delete plus new
row append, DELETE as equality delete, and INSERT as append. Iceberg v2 equality
deletes are the initial target because the pinned reader supports them.

FDW row-level writes instead identify rows through `_file` and `_pos`, using v3
deletion vectors with `RowDelta` or data-file rewrites for v2 tables.

### Shared replication streams

One slot per table repeatedly decodes unrelated WAL. A shared connection/slot
per source database can route by relation OID, advancing only when every target
in a source transaction is safe. Because cross-table Iceberg publication is not
atomic, keep per-table batch evidence and hold the slot at the minimum safe LSN.

### Sub-second reads

Avoid one Iceberg snapshot per source transaction. A future query layer can keep
committed Arrow batches and pending deletes, persist a local recovery WAL, wait
for a requested LSN, and pin a combined Iceberg/hot-batch view. This optional
layer must preserve native FDW and Table AM execution.

### Maintenance and observability

Define file sizing, compaction, delete consolidation, snapshot expiration, and
orphan cleanup. Before automatic optimization, expose copy progress, commit/flush
LSNs, lag, file counts, and the last error.
