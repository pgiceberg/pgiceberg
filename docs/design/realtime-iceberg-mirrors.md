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

# Real-time PostgreSQL-to-Iceberg Mirrors

## Status

This document defines the target architecture for logical mirrors and records
the intentionally smaller first implementation. The first implementation
provides a consistent, lock-based initial copy and idempotent replay of INSERT
batches. Full UPDATE/DELETE replication, shared replication streams, and a
sub-second in-memory read path are follow-up work.

## Context

pgiceberg has three separate PostgreSQL integration surfaces:

- the FDW exposes existing external Iceberg tables;
- the table access method makes Iceberg the storage for a PostgreSQL relation;
- logical decoding keeps a downstream Iceberg copy of a PostgreSQL heap table.

Logical mirrors must remain a CDC feature. They must not turn the native table
access method into a marker relation whose queries are executed by another SQL
engine. Keeping the surfaces separate preserves PostgreSQL planning and
transaction semantics and lets the FDW, table AM, and logical worker share the
same iceberg-cpp scan and write primitives.

The logical mirror is implemented directly in pgiceberg's existing C++ stack:
PostgreSQL logical decoding provides the ordered change stream, the pgiceberg
worker owns replication progress, and the shared engine write path uses
iceberg-cpp to publish data files and metadata. The first implementation favors
a small correctness proof over maximum concurrency; later phases can improve
backfill throughput, change coverage, freshness, and maintenance without
changing these ownership boundaries.

## Goals

1. Include rows that existed before a mirror was created without missing or
   duplicating writes at the transition to logical replication.
2. Never acknowledge WAL before the corresponding Iceberg commit is durable.
3. Recognize an Iceberg batch that committed before a crash so replay does not
   append the same rows again.
4. Persist mirror configuration and progress across a PostgreSQL crash.
5. Establish state and metadata contracts that can later support UPDATE,
   DELETE, parallel backfill, and shared replication streams.

## Non-goals of the first implementation

- The initial copy does not run concurrently with source writes. Large tables
  will hold a write-conflicting lock for the duration of the copy.
- Logical mirrors remain INSERT-only. UPDATE, DELETE, and TRUNCATE disable the
  mirror rather than silently producing an incorrect target.
- The worker still owns one logical slot per mirror.
- Iceberg data becomes visible at Iceberg commit boundaries; there is no hot
  Arrow overlay or LSN-bounded union read.
- The implementation does not use Iceberg v3 deletion vectors. The pinned
  iceberg-cpp revision cannot yet load a deletion vector during a scan or merge
  multiple deletion vectors for one data file.

## First implementation

### Persistent control state

`pgiceberg.logical_mirrors` is a regular logged PostgreSQL table. It stores:

- lifecycle state (`backfilling`, `ready`, or `error`);
- the LSN at which the slot was created;
- the number of rows copied during backfill;
- the last consumed LSN and logical batch identifier.

The output plugin omits relations in the `pgiceberg` schema. This prevents
mirror progress updates from feeding the same logical slot while allowing the
configuration to survive crash recovery. Changes for other non-source tables
may still appear in a slot and are consumed without being applied.

Each configured Iceberg target belongs to one logical mirror. The batch
property is a single ordered-stream checkpoint, so concurrent logical mirrors
must not share a target. External writers may append to a mirror target because
Iceberg table properties are preserved, but operators must not remove or alter
the `pgiceberg.logical.*` checkpoint properties.

### Correctness-first initial copy

Mirror creation performs the following sequence in one PostgreSQL transaction:

1. Acquire `SHARE ROW EXCLUSIVE` on the source heap table. This conflicts with
   INSERT, UPDATE, DELETE, and TRUNCATE.
2. Create the logical slot and record its consistent point.
3. Create or validate the target Iceberg table.
4. Under `READ COMMITTED`, acquire a fresh PostgreSQL snapshot after the lock,
   then scan all source rows and append them to the target in bounded batches.
5. Mark the mirror `ready` and commit the pending Iceberg append during
   PostgreSQL `PRE_COMMIT`.
6. Commit PostgreSQL and release the source-table lock.

The lock is the handoff barrier. Every row committed before the lock is visible
to the backfill snapshot. A writer that starts after the lock waits until the
mirror transaction commits, and its change is then newer than the already
created slot. There is therefore neither a missing interval nor an overlap.

`REPEATABLE READ` and `SERIALIZABLE` reuse the transaction snapshot and cannot
provide this post-lock visibility guarantee. Consistent backfill rejects those
isolation levels; callers can create the mirror in a short `READ COMMITTED`
transaction.

If `backfill` is false, mirror creation retains the former from-now behavior.
If the caller supplies an existing target and requests backfill, pgiceberg
requires that target to be empty.

This approach prioritizes a small, auditable correctness proof. A later
implementation should replace the long-held lock with a replication-protocol
`CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT`, parallel readers that import the
same snapshot, and CTID-range sharding. That changes availability and
throughput, not the handoff invariants.

### Idempotent Iceberg batches

The worker continues to peek a bounded prefix from the slot, commit Iceberg,
and consume exactly that prefix. The gap between the external Iceberg commit
and slot consumption cannot be made atomic by PostgreSQL.

For every batch containing source INSERTs, pgiceberg computes a SHA-256 batch
identifier over the slot name and the exact ordered `(LSN, payload)` prefix.
The FastAppend snapshot summary and the resulting table properties contain:

- `pgiceberg.logical.last-batch-id`;
- `pgiceberg.logical.last-source-lsn`.

The property update and append are staged in the same iceberg-cpp transaction.
Before replaying a peeked prefix, the worker loads the target table. If its
last-batch property matches, the Iceberg commit already succeeded and the
worker only consumes the slot prefix and updates PostgreSQL progress. This
makes the normal crash window effectively-once without treating LSN alone as a
batch identity; a single PostgreSQL transaction may span multiple worker-sized
batches that share the same LSN.

The batch identifier is an idempotency key, not an ordering key. WAL is still
consumed in slot order, and the slot is never advanced merely because the
target contains a numerically newer LSN.

## Failure semantics

| Failure point | Result |
| --- | --- |
| Before Iceberg commit | PostgreSQL aborts; the slot prefix remains and newly written files are cleaned up best-effort. |
| Iceberg commit fails | The slot is not advanced. The batch is retried. |
| Iceberg commit state is unknown | New files are retained. A retry consults the target batch property. |
| After Iceberg commit, before slot consumption | The retry recognizes the batch property and consumes without appending. |
| After slot consumption, before PostgreSQL progress commit | Slot semantics decide whether consumption is rolled back; either outcome is safe because the Iceberg batch is identifiable. |
| Initial copy fails | The creating transaction aborts and a newly created slot is dropped best-effort. External Iceberg catalog changes may require operator cleanup. |

## Follow-up phases

### Transaction-aware UPDATE and DELETE

The output plugin should emit BEGIN/COMMIT boundaries and old/new replica
identity values. For mirrors with a primary key or suitable replica identity:

- UPDATE becomes an equality delete of the old key plus an append of the new
  row;
- DELETE becomes an equality delete;
- INSERT remains an append.

Iceberg v2 equality deletes are the appropriate first target because the
pinned reader supports them. Arbitrary FDW and table-AM UPDATE/DELETE should
instead project Iceberg `_file` and `_pos`, write position-delete files, and
commit a `RowDelta`.

### Shared replication streams

One slot per mirrored table causes every slot to decode unrelated database WAL.
The scalable design owns one replication connection and slot per source
database, routes changes by relation OID, and advances only after every target
touched by the source transaction has reached a safe state. Cross-table
Iceberg commits are not atomic, so recovery must retain per-table batch keys
and hold the shared slot at the minimum safe LSN.

### Sub-second union reads

Sub-second visibility should not be implemented by producing an Iceberg
snapshot for every source transaction. A future service can retain committed
Arrow batches and pending deletes, persist a local recovery WAL, wait until a
requested LSN is replicated, and return a pinned read state combining Iceberg
files with hot batches. This is an optional query layer and must not replace
the native FDW or table-AM execution paths.

### Maintenance

Streaming ingestion eventually requires file-size policy, compaction, delete
file consolidation, snapshot expiration, and orphan cleanup. Operational SQL
should expose initial-copy progress, commit and flush LSNs, replication lag,
file counts, and the last error before automatic optimization is added.
