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

# PostgreSQL and Iceberg commit protocol

## Status

This document describes the commit protocol for FDW and table-AM DML that
writes Iceberg tables, the recovery log used when that protocol cannot be
atomic, and the isolation guarantees pgiceberg does **not** provide.

Logical mirrors use the same engine commit path for Iceberg publishes, then
advance a replication slot. Their additional batch-id contract is documented
in [realtime-iceberg-mirrors.md](realtime-iceberg-mirrors.md).

## Why the commit cannot be atomic

Iceberg catalogs are not PostgreSQL storage. pgiceberg can abort a PostgreSQL
transaction after an Iceberg catalog error, but it cannot enlist the Iceberg
catalog in PostgreSQL's two-phase commit. Prepared transactions are rejected
for the same reason: pending Iceberg work is backend-local and cannot be
recovered by `COMMIT PREPARED`.

The engine therefore publishes Iceberg at PostgreSQL `PRE_COMMIT`:

1. Stamp every snapshot-producing update with `pgiceberg.xact.commit-id`.
2. Fsync a recovery log under `$PGDATA/pg_iceberg/xact/` listing every table
   in the PostgreSQL transaction and its base snapshot.
3. Commit Iceberg tables **one at a time**, rewriting the recovery log after
   each success (or `kCommitStateUnknown`).
4. On PostgreSQL `COMMIT`, delete the recovery log.
5. On PostgreSQL `ABORT` after any Iceberg publish, attempt a best-effort
   rollback to each table's base snapshot. If that fails, leave the log as
   `needs_repair`.

`PRE_COMMIT` is still the latest point at which an Iceberg catalog error can
fail the PostgreSQL transaction. That does **not** make the two systems a
single atomic commit.

## Failure cases

| Failure point | PostgreSQL | Iceberg | Recovery |
| --- | --- | --- | --- |
| Before the recovery log is written | Aborts | Unchanged; data files cleaned best-effort | None |
| Iceberg commit fails with a definite error | Aborts | Earlier tables in the same transaction may already be published | Recovery log is `iceberg_partial` / `needs_repair`; abort tries rollback |
| Iceberg commit state unknown | Aborts | May or may not have published | Files are retained; log records `unknown`; reconcile inspects the table |
| Iceberg succeeds, later `PRE_COMMIT` hook fails | Aborts | Published | Abort tries rollback; otherwise `pgiceberg.repair_commit` |
| Iceberg succeeds, PostgreSQL crashes before `COMMIT` | Aborts at recovery | Published | Durable recovery log survives; `reconcile_commits` / `repair_commit` |
| Multi-table publish, table *k* fails after 1..*k-1* succeed | Aborts | Prefix of tables published | Same as partial Iceberg commit |
| PostgreSQL `COMMIT` succeeds | Visible | Published | Recovery log removed |

## Recovery and repair

Durable logs live in `$PGDATA/pg_iceberg/xact/<commit-id>.log`. They are
intentionally **not** PostgreSQL tables: a log that participated in the same
transaction would roll back with it and could not describe an Iceberg publish
that already happened.

SQL:

- `pgiceberg.commit_recovery_log()` lists unfinished logs.
- `pgiceberg.reconcile_commits()` compares each log with the current Iceberg
  snapshot. Verdicts:
  - `stale_intent` — log exists, Iceberg never published or already rolled back
  - `iceberg_partial` — some tables in the log are published, others are not
  - `iceberg_orphan` — Iceberg has the commit and PostgreSQL does not
  - `needs_operator` — current snapshot moved, load failed, or the table had
    no parent snapshot to roll back to
- `pgiceberg.repair_commit(commit_id, 'rollback')` rolls each published table
  back to its recorded base snapshot **only if** that table's current snapshot
  is still the in-doubt commit.
- `pgiceberg.repair_commit(commit_id, 'acknowledge')` keeps the Iceberg data
  and removes the log.
- `pgiceberg.rollback_iceberg_snapshot(catalog, namespace, table, snapshot_id)`
  is the lower-level ancestor rollback used by repair and by operators.

Rollback of a table's **first** snapshot is not supported: Iceberg has no
parent to restore. Those tables must be acknowledged or replaced.

## Isolation guarantees that are not provided

pgiceberg does **not** provide:

1. **Atomic PostgreSQL + Iceberg commit.** A committed Iceberg snapshot can
   exist for an aborted PostgreSQL transaction, and an external Iceberg reader
   can see DML at `PRE_COMMIT` before PostgreSQL `COMMIT`.
2. **Atomic multi-table Iceberg commit.** Tables are published sequentially.
   A failure can leave a prefix of the tables visible in Iceberg.
3. **Serializable (or repeatable-read) isolation across PostgreSQL heap and
   Iceberg.** `SERIALIZABLE` / `REPEATABLE READ` apply to PostgreSQL relations
   only. Iceberg catalog commits, snapshot visibility, and concurrent writers
   in other engines are outside that snapshot.
4. **A single isolation level for mixed FDW / table-AM / heap statements.**
   Read-your-writes inside one PostgreSQL transaction is implemented with an
   in-memory Iceberg `StaticTable` for pending work. That is not MVCC, not
   predicate locking, and not visible to other PostgreSQL backends until
   Iceberg `PRE_COMMIT` publish.
5. **Cross-engine serializability.** Concurrent Spark, Flink, or other
   Iceberg writers can commit between pgiceberg's base snapshot and
   `PRE_COMMIT`. iceberg-cpp conflict checks apply per table update; they do
   not span the PostgreSQL transaction.
6. **Prepared transactions (`PREPARE TRANSACTION`).** Rejected for pgiceberg
   DML.
7. **Automatic cluster-wide repair after crash.** Abort-time rollback is
   best-effort in the same backend. After a crash, operators must run
   `reconcile_commits` and `repair_commit`.

What *is* provided:

- Iceberg catalog errors at `PRE_COMMIT` still abort PostgreSQL.
- Pending Iceberg metadata is discarded on PostgreSQL abort that happens
  **before** any catalog publish, with best-effort data-file cleanup.
- Each published snapshot carries `pgiceberg.xact.commit-id` so an in-doubt
  commit is recognizable without the recovery log.
- Same-transaction Iceberg reads see earlier writes in that backend.

## Commit identifier

`pgiceberg.xact.commit-id` is a 128-bit hex identifier generated once per
PostgreSQL transaction that performs Iceberg DML. It is written to:

- the snapshot summary of every snapshot-producing update in that transaction
- table properties for the same keys (so `ReadTableProperty` can observe it)
- the durable recovery log

`pgiceberg.xact.postgres-xid` stores the full PostgreSQL transaction id for
diagnostics. It is not used as the recovery key because transaction ids are
reused after wraparound and are not meaningful after clog truncation.
