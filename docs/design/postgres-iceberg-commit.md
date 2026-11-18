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

This is the current FDW/Table AM DML protocol. Iceberg catalogs do not participate
in PostgreSQL two-phase commit. Publication therefore happens at `PRE_COMMIT`,
when an Iceberg error can still abort PostgreSQL, but the two commits are not atomic.
Prepared transactions (`PREPARE TRANSACTION`) are rejected because pending
Iceberg work is backend-local and cannot be recovered by `COMMIT PREPARED`.

Logical mirrors publish through the engine before advancing their slot; their
additional [batch checkpoint contract](realtime-iceberg-mirrors.md) handles replay.
The proposed worker-owned replacement is in
[metadata transactions](metadata-transactions.md).

## Publication sequence

1. Stamp snapshot-producing updates with `pgiceberg.xact.commit-id`.
2. Fsync a recovery log under `$PGDATA/pg_iceberg/xact/` containing every affected
   table and its base snapshot.
3. Publish tables sequentially, rewriting the log after each success or
   `kCommitStateUnknown`.
4. On PostgreSQL COMMIT, remove the log.
5. On PostgreSQL ABORT after publication, attempt best-effort rollback to each
   base snapshot only while the current snapshot is still the in-doubt commit.
   Retain `needs_repair` if the check or rollback fails.

External readers may see Iceberg changes before PostgreSQL commits. A later
PostgreSQL failure can leave published snapshots, and a multi-table failure can
leave a published prefix.

## Failure cases

| Failure point | Result and recovery |
| --- | --- |
| Before the recovery log | PostgreSQL aborts; catalog unchanged; clean data files best-effort. |
| Definite catalog error | PostgreSQL aborts; earlier tables may already be published. Retain `iceberg_partial` / `needs_repair`; abort attempts rollback. |
| Unknown catalog result | PostgreSQL aborts; keep files and an `unknown` record for reconciliation. |
| Later PRE_COMMIT hook fails after publication | PostgreSQL aborts; attempt rollback or use `repair_commit`. |
| Crash after PostgreSQL WAL COMMIT, before log removal | Both systems committed; reconciliation recognizes the stored xid and removes the leftover log. |
| Normal PostgreSQL COMMIT | Both published; remove the recovery log. |

## Recovery tools

Logs live at `$PGDATA/pg_iceberg/xact/<commit-id>.log`, outside transactional
PostgreSQL tables so that abort cannot erase evidence of external publication.

| SQL function | Purpose |
| --- | --- |
| `pgiceberg.commit_recovery_log()` | List unfinished logs. |
| `pgiceberg.reconcile_commits()` | Check the originating xid, then compare unresolved records with Iceberg. |
| `pgiceberg.repair_commit(id, 'rollback')` | Inspect every table, including pending rows, and restore the recorded base only if its current snapshot still identifies the in-doubt commit. Refuse while PostgreSQL is in progress or committed. |
| `pgiceberg.repair_commit(id, 'acknowledge')` | Keep Iceberg data and remove the record. |
| `pgiceberg.rollback_iceberg_snapshot(catalog, namespace, table, snapshot_id)` | Explicit ancestor rollback used by repair and operators. |

Reconciliation verdicts:

| Verdict | Meaning |
| --- | --- |
| `in_progress` | Originating PostgreSQL transaction is still running. |
| `postgres_committed` | Remove the leftover log without rollback. |
| `stale_intent` | Iceberg never published or already rolled back. |
| `iceberg_partial` | Only some tables published. |
| `iceberg_orphan` | Iceberg published and PostgreSQL aborted. |
| `needs_operator` | Current snapshot moved, table load failed, or no parent snapshot exists. |

The first snapshot has no parent to restore; acknowledge or replace the table.
After a crash, operators run reconciliation and repair. Abort-time rollback is
best-effort within the original backend, not automatic cluster-wide recovery.

## Visibility and isolation limits

- PostgreSQL and Iceberg, and separate Iceberg tables, do not commit atomically.
- PostgreSQL `REPEATABLE READ`/`SERIALIZABLE` do not cover Iceberg catalog state,
  external writers, or a common heap/Iceberg snapshot. Per-update iceberg-cpp
  conflict checks do not provide cross-engine serializability.
- Same-transaction reads use a private in-memory `StaticTable` to see earlier
  writes. Other backends see them only after publication. This provides no
  PostgreSQL MVCC or predicate locking for Iceberg.
- Errors at PRE_COMMIT abort PostgreSQL. Before any publication, abort discards
  pending metadata and cleans new files best-effort. After publication, retain
  evidence and repair conservatively.

## Commit identity

Generate one random 128-bit hex `pgiceberg.xact.commit-id` per PostgreSQL
transaction performing Iceberg DML. Store it in snapshot summaries, matching
table properties, and the recovery log so publications remain recognizable.

`pgiceberg.xact.postgres-xid` records the full PostgreSQL transaction ID for
in-progress/committed checks; the commit ID remains the recovery key. Unknown
status after commit-log truncation or xid wraparound requires an operator rather
than automatic rollback.
