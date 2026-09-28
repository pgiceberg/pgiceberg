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

# Metadata worker foundation

This is phase 1 of the [metadata backend design](design/metadata-backend.md):
an instance launcher, one worker per explicitly enabled database, and a bounded
request/reply transport. The SQL functions below exercise that transport.
Existing FDW, table AM, logical mirror, and metadata helper operations still
execute through their existing paths. Metadata caching, scan planning, Iceberg
transactions, and publication recovery have not moved into these workers.

## Enable

Install the extension library, then configure `postgresql.conf`:

```conf
shared_preload_libraries = 'pgiceberg'  # retain other required libraries
pgiceberg.metadata_databases = 'analytics, reporting'
pgiceberg.metadata_max_workers = 8
pgiceberg.metadata_max_connections = 64
# Allow room in max_worker_processes for the launcher, enabled database workers,
# the existing logical sync worker, parallel queries, and other extensions.
```

Restart PostgreSQL, then run `CREATE EXTENSION pgiceberg` separately in each
listed database. Preloading also starts pgiceberg's existing logical sync worker;
its `logical_sync_database` and `logical_sync_user` settings still apply.
Preloading alone does not install SQL objects in any database.

`metadata_databases` is an explicit list of database identifiers, with SQL-style
quoting for names that need it. The default is empty. Change it with
`ALTER SYSTEM SET pgiceberg.metadata_databases = 'analytics, reporting'`, followed
by `SELECT pg_reload_conf()`. The launcher resolves names against the shared
`pg_database` catalog and skips nonexistent databases, templates, and databases
that disallow connections. It connects to no database itself.

| Setting | Default | Range | Applies |
| --- | --- | --- | --- |
| `pgiceberg.metadata_databases` | empty | at most `metadata_max_workers` names | reload |
| `pgiceberg.metadata_max_workers` | 8 | 1–32 | restart |
| `pgiceberg.metadata_max_connections` | 64 | 1–128, instance-wide | restart |
| `pgiceberg.metadata_request_timeout_ms` | 5000 | 1–600000 ms | session |

In each enabled database, the extension owner can run:

```sql
SELECT * FROM pgiceberg.metadata_worker_ping();
-- database_oid | worker_pid | generation | protocol_version

SELECT pgiceberg.metadata_worker_echo(decode('00ff7e81', 'hex'));
```

These functions are revoked from `PUBLIC`; an administrator can explicitly grant
execution for diagnostics. `echo` accepts at most 65,536 payload bytes, including
binary zeroes. A request has a fresh DSM segment with two 16 KiB `shm_mq` queues.
Larger messages stream through the queues. Transport uses fixed-width,
big-endian headers with a protocol version, database OID, worker generation,
request ID, and checked payload length; no C++ pointers or objects cross it.
A worker responds only to ping and echo. It does not execute client SQL or access
an Iceberg catalog, even though its database connection uses the bootstrap
superuser. Authorization for future metadata operations remains separate work.

## Lifecycle and failure behavior

The launcher owns restart decisions; dynamic workers use `BGW_NEVER_RESTART`.
An incrementing generation fences registrations and requests from earlier worker
incarnations. Graceful worker termination restarts that worker. An abnormal exit
such as SIGKILL or a segmentation fault invokes PostgreSQL's instance-wide crash
recovery, as for other shared-memory background workers. A launcher restart adopts live children instead of duplicating
them. Generations are scoped to one postmaster lifetime.

A worker checks for its database's extension before accepting requests and
rechecks at most every 250 ms. `DROP EXTENSION` stops that database's worker;
other databases keep their own workers and SQL objects. Recreating the extension
starts a fresh worker. Catalog checks see committed state, so rolling back a
`DROP EXTENSION` does not stop the service. An extension transaction hook wakes
processes after commit/abort, with a 30-second fallback probe for databases
without the extension (including missed/prepared-transaction notifications).
The fallback briefly creates a worker to inspect the database-local catalog.

Remove a database from the configured list and reload before `DROP DATABASE`.
Wait for its worker to leave `pg_stat_activity`; a connected worker otherwise
counts as an active connection. Removing a database stops its worker without
dropping the extension or any local tables.

An unavailable worker produces a bounded startup timeout; an unconfigured
database produces a configuration error. A full connection registry fails
immediately with a retry hint. Worker exit, request timeout, statement
cancellation, and client backend exit release request resources. A disconnected
request fails; it is never silently replayed to a new worker. The worker processes
queues without waiting for an individual sender or receiver, so a stalled client
does not block other clients.

No new persistent metadata tables are introduced in this phase. The shared
registry and request queues are transient. Existing extension configuration is
per database, and external Iceberg metadata remains in its catalog/warehouse.
Ordinary extension paths continue to work without preloading; these diagnostic
RPCs report an explicit preload error if it is missing.

## Tests

`ctest --test-dir build --output-on-failure` includes a wire-format test and an
isolated-cluster integration test (`metadata_worker`). The latter covers two
databases, concurrent and maximum-size binary requests, SQL permissions,
backpressure, timeout/cancel/disconnect cleanup, worker and launcher restart,
transactional extension lifecycle, reload, and postmaster restart. Its server
log is saved to `build/test/metadata_worker.log`.

PostgreSQL 18 uses the build-tree extension control file. PostgreSQL 16/17 tests
use the installed extension, so run `cmake --install build --component pgiceberg`
first, as for the existing regression tests. Existing installations created with
an older checkout's `0.1.0` SQL do not automatically acquire the new diagnostics;
use a fresh test database for this development version.
