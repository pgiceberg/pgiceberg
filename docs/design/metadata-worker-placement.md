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

# Metadata worker placement

Decision: one instance launcher and one resident worker per explicitly enabled
database, as implemented in the [worker foundation](../metadata-workers.md).
This fits pgiceberg's configuration and recovery model; PostgreSQL does not
require metadata services to use this layout.

This rationale supplements the [architecture](metadata-backend.md). Source review
was performed on 2026-09-28 against PostgreSQL `REL_18_STABLE` and the extension
repositories' then-current `main` branches. Those links move and do not describe
every historical release.

## Alternatives

A per-database worker owns its catalog clients, caches, private transactions, and
publication queue. The launcher manages lifecycle and instance admission; clients
contact their database worker directly. Different workers may still access the
same external Iceberg table.

An instance-wide service handles multiple databases and separates state by
database, authorization, transaction, and external table identity. It may run
without a database connection or use a management database, plus libpq or resolver
workers for local configuration. It can perform concurrent I/O without routing
all data files through one process.

## Existing process models

| Example | Observed ownership | Relevant lesson |
| --- | --- | --- |
| PostgreSQL [background writer](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/postmaster/bgwriter.c) | Instance process calls `BgBufferSync` for shared dirty buffers. | Instance scope fits an already shared resource. |
| PostgreSQL [autovacuum](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/postmaster/autovacuum.c) | Launcher chooses a database; workers connect, vacuum, and exit. | Global scheduling can coexist with database-local execution. |
| PostgreSQL [logical replication](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/launcher.c) | Launcher starts apply workers per subscription, with tablesync/parallel workers as needed. | Subscription ownership determines workers; it is not one worker per database. |
| [Autoprewarm](https://github.com/postgres/postgres/blob/REL_18_STABLE/contrib/pg_prewarm/autoprewarm.c) | Leader groups work by database and starts database-connected workers in turn. | Database-local work need not imply permanently resident executors. |
| TimescaleDB [launcher](https://github.com/timescale/timescaledb/blob/main/src/loader/bgw_launcher.c) / [scheduler](https://github.com/timescale/timescaledb/blob/main/src/bgw/scheduler.c) | Cluster launcher manages database schedulers; schedulers check extension versions and start jobs. | Database-local configuration favors database-local lifecycle management. |
| Citus [maintenance daemon](https://github.com/citusdata/citus/blob/main/src/backend/distributed/utils/maintenanced.c) | Database-OID registry; backends may start their database daemon, with a configured MainDb startup path too. | Reuse claiming and deduplication ideas without assuming a mandatory global launcher. |
| [pg_cron](https://github.com/citusdata/pg_cron/blob/main/README.md) / [execution](https://github.com/citusdata/pg_cron/blob/main/src/pg_cron.c) | Scheduler connects to one management database; jobs use libpq or background workers for other databases. | Central scheduling works when persistent configuration is centralized. |

Core auxiliary processes do not all use the extension `RegisterBackgroundWorker`
API, so their private startup mechanisms and slot budgets cannot be copied
literally. The logical launcher can discover subscriptions without a user-database
connection because [`pg_subscription`](https://www.postgresql.org/docs/18/catalog-pg-subscription.html)
is shared. Ordinary extension configuration tables are not.

These are scheduling/maintenance examples, not evidence that every query should
RPC to a metadata process. Citus, for example, maintains
[backend-local metadata caches](https://github.com/citusdata/citus/blob/main/src/backend/distributed/metadata/metadata_cache.c).
They also establish no performance advantage for either pgiceberg layout.

## PostgreSQL boundaries

A worker initializes its internal database connection once; it cannot switch
that SPI context between databases. It can instead remain unbound, access shared
catalogs, or open libpq connections. This limits one PostgreSQL session, not the
number of databases a service can support. See the
[background worker API](https://www.postgresql.org/docs/18/bgworker.html).

Per-database workers separate normal queues, errors, and quotas. They do not
provide crash isolation: an abnormal shared-memory worker exit may trigger
instance-wide recovery through the postmaster's
[child-crash handling](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/postmaster/postmaster.c).

## Tradeoffs for pgiceberg

The following are design judgments, not benchmark results.

| Concern | Per-database workers | Instance-wide service |
| --- | --- | --- |
| Committed configuration and recovery | Read existing local extension tables. | Need centralized configuration or cross-database resolution that also works after restart. |
| Uncommitted caller configuration | Backend resolves under its snapshot. | Same requirement; a separate service cannot join the caller's transaction. |
| Cache and connection reuse | May duplicate work for the same external table. | Can share across compatible authorization scopes. |
| Publication queues | Serialize locally; catalog resolves cross-database conflicts. | Can serialize an external table across the instance. |
| External writers | Catalog conditional commits remain mandatory. | Same; one process cannot constrain Spark or Flink. |
| Baseline resources | About D workers plus one launcher for D enabled databases; require instance-wide limits. | Fewer service processes; resolver connections still cost backends, and tenants need fair scheduling. |
| Extension/database lifecycle | Follows local installation state. | Must isolate each database's protocol, identity, and pending work. |
| Shared Arrow data | Possible within a database. | Possible across databases with authorization and complete cache keys. Neither layout provides zero-copy automatically. |

`pgiceberg.catalogs`, `table_bindings`, and `column_bindings` are database-local
[extension tables](../../sql/pgiceberg--0.1.0.sql), read by
[`LoadCatalogOptions`](../../src/common/catalog.cc) through caller SPI. Per-database
workers preserve this ownership and can recover committed bindings independently.
They still cannot see uncommitted caller rows or wait for configuration locks held
by a caller waiting for PRE_COMMIT.

An external table UUID can be shared by several PostgreSQL databases. Duplicate
caches and publication queues are therefore real costs. Both layouts need actual
catalog/tenant and table identity rather than local names/OIDs, and must keep
incompatible authorization scopes separate.

## Chosen lifecycle

The launcher handles explicit enablement, start/stop, backoff, and instance-wide
worker/memory/I/O admission. It never forwards normal requests. Dynamic workers
use `BGW_NEVER_RESTART` so only the launcher schedules replacements; the postmaster
may restart the static launcher.

Workers claim shared registry slots by database, installation identity, and
generation. A restarted launcher reconciles healthy workers from shared state,
not lost private handles. Keep enabled workers resident initially. Future idle
exit must wait for read handles, private transactions, pending publications, and
recovery work to finish, then define a safe wakeup path.

TimescaleDB's launcher restart terminates and recreates old schedulers. pgiceberg
cannot copy that policy blindly: its workers may own private user transactions.
Prefer adopting healthy workers. Forced termination must fail affected
transactions explicitly and retain publication journals.

Keep client/protocol APIs independent of placement. Reconsider a centralized
service if many databases share external tables, bindings and credentials are
centrally recoverable, and shared caches justify tenant scheduling and lifecycle
complexity. That is a different state-ownership design, not just fewer processes.

## Validation

Compare one database, many databases with distinct tables, many with the same
table, and one hot database alongside lightly loaded ones. Measure cold/warm RPC
wait, catalog calls, duplicated parse bytes, connection pools, private/shared
memory, and publication conflicts. Test recovery with no connected users and
installation upgrade/drop effects on other databases.

Measure shared-data caching separately. Scheduler precedents cannot replace
pgiceberg-specific performance evidence.
