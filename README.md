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

# pgiceberg

pgiceberg is a PostgreSQL extension for working with Apache Iceberg tables.

It provides three independent PostgreSQL surfaces on top of apache/iceberg-cpp:
a Foreign Data Wrapper, a native table access method, and logical-decoding
mirrors. Iceberg catalog access, metadata, manifests, manifest lists, deletes,
and Parquet data files are handled through iceberg-cpp. The surfaces share a
common Iceberg engine; they do not call into each other. See
[PostgreSQL Extension Surfaces](docs/design/postgres-extension-surfaces.md)
for when to use each path.

## Features

- Create Iceberg tables from PostgreSQL SQL helper functions.
- Map Iceberg tables into PostgreSQL as foreign tables (FDW).
- Infer foreign table definitions with `IMPORT FOREIGN SCHEMA`.
- Create native Iceberg-backed tables with `CREATE TABLE ... USING iceberg`.
- Mirror PostgreSQL heap inserts into Iceberg with logical decoding.
- Register existing Iceberg table metadata into a SQL catalog.
- Read table metadata and snapshot file summaries from SQL.
- Read a historical Iceberg snapshot with the `snapshot_id` foreign-table option.
- Append rows with `INSERT`.
- Apply `UPDATE` and `DELETE` to unpartitioned foreign tables, using deletion
  vectors for Iceberg v3 and data-file rewrites for Iceberg v2.

## Install

Build pgiceberg against the PostgreSQL installation that will load the
extension. The vendored iceberg-cpp dependency requires a C++23 compiler such
as GCC 14 or newer.

```sh
BUILD_DIR=/tmp/pgiceberg-build/pg18.4
cmake -S . -B "$BUILD_DIR" -GNinja -DPG_CONFIG="$(command -v pg_config)"
cmake --build "$BUILD_DIR"
cmake --install "$BUILD_DIR" --component pgiceberg
```

For pgenv, local regression tests, hooks, sanitizer builds, and Dev Container
setup, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Basic Usage

Start PostgreSQL, create a database, and enable the extension:

```sh
createdb -U postgres pgiceberg_dev
psql -U postgres -d pgiceberg_dev
```

```sql
CREATE EXTENSION pgiceberg;
```

Register a local SQLite-backed Iceberg catalog:

```sql
SELECT pgiceberg.add_catalog(
  'dev',
  'sqlite',
  '/tmp/pgiceberg_catalog_dev.db',
  '/tmp/pgiceberg_warehouse'
);
```

The first argument is the local catalog name used by pgiceberg helper
functions. pgiceberg uses the same value as the logical Iceberg catalog name
unless an explicit `iceberg_catalog_name` fifth argument is provided.

Create an Iceberg table:

```sql
SELECT pgiceberg.create_table(
  'dev',
  'default',
  'trip_fixture',
  ARRAY['vendorid', 'passenger_count', 'trip_distance', 'store_and_fwd_flag'],
  ARRAY[
    'bigint'::regtype,
    'bigint'::regtype,
    'double precision'::regtype,
    'text'::regtype
  ],
  ARRAY[true, false, false, false],
  true
);
```

Create a PostgreSQL foreign server for that catalog:

```sql
CREATE SERVER iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog 'dev');
```

Map the Iceberg table into PostgreSQL:

```sql
CREATE FOREIGN TABLE pgiceberg_trip_fixture (
  vendorid bigint,
  passenger_count bigint,
  trip_distance double precision,
  store_and_fwd_flag text
)
SERVER iceberg
OPTIONS (
  namespace 'default',
  table 'trip_fixture'
);
```

To read a historical Iceberg snapshot through iceberg-cpp
`TableScanBuilder::UseSnapshot`, add a `snapshot_id` foreign-table option:

```sql
ALTER FOREIGN TABLE pgiceberg_trip_fixture
OPTIONS (ADD snapshot_id '1234567890');
```

DML is rejected while `snapshot_id` is set. Drop the option to return to the
current table snapshot before `INSERT`, `UPDATE`, or `DELETE`.

Read and write through the foreign table:

```sql
INSERT INTO pgiceberg_trip_fixture
VALUES (1, 2, 3.5, 'N');

SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM pgiceberg_trip_fixture
ORDER BY vendorid, trip_distance;

UPDATE pgiceberg_trip_fixture
SET passenger_count = 3
WHERE vendorid = 1;

DELETE FROM pgiceberg_trip_fixture
WHERE vendorid = 1;
```

Iceberg data files are Parquet, but pgiceberg does not read a Parquet path
directly. It loads the Iceberg table through iceberg-cpp so table metadata,
snapshots, manifests, and deletes are respected.

## Import Tables

Use `IMPORT FOREIGN SCHEMA` when the Iceberg table already exists and you want
PostgreSQL to infer the foreign table columns:

```sql
CREATE SCHEMA imported;

IMPORT FOREIGN SCHEMA "default"
LIMIT TO (trip_fixture)
FROM SERVER iceberg
INTO imported;
```

## Register Existing Tables

Use `pgiceberg.register_table_from_location` to register an existing Iceberg
table location into a SQL catalog:

```sql
SELECT pgiceberg.register_table_from_location(
  'dev',
  'default',
  'external_trip',
  '/tmp/external_warehouse/default/external_trip'
);
```

The helper finds the latest `metadata/*.metadata.json` file under the table
location. To pin a specific metadata file, call `pgiceberg.register_table`:

```sql
SELECT pgiceberg.register_table(
  'dev',
  'default',
  'external_trip',
  '/tmp/external_warehouse/default/external_trip/metadata/00001.metadata.json'
);
```

The table location or metadata file must be readable by pgiceberg's local file
IO. The function creates the namespace when needed and rejects an existing
catalog table unless `drop_if_exists` is set to `true`.

## Metadata Helpers

Use the metadata utility functions to inspect the Iceberg table backing a
catalog entry:

```sql
SELECT pgiceberg.table_metadata_file_location(
  'dev',
  'default',
  'trip_fixture'
);

SELECT pgiceberg.table_format_version(
  'dev',
  'default',
  'trip_fixture'
);

SELECT pgiceberg.table_metadata_json(
  'dev',
  'default',
  'trip_fixture'
) ->> 'table-uuid';

SELECT pgiceberg.table_snapshot_files_summary(
  'dev',
  'default',
  'trip_fixture'
);

-- Inspect manifests and data files for a specific Iceberg snapshot id.
SELECT pgiceberg.table_snapshot_files_summary(
  'dev',
  'default',
  'trip_fixture',
  1234567890
);
```

To inspect a metadata file directly, call `metadata_file_json` with the
metadata file path:

```sql
SELECT pgiceberg.metadata_file_json(
  '/tmp/pgiceberg_warehouse/default/trip_fixture/metadata/00001.metadata.json'
);
```

psql backslash commands are client-side commands, so pgiceberg cannot extend
them. They are still useful next to the metadata helpers:

```psql
\df pgiceberg.*metadata*
\x on
SELECT pgiceberg.table_metadata_json(
  'dev',
  'default',
  'trip_fixture'
);
\d+ pgiceberg_trip_fixture
```

`\d+` shows the PostgreSQL foreign table, server, and table options. Use the
metadata utility functions for Iceberg metadata file contents.

## REST Catalogs

REST catalog support is optional and disabled by default. Build with
`-DPGICEBERG_ENABLE_REST_CATALOG=ON` before using a REST catalog server:

```sql
CREATE SERVER pgiceberg_rest_server
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (
  catalog_type 'rest',
  catalog_uri 'http://127.0.0.1:8181',
  warehouse 'dev'
);
```

## Cleanup

Drop the PostgreSQL objects when finished:

```sql
DROP FOREIGN TABLE IF EXISTS pgiceberg_trip_fixture;
DROP SCHEMA IF EXISTS imported CASCADE;
DROP SERVER IF EXISTS iceberg;
DROP EXTENSION IF EXISTS pgiceberg;
```
