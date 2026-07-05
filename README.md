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

PostgreSQL extension for Apache Iceberg tables.

The project keeps the PostgreSQL integration layer separate from the Iceberg
access layer. Iceberg catalog, metadata, manifest, manifest list, and Parquet
data file handling are delegated to apache/iceberg-cpp.

## Layout

- `src/fdw/`: PostgreSQL Foreign Data Wrapper callbacks and scan state.
- `src/common/`: PostgreSQL/C++ boundary helpers.
- `sql/` and `pgiceberg.control`: extension install metadata.
- `test/regress/`: `pg_regress` smoke tests.
- `scripts/`: pgenv-oriented build helpers.

## Build

Activate the desired PostgreSQL version with pgenv, then configure with the
matching `pg_config`.

Install pgenv locally if it is not already available:

```sh
git clone --branch v1.4.3 --depth 1 https://github.com/theory/pgenv.git ~/.pgenv
printf '\n# pgenv\nexport PATH="$HOME/.pgenv/bin:$HOME/.pgenv/pgsql/bin:$PATH"\n' >> ~/.bashrc
source ~/.bashrc
```

Build and switch to PostgreSQL 18.4:

```sh
pgenv check
pgenv build 18.4
pgenv switch 18.4
```

Build pgiceberg with the active PostgreSQL installation. The vendored
iceberg-cpp dependency requires a C++23 compiler such as GCC 14+; the
devcontainer sets GCC 14 as the default compiler through `update-alternatives`.

```sh
BUILD_DIR=/tmp/pgiceberg-build/pg18.4
cmake -S . -B "$BUILD_DIR" -GNinja -DPG_CONFIG="$(command -v pg_config)"
cmake --build "$BUILD_DIR"
cmake --install "$BUILD_DIR" --component pgiceberg
```

For a pgenv-driven build, install, and test cycle:

```sh
scripts/pgenv-build.sh 18.4
```

## CI and Checks

GitHub Actions runs the PR gate on push and pull request:

- `pre-commit` formatting and repository hygiene checks.
- Build, install, and `pg_regress` on Ubuntu and macOS.
- Ubuntu ASAN/UBSAN build and regression test.

Install the local hooks with:

```sh
pre-commit install --install-hooks
```

Run the same formatting checks locally with:

```sh
pre-commit run --all-files
```

Run a local sanitizer configure with:

```sh
BUILD_DIR=/tmp/pgiceberg-build/sanitizers
cmake -S . -B "$BUILD_DIR" -GNinja \
  -DPG_CONFIG="$(command -v pg_config)" \
  -DPGICEBERG_SANITIZERS=address,undefined
```

pgiceberg builds against apache/iceberg-cpp with `ICEBERG_BUILD_SQL_CATALOG=ON`
and `ICEBERG_SQL_POSTGRESQL=ON`; SQL catalog behavior is provided by
iceberg-cpp rather than a local pgiceberg catalog implementation. REST catalog
support is optional and disabled by default:

```sh
BUILD_DIR=/tmp/pgiceberg-build/pg18.4
cmake -S . -B "$BUILD_DIR" -GNinja \
  -DPG_CONFIG="$(command -v pg_config)" \
  -DPGICEBERG_ENABLE_REST_CATALOG=ON
```

## Manual Smoke Test

Use a non-default port if another PostgreSQL instance already owns `5432`.
The examples below use PostgreSQL 18.4 from pgenv, port `55432`, and the
built-in `postgres` database user.

```sh
pgenv switch 18.4
cmake --install /tmp/pgiceberg-build/pg18.4 --component pgiceberg
PGPORT=55432 pgenv start 18.4
PGPORT=55432 createdb -U postgres pgiceberg_dev
PGPORT=55432 psql -U postgres -d pgiceberg_dev
```

If `pgiceberg_dev` already exists, skip `createdb` and connect directly:

```sh
PGPORT=55432 psql -U postgres -d pgiceberg_dev
```

Run this SQL in `psql`:

```sql
CREATE EXTENSION pgiceberg;

SELECT fdwname
FROM pg_foreign_data_wrapper
WHERE fdwname = 'pgiceberg';

SELECT pgiceberg.add_catalog(
  'dev',
  'sqlite',
  '/tmp/pgiceberg_catalog_dev.db',
  '/tmp/pgiceberg_warehouse'
);

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

CREATE SERVER iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog 'dev');

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

SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM pgiceberg_trip_fixture
ORDER BY vendorid, trip_distance;
```

The Iceberg table must already exist in the configured Iceberg SQL catalog.
The `pgiceberg.create_table` call above creates the `default.trip_fixture`
table in the SQLite Iceberg catalog before `CREATE FOREIGN TABLE` maps it into
PostgreSQL.

Iceberg data files are Parquet, but pgiceberg does not read a Parquet path
directly; it loads the Iceberg table through iceberg-cpp so snapshots,
manifests, deletes, and metadata are respected.

`INSERT` appends rows through Iceberg `FastAppend`. `UPDATE` and `DELETE` are
supported for unpartitioned Iceberg tables by rewriting the current data files.

```sql
INSERT INTO pgiceberg_trip_fixture
VALUES (1, 2, 3.5, 'N');

UPDATE pgiceberg_trip_fixture
SET passenger_count = 3
WHERE vendorid = 1;

DELETE FROM pgiceberg_trip_fixture
WHERE vendorid = 1;
```

For schema inference from an existing Iceberg table, use `IMPORT FOREIGN
SCHEMA`:

```sql
CREATE SCHEMA imported;

IMPORT FOREIGN SCHEMA "default"
LIMIT TO (trip_fixture)
FROM SERVER iceberg
INTO imported;
```

To register an existing Iceberg table location into a SQL catalog first, use
`pgiceberg.register_table_from_location`:

```sql
SELECT pgiceberg.register_table_from_location(
  'dev',
  'default',
  'trip_fixture',
  '/tmp/external_warehouse/default/trip_fixture'
);
```

The helper finds the latest `metadata/*.metadata.json` file under the table
location. If you need to pin a specific metadata file, use
`pgiceberg.register_table`:

```sql
SELECT pgiceberg.register_table(
  'dev',
  'default',
  'trip_fixture',
  '/tmp/external_warehouse/default/trip_fixture/metadata/00001.metadata.json'
);
```

The table location or registered metadata location must be readable by
pgiceberg's local file IO. The function creates the namespace when needed and
rejects an existing catalog table unless `drop_if_exists` is set to `true`.

Use the metadata utility functions to inspect the Iceberg metadata file backing
a catalog table. Register catalog connection details once:

```sql
SELECT pgiceberg.add_catalog(
  'dev',
  'sqlite',
  '/tmp/pgiceberg_catalog_dev.db',
  '/tmp/pgiceberg_warehouse'
);
```

The first argument is the local `name` used by pgiceberg helper functions.
pgiceberg uses the same value as the logical Iceberg catalog name unless an
explicit `iceberg_catalog_name` fifth argument is provided.

Then use the short helper signatures:

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
```

To inspect a metadata file directly, call `metadata_file_json` with the metadata
file path:

```sql
SELECT pgiceberg.metadata_file_json(
  '/tmp/pgiceberg_warehouse/default/trip_fixture/metadata/00001.metadata.json'
);
```

psql backslash commands are client-side commands and cannot be extended by a
PostgreSQL extension. They are still useful around these helpers:

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
metadata utility functions for the Iceberg metadata file contents.

REST catalog support is optional. This requires
`-DPGICEBERG_ENABLE_REST_CATALOG=ON`; default builds fail with a
feature-not-supported error:

```sql
CREATE SERVER pgiceberg_rest_server
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (
  catalog_type 'rest',
  catalog_uri 'http://127.0.0.1:8181',
  warehouse 'dev'
);
```

Clean up the manual objects when finished:

```sql
DROP FOREIGN TABLE IF EXISTS trip_fixture;
DROP FOREIGN TABLE pgiceberg_trip_fixture;
DROP SCHEMA IF EXISTS imported CASCADE;
DROP SERVER iceberg;
DROP EXTENSION pgiceberg;
```

Stop the test server when finished:

```sh
PGPORT=55432 pgenv stop 18.4
```

## Dev Containers

We provide Dev Container configuration file templates.

To use a Dev Container as your development environment, follow the steps below,
then select `Dev Containers: Reopen in Container` from VS Code's Command
Palette.

```sh
cd .devcontainer
cp Dockerfile.template Dockerfile
cp devcontainer.json.template devcontainer.json
cp post-create.sh.template post-create.sh
```

The post-create script installs developer tools that should not be baked into
the image, such as Codex and Cursor Agent.

If you make improvements that could benefit all developers, please update the
template files and submit a pull request.
