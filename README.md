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

SELECT pgiceberg.create_table(
  'sqlite',
  '/tmp/pgiceberg_catalog_dev.db',
  '/tmp/pgiceberg_warehouse',
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
  true,
  'pgiceberg_dev'
);

CREATE SERVER iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (
  catalog_type 'sqlite',
  catalog_uri '/tmp/pgiceberg_catalog_dev.db',
  warehouse '/tmp/pgiceberg_warehouse',
  catalog_name 'pgiceberg_dev'
);

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
