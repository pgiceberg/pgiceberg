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

# Contributing

This file covers local development commands for pgiceberg. For installation
and SQL usage examples, see [README.md](README.md).

## Source Layout

pgiceberg has three independent PostgreSQL surfaces over one shared Iceberg
engine. See
[docs/design/postgres-extension-surfaces.md](docs/design/postgres-extension-surfaces.md)
for roles, overlap, and the dependency rules.

- `src/engine/`: shared Iceberg read/write engine (options, scans, DML, pending
  transaction state) used by the FDW, table AM, and logical mirroring.
- `src/fdw/`: Iceberg Foreign Data Wrapper callbacks and shared FDW planner
  helpers (also used by the Parquet/Avro utilities).
- `src/tableam/`: native `CREATE TABLE ... USING iceberg` table access method.
- `src/logical/`: logical decoding output plugin, background worker, and
  mirror SQL helpers.
- `src/common/`: PostgreSQL and C++ boundary helpers.
- `src/functions/`: SQL-callable catalog/table helper functions.
- `src/utilities/`: read-only Parquet and Avro FDWs.
- `src/copy/`: reserved for a future COPY format handler (no stable PG API yet).
- `docs/design/`: internal design notes for PostgreSQL extension surfaces and
  implementation tradeoffs.
- `sql/` and `pgiceberg.control`: extension install metadata.
- `test/regress/`: `pg_regress` SQL tests.
- `test/datasets/`: small regression fixtures.
- `scripts/`: local build helpers.

Keep surface modules independent: `fdw/`, `tableam/`, and `logical/` must not
include each other. Shared Iceberg behavior goes in `engine/` or `common/`.
`scripts/check-surface-includes.sh` (also a local pre-commit hook) enforces
that include rule.

## PostgreSQL With pgenv

Install pgenv locally if it is not already available:

```sh
git clone --branch v1.4.3 --depth 1 https://github.com/theory/pgenv.git ~/.pgenv
printf '\n# pgenv\nexport PATH="$HOME/.pgenv/bin:$HOME/.pgenv/pgsql/bin:$PATH"\n' >> ~/.bashrc
source ~/.bashrc
```

Build and switch to the PostgreSQL version you want to test:

```sh
pgenv check
pgenv build 18.4
pgenv switch 18.4
```

## Build

Configure with the `pg_config` from the PostgreSQL installation that should
load pgiceberg:

```sh
BUILD_DIR=/tmp/pgiceberg-build/pg18.4
cmake -S . -B "$BUILD_DIR" -GNinja -DPG_CONFIG="$(command -v pg_config)"
cmake --build "$BUILD_DIR"
cmake --install "$BUILD_DIR" --component pgiceberg
```

The devcontainer sets GCC 14 as the default compiler through
`update-alternatives`. Outside the devcontainer, use a C++23 compiler supported
by iceberg-cpp.

For a pgenv-driven build, install, and test cycle:

```sh
scripts/pgenv-build.sh 18.4
```

## Regression Tests

Run all configured tests from the build directory:

```sh
ctest --test-dir /tmp/pgiceberg-build/pg18.4 --output-on-failure
```

Run only the FDW regression target:

```sh
ctest --test-dir /tmp/pgiceberg-build/pg18.4 --output-on-failure -R regress_fdw
```

The CTest regression wrapper sets `PG_REGRESS_SOCK_DIR=/tmp` and validates the
committed dataset checksums before running `pg_regress`.

## Checks

Install local hooks:

```sh
pre-commit install --install-hooks
```

Run formatting and repository hygiene checks:

```sh
pre-commit run --all-files
```

Before sending a small patch, also check for whitespace errors:

```sh
git diff --check
```

## Sanitizers

Configure an AddressSanitizer and UndefinedBehaviorSanitizer build with:

```sh
BUILD_DIR=/tmp/pgiceberg-build/sanitizers
cmake -S . -B "$BUILD_DIR" -GNinja \
  -DPG_CONFIG="$(command -v pg_config)" \
  -DPGICEBERG_SANITIZERS=address,undefined
cmake --build "$BUILD_DIR"
ctest --test-dir "$BUILD_DIR" --output-on-failure
```

## Optional REST Catalog Support

REST catalog support is off by default. Enable it at configure time:

```sh
BUILD_DIR=/tmp/pgiceberg-build/pg18.4
cmake -S . -B "$BUILD_DIR" -GNinja \
  -DPG_CONFIG="$(command -v pg_config)" \
  -DPGICEBERG_ENABLE_REST_CATALOG=ON
```

## CI

GitHub Actions runs the PR gate on push and pull request:

- `pre-commit` formatting and repository hygiene checks.
- Build, install, and `pg_regress` on Ubuntu and macOS.
- Ubuntu ASAN/UBSAN build and regression tests.

## Dev Containers

The repository provides Dev Container template files. To use them, create local
copies and then select `Dev Containers: Reopen in Container` from VS Code's
Command Palette.

```sh
cd .devcontainer
cp Dockerfile.template Dockerfile
cp devcontainer.json.template devcontainer.json
cp post-create.sh.template post-create.sh
```

The post-create script installs developer tools that should not be baked into
the image, such as Codex and Cursor Agent.

If you make improvements that could benefit all developers, update the template
files and submit a pull request.
