#!/usr/bin/env bash
set -euo pipefail

pg_version="${1:-18.4}"
build_dir="${BUILD_DIR:-/tmp/pgiceberg-build/pg${pg_version}}"
export CCACHE_DIR="${CCACHE_DIR:-${PWD}/build/.ccache}"
mkdir -p "${CCACHE_DIR}"

if ! command -v pgenv >/dev/null 2>&1; then
  echo "pgenv is required. Install https://github.com/theory/pgenv and add it to PATH." >&2
  exit 1
fi

pgenv switch "${pg_version}"

cmake -S . -B "${build_dir}" -GNinja \
  -DPG_CONFIG="$(command -v pg_config)"
cmake --build "${build_dir}"
cmake --install "${build_dir}" --component pgiceberg
ctest --test-dir "${build_dir}" --output-on-failure
