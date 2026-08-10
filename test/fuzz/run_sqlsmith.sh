#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

usage() {
  cat <<'EOF'
Run SQLsmith against a temporary pgiceberg fixture database.

Usage:
  run_sqlsmith.sh [options]

Options:
  --max-queries=N     Stop after N generated queries (default: 500)
  --seed=N            SQLsmith RNG seed (default: 1)
  --max-runtime=SEC   Wall-clock limit for SQLsmith (default: 300)
  --build-dir=PATH    pgiceberg CMake build directory
  --pg-config=PATH    pg_config binary (default: pg_config in PATH)
  --sqlsmith=PATH     sqlsmith binary (default: build/sqlsmith/sqlsmith)
  --keep-instance     Do not stop the temporary PostgreSQL instance
  --help              Show this help message

Environment:
  PGICEBERG_FUZZ_MAX_QUERIES, PGICEBERG_FUZZ_SEED, PGICEBERG_FUZZ_MAX_RUNTIME
  PGICEBERG_BUILD_DIR, PG_CONFIG, PGICEBERG_SQLSMITH

Exit status is non-zero when PostgreSQL crashes or fixture setup fails.
SQLsmith query errors are expected and do not fail the run.
EOF
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fuzz_dir="${repo_root}/test/fuzz"
sql_dir="${fuzz_dir}/sql"

max_queries="${PGICEBERG_FUZZ_MAX_QUERIES:-500}"
seed="${PGICEBERG_FUZZ_SEED:-1}"
max_runtime="${PGICEBERG_FUZZ_MAX_RUNTIME:-300}"
build_dir="${PGICEBERG_BUILD_DIR:-${repo_root}/build/tests}"
pg_config_bin="${PG_CONFIG:-pg_config}"
sqlsmith_bin="${PGICEBERG_SQLSMITH:-${repo_root}/build/sqlsmith/sqlsmith}"
keep_instance=0

while (($# > 0)); do
  case "$1" in
    --max-queries=*)
      max_queries="${1#*=}"
      ;;
    --seed=*)
      seed="${1#*=}"
      ;;
    --max-runtime=*)
      max_runtime="${1#*=}"
      ;;
    --build-dir=*)
      build_dir="${1#*=}"
      ;;
    --pg-config=*)
      pg_config_bin="${1#*=}"
      ;;
    --sqlsmith=*)
      sqlsmith_bin="${1#*=}"
      ;;
    --keep-instance)
      keep_instance=1
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [[ ! -x "${pg_config_bin}" && "${pg_config_bin}" == "pg_config" ]]; then
  pg_config_bin="$(command -v pg_config)"
fi
if [[ ! -x "${pg_config_bin}" ]]; then
  echo "pg_config not found: ${PG_CONFIG:-pg_config}" >&2
  exit 1
fi

pg_bindir="$("${pg_config_bin}" --bindir)"
pg_libdir="$("${pg_config_bin}" --pkglibdir)"
pg_version="$("${pg_config_bin}" --version | awk '{print $2}')"
pg_version_major="${pg_version%%.*}"

for tool in initdb pg_ctl createdb psql; do
  if [[ ! -x "${pg_bindir}/${tool}" ]]; then
    echo "PostgreSQL tool not found: ${pg_bindir}/${tool}" >&2
    exit 1
  fi
done

if [[ ! -x "${sqlsmith_bin}" ]]; then
  echo "sqlsmith not found at ${sqlsmith_bin}; run scripts/build-sqlsmith.sh" >&2
  exit 1
fi

extension_so="${build_dir}/src/libpgiceberg.so"
if [[ ! -f "${extension_so}" ]]; then
  extension_so="${build_dir}/src/pgiceberg.so"
fi
if [[ ! -f "${extension_so}" ]]; then
  echo "pgiceberg shared library not found under ${build_dir}/src" >&2
  exit 1
fi

instance_dir="${build_dir}/test/fuzz/tmp_check"
runtime_dir="${instance_dir}/pgdata"
socket_dir="/tmp"
database_name="pgiceberg_fuzz"
dataset_source="${repo_root}/test/datasets/yellow_trip"
dataset_runtime="/tmp/pgiceberg_yellow_trip_dataset_fuzz"
crash_flag="${instance_dir}/postmaster.crashed"
log_dir="${instance_dir}/log"
postmaster_pid=""

cleanup() {
  if [[ "${keep_instance}" -eq 1 ]]; then
    return
  fi
  if [[ -n "${postmaster_pid}" ]] && kill -0 "${postmaster_pid}" 2>/dev/null; then
    "${pg_bindir}/pg_ctl" -D "${runtime_dir}" -m fast stop >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

prepare_dataset() {
  if [[ ! -f "${dataset_source}/SHA256SUMS" ]]; then
    echo "missing dataset checksum file: ${dataset_source}/SHA256SUMS" >&2
    exit 1
  fi

  while IFS= read -r line || [[ -n "${line}" ]]; do
    if [[ -z "${line}" ]]; then
      continue
    fi
    read -r expected_sha rel_path <<<"${line}"
    local_path="${dataset_source}/${rel_path}"
    actual_sha="$(sha256sum "${local_path}" | awk '{print $1}')"
    if [[ "${actual_sha}" != "${expected_sha}" ]]; then
      echo "dataset checksum mismatch: ${local_path}" >&2
      exit 1
    fi
  done <"${dataset_source}/SHA256SUMS"

  rm -rf "${dataset_runtime}"
  rm -f /tmp/pgiceberg_catalog_fuzz.db
  rm -rf /tmp/pgiceberg_warehouse_fuzz
  mkdir -p "${dataset_runtime}/default"
  cp -a "${dataset_source}" "${dataset_runtime}/default/yellow_trip"
}

pick_free_port() {
  python3 - <<'PY'
import socket
with socket.socket() as s:
    s.bind(("127.0.0.1", 0))
    print(s.getsockname()[1])
PY
}

write_postgresql_conf() {
  local conf_file="${runtime_dir}/postgresql.conf"
  local fuzz_port
  fuzz_port="$(pick_free_port)"
  export PGICEBERG_FUZZ_PORT="${fuzz_port}"

  cat >>"${conf_file}" <<EOF

# pgiceberg SQLsmith fuzz settings
listen_addresses = '127.0.0.1'
port = ${fuzz_port}
unix_socket_directories = '${socket_dir}'
max_connections = 100
dynamic_library_path = '${build_dir}/src:${pg_libdir}'
log_line_prefix = '%m [%p] '
log_min_messages = warning
EOF

  if ((pg_version_major >= 18)); then
    echo "extension_control_path = '${build_dir}/test/extension:${runtime_dir}:$system" \
      >>"${conf_file}"
  fi
}

prepare_extension_control() {
  local extension_dir="${build_dir}/test/extension"
  mkdir -p "${extension_dir}"

  sed "s|module_pathname = '[^']*'|module_pathname = '${extension_so}'|" \
    "${repo_root}/pgiceberg.control" >"${extension_dir}/pgiceberg.control"
  cp "${repo_root}/sql/pgiceberg--0.1.0.sql" "${extension_dir}/"
}

start_instance() {
  rm -rf "${instance_dir}"
  mkdir -p "${log_dir}" "${runtime_dir}"
  rm -f "${crash_flag}"

  "${pg_bindir}/initdb" -D "${runtime_dir}" --no-sync >/dev/null
  write_postgresql_conf
  prepare_extension_control

  if ((pg_version_major < 18)); then
    local installed_control
    installed_control="$("${pg_config_bin}" --sharedir)/extension/pgiceberg.control"
    if [[ ! -f "${installed_control}" ]]; then
      echo "PostgreSQL ${pg_version_major} requires an installed pgiceberg extension." >&2
      echo "Run: cmake --install \"${build_dir}\" --component pgiceberg" >&2
      exit 1
    fi
  fi

  "${pg_bindir}/pg_ctl" -D "${runtime_dir}" -l "${log_dir}/postmaster.log" -w start
  postmaster_pid="$(awk 'NR == 1 { print $1 }' "${runtime_dir}/postmaster.pid")"
}

load_fixture() {
  "${pg_bindir}/createdb" -h "${socket_dir}" -p "${PGICEBERG_FUZZ_PORT}" "${database_name}"
  "${pg_bindir}/psql" -v ON_ERROR_STOP=1 -h "${socket_dir}" -p "${PGICEBERG_FUZZ_PORT}" \
    -d "${database_name}" -f "${sql_dir}/init.sql"
  "${pg_bindir}/psql" -v ON_ERROR_STOP=1 -h "${socket_dir}" -p "${PGICEBERG_FUZZ_PORT}" \
    -d "${database_name}" -f "${sql_dir}/roles.sql"
}

watch_postmaster() {
  while [[ ! -f "${runtime_dir}/postmaster.pid" ]]; do
    sleep 0.1
  done
  local pid
  pid="$(awk 'NR == 1 { print $1 }' "${runtime_dir}/postmaster.pid")"
  while kill -0 "${pid}" 2>/dev/null; do
    sleep 0.5
  done
  echo "postmaster exited unexpectedly" >"${crash_flag}"
}

run_sqlsmith() {
  local target="host=${socket_dir} port=${PGICEBERG_FUZZ_PORT} dbname=${database_name} user=fuzz_user password=fuzz"

  watch_postmaster &
  local watcher_pid=$!

  set +e
  timeout "${max_runtime}" "${sqlsmith_bin}" \
    --target="${target}" \
    --seed="${seed}" \
    --max-queries="${max_queries}" \
    --exclude-catalog \
    --verbose
  local sqlsmith_status=$?
  set -e

  kill "${watcher_pid}" 2>/dev/null || true
  wait "${watcher_pid}" 2>/dev/null || true

  if [[ -f "${crash_flag}" ]]; then
    echo "PostgreSQL crashed during SQLsmith run" >&2
    if [[ -f "${log_dir}/postmaster.log" ]]; then
      echo ":: PostgreSQL log ::"
      tail -n 80 "${log_dir}/postmaster.log" >&2 || true
    fi
    exit 1
  fi

  if [[ "${sqlsmith_status}" -eq 124 ]]; then
    echo "SQLsmith hit the ${max_runtime}s runtime limit" >&2
    exit 1
  fi
  if [[ "${sqlsmith_status}" -ne 0 ]]; then
    echo "SQLsmith exited with status ${sqlsmith_status}" >&2
    exit "${sqlsmith_status}"
  fi
}

main() {
  prepare_dataset
  start_instance
  load_fixture
  run_sqlsmith
  echo "SQLsmith completed ${max_queries} queries without crashing PostgreSQL"
}

main "$@"
