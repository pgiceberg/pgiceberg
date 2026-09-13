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

fuzz_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
runner="${fuzz_dir}/run_sqlsmith.sh"

extract_function() {
  local name="$1"
  awk -v name="${name}" '
    $0 ~ "^" name "\\(\\) \\{" {capture=1}
    capture {print}
    capture && $0 == "}" {exit}
  ' "${runner}"
}

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT

helpers="${tmp}/helpers.sh"
{
  extract_function pick_free_port
  extract_function write_postgresql_conf
} >"${helpers}"
# shellcheck disable=SC1090
source "${helpers}"

assert_pg18_writes_literal_system_token() {
  runtime_dir="${tmp}/pg18/pgdata"
  build_dir="${tmp}/pg18/build"
  pg_libdir="${tmp}/pg18/lib"
  socket_dir="${tmp}/pg18/sock"
  pg_version_major=18
  mkdir -p "${runtime_dir}"
  : >"${runtime_dir}/postgresql.conf"

  write_postgresql_conf

  local expected="extension_control_path = '${build_dir}/test/extension:${runtime_dir}:\$system'"
  if ! grep -Fxq "${expected}" "${runtime_dir}/postgresql.conf"; then
    echo "PG 18 conf is missing a quoted \$system token:" >&2
    echo "expected: ${expected}" >&2
    echo "got:" >&2
    cat "${runtime_dir}/postgresql.conf" >&2
    exit 1
  fi
}

assert_pg16_omits_extension_control_path() {
  runtime_dir="${tmp}/pg16/pgdata"
  build_dir="${tmp}/pg16/build"
  pg_libdir="${tmp}/pg16/lib"
  socket_dir="${tmp}/pg16/sock"
  pg_version_major=16
  mkdir -p "${runtime_dir}"
  : >"${runtime_dir}/postgresql.conf"

  write_postgresql_conf

  if grep -q 'extension_control_path' "${runtime_dir}/postgresql.conf"; then
    echo "PG 16 conf should not set extension_control_path:" >&2
    cat "${runtime_dir}/postgresql.conf" >&2
    exit 1
  fi
}

assert_pg18_writes_literal_system_token
assert_pg16_omits_extension_control_path
echo "write_postgresql_conf keeps PostgreSQL's \$system token literal"
