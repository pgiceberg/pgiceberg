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

# Fail if a PostgreSQL surface module includes another surface module.
# Allowed shared layers: engine/, common/. utilities/ may use fdw/ helpers.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
failures=0

check_forbidden() {
  local dir="$1"
  shift
  local pattern
  for pattern in "$@"; do
    local matches
    matches="$(rg -n --glob '*.{cc,h}' "#include \"${pattern}" "${root}/src/${dir}" || true)"
    if [[ -n "${matches}" ]]; then
      echo "Forbidden include in src/${dir}/ (must not include ${pattern}):" >&2
      echo "${matches}" >&2
      failures=$((failures + 1))
    fi
  done
}

check_forbidden fdw 'tableam/' 'logical/'
check_forbidden tableam 'fdw/' 'logical/'
check_forbidden logical 'fdw/' 'tableam/'
check_forbidden engine 'fdw/' 'tableam/' 'logical/'
check_forbidden common 'fdw/' 'tableam/' 'logical/' 'engine/'
check_forbidden functions 'fdw/' 'tableam/' 'logical/'

if [[ "${failures}" -ne 0 ]]; then
  echo "${failures} surface include boundary violation(s)." >&2
  exit 1
fi

echo "Surface include boundaries OK."
