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

cache_result() {
  case "${1:-}" in
    true)
      printf "exact hit"
      ;;
    false)
      printf "partial hit"
      ;;
    "")
      printf "miss"
      ;;
    *)
      printf "%s" "$1"
      ;;
  esac
}

context="${CACHE_CONTEXT:-${RUNNER_OS:-unknown} / PostgreSQL ${PG_VERSION:-unknown}}"

{
  echo "### Dependency cache restore"
  echo
  echo "| Cache | Scope | Result |"
  echo "| --- | --- | --- |"
  echo "| pgenv PostgreSQL | ${context} | $(cache_result "${PGENV_CACHE_HIT:-}") |"
  echo "| CMake dependencies | ${context} | $(cache_result "${CMAKE_DEPS_CACHE_HIT:-}") |"
} >> "${GITHUB_STEP_SUMMARY}"
