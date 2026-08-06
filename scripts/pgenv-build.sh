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

pg_version="${1:-18.4}"
build_dir="${BUILD_DIR:-${PWD}/build/pg${pg_version}-debug}"
export CCACHE_DIR="${CCACHE_DIR:-${PWD}/build/.ccache}"
mkdir -p "${CCACHE_DIR}" "${build_dir}"

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
