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

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Pinned to sqlsmith master at 16aad4e2 (2026-03), which provides CMake build files.
sqlsmith_ref="${SQLSMITH_GIT_REF:-16aad4e262f09b286ae891926c990df266feedfd}"
build_dir="${SQLSMITH_BUILD_DIR:-${repo_root}/build/sqlsmith}"
source_dir="${build_dir}/upstream"

if [[ -x "${build_dir}/sqlsmith" ]]; then
  echo "sqlsmith already built at ${build_dir}/sqlsmith"
  exit 0
fi

if ! command -v cmake >/dev/null 2>&1; then
  echo "cmake is required to build sqlsmith" >&2
  exit 1
fi

if ! command -v git >/dev/null 2>&1; then
  echo "git is required to build sqlsmith" >&2
  exit 1
fi

mkdir -p "${build_dir}"

if [[ ! -f "${source_dir}/CMakeLists.txt" ]]; then
  rm -rf "${source_dir}"
  git clone --filter=blob:none --no-checkout \
    https://github.com/anse1/sqlsmith.git "${source_dir}"
  git -C "${source_dir}" checkout "${sqlsmith_ref}"
fi

cmake -S "${source_dir}" -B "${build_dir}" -GNinja \
  -DCMAKE_BUILD_TYPE=Release
cmake --build "${build_dir}"

if [[ ! -x "${build_dir}/sqlsmith" ]]; then
  echo "sqlsmith build failed: ${build_dir}/sqlsmith not found" >&2
  exit 1
fi

echo "built sqlsmith at ${build_dir}/sqlsmith"
