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

# Shared Iceberg engine

Surface-agnostic Iceberg read/write primitives used by the FDW, table AM, and
logical mirrors:

- options parsing and catalog binding (`options`)
- scan state (`scan_state`, `iceberg_scan`)
- modify/append state and PostgreSQL transaction hooks (`modify_state`)

- **Depends on:** `common/`, iceberg-cpp
- **Must not depend on:** `fdw/`, `tableam/`, `logical/`

Callers that need crash-recognizable commits pass generic `CommitProperties`
into `AppendSlots`; logical batch-id bookkeeping stays in `src/logical/`.
