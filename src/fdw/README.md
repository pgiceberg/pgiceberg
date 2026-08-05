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

# FDW surface

PostgreSQL Foreign Data Wrapper callbacks for mapping external Iceberg tables
into SQL (`FOREIGN DATA WRAPPER pgiceberg`).

This module owns planner and executor entry points only. Iceberg scans, DML,
options, and transaction commits live in `src/engine/`.

**Depends on:** `engine/`, `common/`  
**Must not depend on:** `tableam/`, `logical/`

Shared FDW planner helpers such as scan projection may be used by the
Parquet/Avro wrappers under `src/utilities/`.
