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

# COPY support

This directory is reserved for future pgiceberg COPY support.

`COPY ... WITH (FORMAT iceberg)` is intentionally not supported today.
PostgreSQL 18 has internal COPY format routines, but it does not
provide a stable extension API for registering an external COPY format.

Once PostgreSQL core provides a supported custom COPY format API, pgiceberg can
use this directory for the Iceberg COPY format handler implementation.

Reference: https://www.postgresql.org/message-id/flat/20231204.153548.2126325458835528809.kou%40clear-code.com
