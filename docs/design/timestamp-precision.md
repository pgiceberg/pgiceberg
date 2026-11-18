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

# Timestamp precision policy

Iceberg v3 adds nanosecond `timestamp_ns` and `timestamptz_ns` types. PostgreSQL
`timestamp` and `timestamptz` store microseconds, so pgiceberg maps both Iceberg
precisions to the same PostgreSQL types and cannot preserve sub-microsecond digits.

## Read policy

The session-settable `pgiceberg.timestamp_ns_on_loss` GUC controls narrowing:

| Value | Behavior |
| --- | --- |
| `truncate` (default) | Floor nanoseconds to microseconds; emit one `NOTICE` per scan for each projected nanosecond column. |
| `error` | Reject each non-NULL nanosecond temporal value with `ERRCODE_FEATURE_NOT_SUPPORTED` and a hint naming the GUC. |

Floor division handles negative values consistently: `-1500ns` becomes `-2us`.
The previous integer division rounded toward zero and produced `-1us`.
Nearest rounding is not offered; it also loses precision and would require a
different conversion/filtering contract. A lossless PostgreSQL type would need
new I/O, casts, comparisons, operator classes, and ecosystem support.

## Enforcement and scope

[`common/datum_convert.cc`](../../src/common/datum_convert.cc) implements floor
conversion in `ScaleToMicros` and enforces `error` in `ConvertValue` for nanosecond
Arrow `timestamp`/`time64` values. The policy rejects that unit even when a
particular value is an exact microsecond. Empty scans and NULL-only values do not
trigger the per-value error.

[`engine/scan_state.cc`](../../src/engine/scan_state.cc) emits the scan-level
notice in `truncate` mode. It stays silent in `error` mode because conversion
performs the check.

The shared conversion path covers Iceberg FDW, Table AM, and Parquet file scans.
The standalone Avro file FDW has a separate path and still narrows silently.
Predicate translation must account for the precision PostgreSQL actually sees;
see [FDW expression pushdown](fdw-expression-pushdown.md).

## Verification

[`test/regress/sql/fdw_dml.sql`](../../test/regress/sql/fdw_dml.sql) uses a
microsecond fixture with metadata rewritten to `timestamp_ns`. It checks the
default notice, unchanged reads of exact-microsecond values, and rejection under
`error`.
