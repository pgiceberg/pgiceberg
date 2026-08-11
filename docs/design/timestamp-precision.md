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

## Problem

Iceberg v3 defines nanosecond-resolution temporal types (`timestamp_ns` and
`timestamptz_ns`) in addition to the microsecond `timestamp` / `timestamptz`.
PostgreSQL's `timestamp` and `timestamptz` are fixed at **microsecond**
resolution (an `int64` count of microseconds since 2000-01-01), so a nanosecond
Iceberg value cannot round-trip losslessly through PostgreSQL.

`pgiceberg` maps both the microsecond and nanosecond Iceberg types onto the same
PostgreSQL `timestamp` / `timestamptz` columns (see
`common/type_mapping.cc`). Historically the read path narrowed nanoseconds to
microseconds with a bare integer division (`value / 1000`), which:

- silently dropped the sub-microsecond digits, and
- truncated toward zero, so pre-1970 (negative) values narrowed
  non-monotonically (`-1500ns` mapped to `-1us` instead of `-2us`).

## Policy

Narrowing is now explicit, documented, and configurable through the
`pgiceberg.timestamp_ns_on_loss` GUC (`PGC_USERSET`):

| Value | Behavior |
| --- | --- |
| `truncate` (default) | Narrow nanoseconds to microseconds using **floor** division (rounds toward negative infinity, so the mapping is monotonic). A single `NOTICE` is emitted per scan for each projected nanosecond column. |
| `error` | Refuse the read: converting a nanosecond value raises `ERRCODE_FEATURE_NOT_SUPPORTED` with a hint pointing back at this GUC. |

Rounding to the nearest microsecond was considered and rejected: it is not more
correct than truncation (both lose the sub-microsecond digits) and it can move a
value across a microsecond/second boundary, which would break ordering and make
pushed-down range filters inconsistent with the narrowed values PostgreSQL sees.

A dedicated nanosecond-precision PostgreSQL type would preserve full fidelity but
requires new SQL types, I/O and comparison functions, operator classes, and
casts, plus ecosystem support. It is deliberately left as possible future work;
the GUC covers the practical cases today.

## Enforcement points

- **Value conversion** — `common/datum_convert.cc`. `ScaleToMicros` floors on the
  nanosecond unit. Under the `error` policy, `ConvertValue` rejects any Arrow
  `timestamp`/`time64` array whose unit is nanoseconds. Enforcing per value means
  an empty scan of a nanosecond column loses nothing and never errors. This
  covers every reader that goes through `ConvertValue` (Iceberg FDW scans, the
  table access method, and parquet file scans).
- **Scan setup NOTICE** — `engine/scan_state.cc`. When a scan projects a
  nanosecond column and the policy is `truncate`, `BeginScan` emits one `NOTICE`
  per column so the narrowing is observable without per-row log spam. The `error`
  policy stays silent here because the per-value check already rejects the read.

## Scope

The GUC governs reads of Iceberg tables (FDW and table AM). The standalone
`pgiceberg_avro` file FDW has its own value path and always narrows silently; it
is out of scope for this policy.

## Verification

`test/regress/sql/fdw_dml.sql` registers a `timestamp_ns` fixture (a microsecond
table whose metadata is rewritten to `timestamp_ns`) and exercises:

- the default `truncate` policy, asserting the per-scan `NOTICE` and that a
  microsecond value reads back unchanged, and
- the `error` policy, asserting the read is rejected.
