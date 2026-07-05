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

# yellow_trip Dataset

This fixture is a small Iceberg table derived from the NYC Taxi &
Limousine Commission yellow taxi trip data. It is checked in so the
FDW regression tests can read a PyIceberg-created table without
downloading data during normal test runs.

## Source

- URL: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
- SHA256: `32df6f67578fa86c484a6b5ef23a5281992ff085521082340b0f9e5889e9a572`
- Full source rows: `3,066,766`
- Full source size: `46M`

## Sampling

- Rows: first `1000` rows from the source parquet file.
- Columns: all source columns plus derived `tip_per_mile`.
- `tip_per_mile`: `tip_amount / trip_distance`; `NULL` when
  `trip_distance = 0`.
- Output table: `default.yellow_trip`.
- Iceberg format version: `2`.
- Data file compression: uncompressed parquet, to keep the fixture
  readable by the current pgiceberg/iceberg-cpp test build.

## Regeneration

Download the source file first:

```sh
curl -L \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet \
  -o /tmp/yellow_tripdata_2023-01.parquet
```

Then regenerate this dataset:

```sh
python3 scripts/prepare-yellow-trip-dataset.py
```

The generator verifies the source SHA256 before writing files,
then rewrites `SHA256SUMS` for all committed dataset artifacts.
