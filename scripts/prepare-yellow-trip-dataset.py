#!/usr/bin/env python3
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

from __future__ import annotations

import argparse
import hashlib
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "test/datasets"
DEFAULT_RUNTIME_ROOT = Path("/tmp")
YELLOW_TRIP_SOURCE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2023-01.parquet"
)
YELLOW_TRIP_SOURCE_SHA256 = (
    "32df6f67578fa86c484a6b5ef23a5281992ff085521082340b0f9e5889e9a572"
)
YELLOW_TRIP_SOURCE_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]
YELLOW_TRIP_SAMPLE_ROWS = 1000


def yellow_trip_data(source_path: Path) -> pa.Table:
    if not source_path.exists():
        raise FileNotFoundError(
            f"{source_path} does not exist; download {YELLOW_TRIP_SOURCE_URL} first"
        )
    source_sha256 = hashlib.sha256(source_path.read_bytes()).hexdigest()
    if source_sha256 != YELLOW_TRIP_SOURCE_SHA256:
        raise ValueError(
            f"{source_path} sha256 mismatch: expected {YELLOW_TRIP_SOURCE_SHA256}, "
            f"got {source_sha256}"
        )

    table = pq.read_table(
        source_path,
        columns=YELLOW_TRIP_SOURCE_COLUMNS,
    ).slice(0, YELLOW_TRIP_SAMPLE_ROWS)
    tip_per_mile = pc.if_else(
        pc.not_equal(table["trip_distance"], pa.scalar(0.0)),
        pc.divide(table["tip_amount"], table["trip_distance"]),
        pa.nulls(table.num_rows, type=pa.float64()),
    )
    return table.append_column("tip_per_mile", tip_per_mile)


DATASETS = {
    "yellow_trip": yellow_trip_data,
}


def generate_dataset(
    dataset: str, output_root: Path, runtime_root: Path, yellow_trip_source: Path
) -> None:
    runtime_warehouse = runtime_root / f"pgiceberg_{dataset}_dataset_regress"
    runtime_catalog = runtime_root / f"pgiceberg_{dataset}_dataset_regress.db"
    output = output_root / dataset

    shutil.rmtree(runtime_warehouse, ignore_errors=True)
    runtime_catalog.unlink(missing_ok=True)

    catalog = load_catalog(
        "pgiceberg_dataset",
        type="sql",
        uri=f"sqlite:///{runtime_catalog}",
        warehouse=str(runtime_warehouse),
    )
    catalog.create_namespace("default")

    data = DATASETS[dataset](yellow_trip_source)
    table = catalog.create_table(
        f"default.{dataset}",
        schema=data.schema,
        properties={
            "format-version": "2",
            "write.parquet.compression-codec": "uncompressed",
        },
    )
    table.append(data)

    shutil.rmtree(output, ignore_errors=True)
    output.mkdir(parents=True)
    shutil.copytree(runtime_warehouse / "default" / dataset, output, dirs_exist_ok=True)
    if dataset == "yellow_trip":
        write_yellow_trip_source(output)
    write_sha256s(output)


def write_yellow_trip_source(output: Path) -> None:
    (output / "README.md").write_text(
        "\n".join(
            [
                "<!--",
                'Licensed under the Apache License, Version 2.0 (the "License");',
                "you may not use this file except in compliance with the License.",
                "You may obtain a copy of the License at",
                "",
                "    http://www.apache.org/licenses/LICENSE-2.0",
                "",
                "Unless required by applicable law or agreed to in writing, software",
                'distributed under the License is distributed on an "AS IS" BASIS,',
                "WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.",
                "See the License for the specific language governing permissions and",
                "limitations under the License.",
                "-->",
                "",
                "# yellow_trip Dataset",
                "",
                "This fixture is a small Iceberg table derived from the NYC Taxi &",
                "Limousine Commission yellow taxi trip data. It is checked in so the",
                "FDW regression tests can read a PyIceberg-created table without",
                "downloading data during normal test runs.",
                "",
                "## Source",
                "",
                f"- URL: {YELLOW_TRIP_SOURCE_URL}",
                f"- SHA256: `{YELLOW_TRIP_SOURCE_SHA256}`",
                f"- Full source rows: `3,066,766`",
                "- Full source size: `46M`",
                "",
                "## Sampling",
                "",
                f"- Rows: first `{YELLOW_TRIP_SAMPLE_ROWS}` rows from the source parquet file.",
                "- Columns: all source columns plus derived `tip_per_mile`.",
                "- `tip_per_mile`: `tip_amount / trip_distance`; `NULL` when",
                "  `trip_distance = 0`.",
                "- Output table: `default.yellow_trip`.",
                "- Iceberg format version: `2`.",
                "- Data file compression: uncompressed parquet, to keep the fixture",
                "  readable by the current pgiceberg/iceberg-cpp test build.",
                "",
                "## Regeneration",
                "",
                "Download the source file first:",
                "",
                "```sh",
                "curl -L \\",
                f"  {YELLOW_TRIP_SOURCE_URL} \\",
                "  -o /tmp/yellow_tripdata_2023-01.parquet",
                "```",
                "",
                "Then regenerate this dataset:",
                "",
                "```sh",
                "python3 scripts/prepare-yellow-trip-dataset.py",
                "```",
                "",
                "The generator verifies the source SHA256 before writing files,",
                "then rewrites `SHA256SUMS` for all committed dataset artifacts.",
                "",
            ]
        ),
        encoding="utf-8",
    )


def write_sha256s(output: Path) -> None:
    paths = sorted(path for path in output.rglob("*") if path.is_file())
    manifest_lines: list[str] = []
    for path in paths:
        if path.name == "SHA256SUMS":
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        relative = path.relative_to(output).as_posix()
        manifest_lines.append(f"{digest}  {relative}\n")
    (output / "SHA256SUMS").write_text("".join(manifest_lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate Iceberg datasets used by pgiceberg regress tests."
    )
    parser.add_argument(
        "datasets",
        nargs="*",
        choices=sorted(DATASETS),
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--runtime-root", type=Path, default=DEFAULT_RUNTIME_ROOT)
    parser.add_argument(
        "--yellow-trip-source",
        type=Path,
        default=Path("/tmp/yellow_tripdata_2023-01.parquet"),
    )
    args = parser.parse_args()

    for dataset in args.datasets or sorted(DATASETS):
        generate_dataset(dataset, args.output_root, args.runtime_root, args.yellow_trip_source)


if __name__ == "__main__":
    main()
