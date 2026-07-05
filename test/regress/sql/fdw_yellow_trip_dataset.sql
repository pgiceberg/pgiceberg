-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE EXTENSION pgiceberg;

\pset format unaligned
\set VERBOSITY terse

DO $$
BEGIN
  PERFORM pgiceberg.add_catalog(
    'yellow_trip_dataset_regress',
    'sqlite',
    '/tmp/pgiceberg_catalog_yellow_trip_dataset_regress.db',
    '/tmp/pgiceberg_yellow_trip_dataset_regress'
  );
END $$;

SELECT pgiceberg.register_table_from_location(
  'yellow_trip_dataset_regress',
  'default',
  'yellow_trip',
  '/tmp/pgiceberg_yellow_trip_dataset_regress/default/yellow_trip',
  true
);

CREATE SERVER yellow_trip_dataset_server
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog 'yellow_trip_dataset_regress');

CREATE SCHEMA dataset_imported;

IMPORT FOREIGN SCHEMA "default"
LIMIT TO (yellow_trip)
FROM SERVER yellow_trip_dataset_server
INTO dataset_imported;

SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'dataset_imported'
  AND table_name = 'yellow_trip'
ORDER BY ordinal_position;

SELECT "VendorID", tpep_pickup_datetime, tpep_dropoff_datetime,
       passenger_count, trip_distance, "RatecodeID", store_and_fwd_flag,
       "PULocationID", "DOLocationID", payment_type, fare_amount,
       tip_amount, total_amount, tip_per_mile
FROM dataset_imported.yellow_trip
ORDER BY "VendorID", trip_distance, tpep_pickup_datetime
LIMIT 10;

SELECT count(*) AS row_count,
       sum(trip_distance)::numeric(10,2) AS trip_distance_sum,
       sum(fare_amount)::numeric(10,2) AS fare_amount_sum,
       sum(tip_amount)::numeric(10,2) AS tip_amount_sum,
       sum(total_amount)::numeric(10,2) AS total_amount_sum,
       sum(tip_per_mile)::numeric(10,2) AS tip_per_mile_sum
FROM dataset_imported.yellow_trip;

SELECT pgiceberg.table_format_version(
  'yellow_trip_dataset_regress',
  'default',
  'yellow_trip'
) AS format_version;

SELECT pgiceberg.table_metadata_json(
  'yellow_trip_dataset_regress',
  'default',
  'yellow_trip'
) ->> 'location' AS table_location;

\set VERBOSITY default

DROP SCHEMA dataset_imported CASCADE;
DROP SERVER yellow_trip_dataset_server;
DROP EXTENSION pgiceberg;
