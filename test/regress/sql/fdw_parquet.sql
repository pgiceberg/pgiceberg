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

CREATE SERVER parquet_server FOREIGN DATA WRAPPER pgiceberg_parquet OPTIONS (
  dirname '/tmp/pgiceberg_yellow_trip_dataset_regress/default/yellow_trip/data'
);

CREATE FOREIGN TABLE parquet_yellow_trip (
  "VendorID" bigint,
  tpep_pickup_datetime timestamp,
  tpep_dropoff_datetime timestamp,
  passenger_count double precision,
  trip_distance double precision,
  "RatecodeID" double precision,
  store_and_fwd_flag text,
  "PULocationID" bigint,
  "DOLocationID" bigint,
  payment_type bigint,
  fare_amount double precision,
  extra double precision,
  mta_tax double precision,
  tip_amount double precision,
  tolls_amount double precision,
  improvement_surcharge double precision,
  total_amount double precision,
  congestion_surcharge double precision,
  airport_fee double precision,
  tip_per_mile double precision
)
SERVER parquet_server
OPTIONS (
  filename '/tmp/pgiceberg_yellow_trip_dataset_regress/default/yellow_trip/data/00000-0-81fc6f9d-c034-4d6b-ad00-895e23ab2189.parquet'
);

SELECT count(*) AS row_count,
       sum(trip_distance)::numeric(10,2) AS trip_distance_sum,
       sum(fare_amount)::numeric(10,2) AS fare_amount_sum
FROM parquet_yellow_trip;

CREATE SCHEMA parquet_imported;

IMPORT FOREIGN SCHEMA "."
LIMIT TO ("00000-0-81fc6f9d-c034-4d6b-ad00-895e23ab2189")
FROM SERVER parquet_server
INTO parquet_imported;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'parquet_imported'
  AND table_name = '00000-0-81fc6f9d-c034-4d6b-ad00-895e23ab2189'
ORDER BY ordinal_position
LIMIT 5;

\set VERBOSITY default

DROP SCHEMA parquet_imported CASCADE;
DROP FOREIGN TABLE parquet_yellow_trip;
DROP SERVER parquet_server;
DROP EXTENSION pgiceberg;
