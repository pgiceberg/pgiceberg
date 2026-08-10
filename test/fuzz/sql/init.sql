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

-- SQLsmith fixture schema for pgiceberg fuzz testing.
-- Loaded by test/fuzz/run_sqlsmith.sh into a dedicated database.

CREATE EXTENSION pgiceberg;

SET DateStyle = 'ISO, YMD';
SET TIME ZONE 'UTC';
SET pgiceberg.arrow_cpu_threads = 1;
SET pgiceberg.arrow_io_threads = 1;

DO $$
BEGIN
  PERFORM pgiceberg.add_catalog(
    'fuzz_regress',
    'sqlite',
    '/tmp/pgiceberg_catalog_fuzz.db',
    '/tmp/pgiceberg_warehouse_fuzz'
  );

  PERFORM pgiceberg.create_table(
    'fuzz_regress',
    'default',
    'type_probe',
    ARRAY['id', 'big', 'price', 'ratio', 'amount', 'label', 'flag',
          'created', 'event_time', 'event_ts', 'event_tstz', 'payload',
          'token'],
    ARRAY[
      'integer'::regtype,
      'bigint'::regtype,
      'double precision'::regtype,
      'real'::regtype,
      'numeric'::regtype,
      'text'::regtype,
      'boolean'::regtype,
      'date'::regtype,
      'time'::regtype,
      'timestamp'::regtype,
      'timestamptz'::regtype,
      'bytea'::regtype,
      'uuid'::regtype
    ],
    ARRAY[true, false, false, false, false, false, false,
          false, false, false, false, false, false],
    true
  );

  PERFORM pgiceberg.create_table(
    'fuzz_regress',
    'default',
    'pruning_probe',
    ARRAY['id', 'category', 'amount'],
    ARRAY['bigint'::regtype, 'text'::regtype, 'double precision'::regtype],
    ARRAY[true, false, false],
    true
  );

  PERFORM pgiceberg.create_table(
    'fuzz_regress',
    'default',
    'trip_fixture',
    ARRAY['vendorid', 'passenger_count', 'trip_distance', 'store_and_fwd_flag'],
    ARRAY[
      'bigint'::regtype,
      'bigint'::regtype,
      'double precision'::regtype,
      'text'::regtype
    ],
    ARRAY[true, false, false, false],
    true
  );

  PERFORM pgiceberg.create_table(
    'fuzz_regress',
    'default',
    'type_fixture',
    ARRAY['id', 'amount', 'event_time', 'event_ts', 'payload', 'token'],
    ARRAY[
      'integer'::regtype,
      'numeric'::regtype,
      'time'::regtype,
      'timestamp'::regtype,
      'bytea'::regtype,
      'uuid'::regtype
    ],
    ARRAY[true, false, false, false, false, false],
    true
  );

  PERFORM pgiceberg.create_table(
    'fuzz_regress',
    'default',
    'range_fixture',
    ARRAY['small_value'],
    ARRAY['integer'::regtype],
    ARRAY[false],
    true
  );
END $$;

CREATE SERVER fuzz_iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog 'fuzz_regress');

CREATE FOREIGN TABLE type_probe (
  id integer,
  big bigint,
  price double precision,
  ratio real,
  amount numeric,
  label text,
  flag boolean,
  created date,
  event_time time,
  event_ts timestamp,
  event_tstz timestamptz,
  payload bytea,
  token uuid
)
SERVER fuzz_iceberg
OPTIONS (namespace 'default', table 'type_probe');

CREATE FOREIGN TABLE pruning_probe (
  id bigint,
  category text,
  amount double precision
)
SERVER fuzz_iceberg
OPTIONS (namespace 'default', table 'pruning_probe');

CREATE FOREIGN TABLE trip_fixture (
  vendorid bigint,
  passenger_count bigint,
  trip_distance double precision,
  store_and_fwd_flag text
)
SERVER fuzz_iceberg
OPTIONS (namespace 'default', table 'trip_fixture');

CREATE FOREIGN TABLE type_fixture (
  id integer,
  amount numeric,
  event_time time,
  event_ts timestamp,
  payload bytea,
  token uuid
)
SERVER fuzz_iceberg
OPTIONS (namespace 'default', table 'type_fixture');

CREATE FOREIGN TABLE range_fixture (
  small_value integer
)
SERVER fuzz_iceberg
OPTIONS (namespace 'default', table 'range_fixture');

INSERT INTO type_probe (id, label, flag)
VALUES (1, 'alpha', true);

INSERT INTO pruning_probe VALUES (1, 'alpha', 10.0);
INSERT INTO pruning_probe VALUES (2, 'beta', 20.0);
INSERT INTO pruning_probe VALUES (3, NULL, 30.0);

INSERT INTO trip_fixture
VALUES
  (1, 2, 3.5, 'N'),
  (2, 1, 8.25, 'Y'),
  (3, NULL, 0.75, NULL);

INSERT INTO type_fixture
VALUES (
  1,
  99,
  TIME '12:34:56.123456',
  TIMESTAMP '2026-07-07 08:09:10.123456',
  decode('000102ff', 'hex'),
  '11111111-2222-3333-4444-555555555555'
);

INSERT INTO range_fixture VALUES (7);
INSERT INTO range_fixture VALUES (-3);

CREATE SCHEMA fuzz_native;

SET pgiceberg.default_catalog = 'fuzz_regress';
SET pgiceberg.default_namespace = 'native';

CREATE TABLE fuzz_native.native_trip (
  vendorid bigint NOT NULL,
  passenger_count bigint,
  trip_distance double precision,
  store_and_fwd_flag text
) USING iceberg;

INSERT INTO fuzz_native.native_trip
VALUES
  (10, 2, 3.5, 'N'),
  (11, 1, 8.25, 'Y'),
  (12, NULL, 0.75, NULL);

CREATE TABLE fuzz_native.native_types (
  id integer NOT NULL,
  amount numeric,
  event_ts timestamp,
  payload bytea
) USING iceberg;

INSERT INTO fuzz_native.native_types
VALUES (
  1,
  99,
  TIMESTAMP '2026-07-07 08:09:10.123456',
  decode('aabbcc', 'hex')
);

RESET pgiceberg.default_catalog;
RESET pgiceberg.default_namespace;

SELECT pgiceberg.register_table_from_location(
  'fuzz_regress',
  'default',
  'yellow_trip',
  '/tmp/pgiceberg_yellow_trip_dataset_fuzz/default/yellow_trip',
  true
);

CREATE SERVER yellow_trip_server
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog 'fuzz_regress');

CREATE SCHEMA fuzz_dataset;

IMPORT FOREIGN SCHEMA "default"
LIMIT TO (yellow_trip)
FROM SERVER yellow_trip_server
INTO fuzz_dataset;

CREATE SERVER parquet_server
FOREIGN DATA WRAPPER pgiceberg_parquet
OPTIONS (
  dirname '/tmp/pgiceberg_yellow_trip_dataset_fuzz/default/yellow_trip/data'
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
  filename '/tmp/pgiceberg_yellow_trip_dataset_fuzz/default/yellow_trip/data/00000-0-81fc6f9d-c034-4d6b-ad00-895e23ab2189.parquet'
);

CREATE SERVER avro_server
FOREIGN DATA WRAPPER pgiceberg_avro;

CREATE FOREIGN TABLE avro_manifest_entries (
  status integer,
  snapshot_id bigint,
  sequence_number bigint,
  file_sequence_number bigint
)
SERVER avro_server
OPTIONS (
  filename '/tmp/pgiceberg_yellow_trip_dataset_fuzz/default/yellow_trip/metadata/81fc6f9d-c034-4d6b-ad00-895e23ab2189-m0.avro'
);
