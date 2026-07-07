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
SET DateStyle = 'ISO, YMD';
SET TIME ZONE 'UTC';

DO $$
BEGIN
  PERFORM pgiceberg.add_catalog(
    'crud_regress',
    'sqlite',
    '/tmp/pgiceberg_catalog_crud_regress.db',
    '/tmp/pgiceberg_warehouse_crud_regress'
  );

  PERFORM pgiceberg.create_table(
    'crud_regress',
    'default',
    'trip_fixture',
    ARRAY['vendorid', 'passenger_count', 'trip_distance', 'store_and_fwd_flag'],
    ARRAY['bigint'::regtype, 'bigint'::regtype, 'double precision'::regtype, 'text'::regtype],
    ARRAY[true, false, false, false],
    true
  );

  PERFORM pgiceberg.create_table(
    'crud_regress',
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
    'crud_regress',
    'default',
    'range_fixture',
    ARRAY['small_value'],
    ARRAY['integer'::regtype],
    ARRAY[false],
    true
  );

  PERFORM pgiceberg.create_table(
    'crud_regress',
    'default',
    'timestamp_ns_source',
    ARRAY['id', 'event_ts'],
    ARRAY['integer'::regtype, 'timestamp'::regtype],
    ARRAY[true, false],
    true,
    3
  );
END $$;

CREATE SERVER iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog 'crud_regress');

CREATE FOREIGN TABLE pgiceberg_trip_fixture (
  vendorid bigint,
  passenger_count bigint,
  trip_distance double precision,
  store_and_fwd_flag text
)
SERVER iceberg
OPTIONS (
  namespace 'default',
  table 'trip_fixture'
);

EXPLAIN (COSTS OFF)
SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM pgiceberg_trip_fixture;

SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM pgiceberg_trip_fixture
ORDER BY vendorid;

INSERT INTO pgiceberg_trip_fixture
VALUES
  (1, 2, 3.5, 'N'),
  (2, 1, 8.25, 'Y'),
  (3, NULL, 0.75, NULL)
RETURNING vendorid, passenger_count, trip_distance, store_and_fwd_flag;

SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM pgiceberg_trip_fixture
ORDER BY vendorid;

BEGIN;
INSERT INTO pgiceberg_trip_fixture
VALUES (4, 4, 4.0, 'R');
UPDATE pgiceberg_trip_fixture
SET passenger_count = 5
WHERE vendorid = 4;
SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM pgiceberg_trip_fixture
WHERE vendorid = 4;
ROLLBACK;

SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM pgiceberg_trip_fixture
ORDER BY vendorid;

UPDATE pgiceberg_trip_fixture
SET passenger_count = passenger_count + 10,
    store_and_fwd_flag = 'U'
WHERE vendorid = 2
RETURNING vendorid, passenger_count, trip_distance, store_and_fwd_flag;

SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM pgiceberg_trip_fixture
ORDER BY vendorid;

BEGIN;
UPDATE pgiceberg_trip_fixture
SET passenger_count = 99
WHERE vendorid = 3;
SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM pgiceberg_trip_fixture
ORDER BY vendorid;
ROLLBACK;

SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM pgiceberg_trip_fixture
ORDER BY vendorid;

DELETE FROM pgiceberg_trip_fixture
WHERE vendorid = 1
RETURNING vendorid, passenger_count, trip_distance, store_and_fwd_flag;

SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM pgiceberg_trip_fixture
ORDER BY vendorid;

BEGIN;
DELETE FROM pgiceberg_trip_fixture
WHERE vendorid = 2;
SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM pgiceberg_trip_fixture
ORDER BY vendorid;
ROLLBACK;

SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM pgiceberg_trip_fixture
ORDER BY vendorid;

TRUNCATE pgiceberg_trip_fixture;

CREATE FOREIGN TABLE pgiceberg_type_fixture (
  id integer,
  amount numeric,
  event_time time,
  event_ts timestamp,
  payload bytea,
  token uuid
)
SERVER iceberg
OPTIONS (
  namespace 'default',
  table 'type_fixture'
);

INSERT INTO pgiceberg_type_fixture
VALUES (
  1,
  12345,
  TIME '12:34:56.123456',
  TIMESTAMP '2026-07-07 08:09:10.123456',
  decode('000102ff', 'hex'),
  '11111111-2222-3333-4444-555555555555'
)
RETURNING id, amount, event_time, event_ts, payload, token;

SELECT id, amount, event_time, event_ts, payload, token
FROM pgiceberg_type_fixture
ORDER BY id;

UPDATE pgiceberg_type_fixture
SET amount = amount + 5,
    event_time = TIME '01:02:03.000004',
    payload = decode('abcd', 'hex')
WHERE token = '11111111-2222-3333-4444-555555555555'
RETURNING id, amount, event_time, event_ts, payload, token;

DELETE FROM pgiceberg_type_fixture
WHERE id = 1
RETURNING id, amount, event_time, event_ts, payload, token;

CREATE FOREIGN TABLE pgiceberg_range_fixture (
  small_value integer
)
SERVER iceberg
OPTIONS (
  namespace 'default',
  table 'range_fixture'
);

INSERT INTO pgiceberg_range_fixture
VALUES (32767), (32768);

CREATE FOREIGN TABLE pgiceberg_range_fixture_smallint (
  small_value smallint
)
SERVER iceberg
OPTIONS (
  namespace 'default',
  table 'range_fixture'
);

SELECT small_value
FROM pgiceberg_range_fixture_smallint
ORDER BY small_value;

SELECT pgiceberg.table_metadata_file_location(
  'crud_regress',
  'default',
  'timestamp_ns_source'
) AS timestamp_ns_metadata_file \gset

COPY (
  SELECT replace(
    pg_read_file(:'timestamp_ns_metadata_file'),
    '"type":"timestamp"',
    '"type":"timestamp_ns"'
  )
) TO '/tmp/pgiceberg_timestamp_ns.metadata.json';

SELECT pgiceberg.register_table(
  'crud_regress',
  'default',
  'timestamp_ns_fixture',
  '/tmp/pgiceberg_timestamp_ns.metadata.json',
  true
);

SELECT pgiceberg.table_metadata_json(
  'crud_regress',
  'default',
  'timestamp_ns_fixture'
) #>> '{schemas,0,fields,1,type}' AS event_ts_type;

CREATE FOREIGN TABLE pgiceberg_timestamp_ns_fixture (
  id integer,
  event_ts timestamp
)
SERVER iceberg
OPTIONS (
  namespace 'default',
  table 'timestamp_ns_fixture'
);

INSERT INTO pgiceberg_timestamp_ns_fixture
VALUES (1, TIMESTAMP '2026-07-07 08:09:10.123456')
RETURNING id, event_ts;

SELECT id, event_ts
FROM pgiceberg_timestamp_ns_fixture
ORDER BY id;

\set VERBOSITY default

DROP FOREIGN TABLE pgiceberg_timestamp_ns_fixture;
DROP FOREIGN TABLE pgiceberg_range_fixture_smallint;
DROP FOREIGN TABLE pgiceberg_range_fixture;
DROP FOREIGN TABLE pgiceberg_type_fixture;
DROP FOREIGN TABLE pgiceberg_trip_fixture;
DROP SERVER iceberg;
DROP EXTENSION pgiceberg;
