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

\! rm -f /tmp/pgiceberg_catalog_schema_binding_regress.db
\! rm -rf /tmp/pgiceberg_warehouse_schema_binding_regress

CREATE EXTENSION pgiceberg;

\pset format unaligned
\set VERBOSITY terse

DO $$
BEGIN
  PERFORM pgiceberg.add_catalog(
    'schema_binding_regress',
    'sqlite',
    '/tmp/pgiceberg_catalog_schema_binding_regress.db',
    '/tmp/pgiceberg_warehouse_schema_binding_regress'
  );

  PERFORM pgiceberg.create_table(
    'schema_binding_regress',
    'default',
    'trips',
    ARRAY['vendorid', 'passenger_count', 'trip_distance'],
    ARRAY['bigint'::regtype, 'bigint'::regtype, 'double precision'::regtype],
    ARRAY[true, false, false],
    true
  );
END $$;

CREATE SERVER iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog 'schema_binding_regress');

IMPORT FOREIGN SCHEMA "default"
LIMIT TO (trips)
FROM SERVER iceberg
INTO public;

SELECT att.attname, opt.option_value AS field_id
FROM pg_attribute AS att
JOIN LATERAL pg_options_to_table(att.attfdwoptions) AS opt(option_name, option_value)
  ON opt.option_name = 'field_id'
WHERE att.attrelid = 'trips'::regclass
  AND att.attnum > 0
  AND NOT att.attisdropped
ORDER BY att.attnum;

INSERT INTO trips VALUES (1, 2, 3.5);
SELECT vendorid, passenger_count, trip_distance FROM trips ORDER BY vendorid;

ALTER FOREIGN TABLE trips RENAME COLUMN passenger_count TO riders;

SELECT vendorid, riders, trip_distance FROM trips ORDER BY vendorid;

SELECT change, local_column, iceberg_field_id, iceberg_name
FROM pgiceberg.schema_diff('trips')
ORDER BY iceberg_field_id, change;

SELECT pgiceberg.refresh_schema('trips');

SELECT att.attname
FROM pg_attribute AS att
WHERE att.attrelid = 'trips'::regclass
  AND att.attnum > 0
  AND NOT att.attisdropped
ORDER BY att.attnum;

SELECT change FROM pgiceberg.schema_diff('trips');

SELECT pgiceberg.update_schema(
  'schema_binding_regress',
  'default',
  'trips',
  ARRAY['extra_flag'],
  ARRAY['text'::regtype],
  ARRAY[]::text[],
  ARRAY[]::text[],
  ARRAY[]::text[]
);

SELECT change, iceberg_name, iceberg_field_id
FROM pgiceberg.schema_diff('trips')
ORDER BY iceberg_field_id, change;

INSERT INTO trips VALUES (2, 1, 8.25);
SELECT vendorid, passenger_count, trip_distance FROM trips ORDER BY vendorid;

SELECT pgiceberg.refresh_schema('trips');

SELECT att.attname, opt.option_value AS field_id
FROM pg_attribute AS att
JOIN LATERAL pg_options_to_table(att.attfdwoptions) AS opt(option_name, option_value)
  ON opt.option_name = 'field_id'
WHERE att.attrelid = 'trips'::regclass
  AND att.attnum > 0
  AND NOT att.attisdropped
ORDER BY att.attnum;

SELECT pgiceberg.update_schema(
  'schema_binding_regress',
  'default',
  'trips',
  ARRAY[]::text[],
  ARRAY[]::regtype[],
  ARRAY[]::text[],
  ARRAY['extra_flag'],
  ARRAY['extra']
);

SELECT vendorid, extra_flag FROM trips ORDER BY vendorid;

SELECT change, local_column, iceberg_name
FROM pgiceberg.schema_diff('trips')
WHERE change = 'renamed';

SELECT pgiceberg.refresh_schema('trips');

SELECT att.attname
FROM pg_attribute AS att
WHERE att.attrelid = 'trips'::regclass
  AND att.attnum > 0
  AND NOT att.attisdropped
ORDER BY att.attnum;

SELECT pgiceberg.update_schema(
  'schema_binding_regress',
  'default',
  'trips',
  ARRAY[]::text[],
  ARRAY[]::regtype[],
  ARRAY['extra'],
  ARRAY[]::text[],
  ARRAY[]::text[]
);

SELECT change, local_column, iceberg_field_id
FROM pgiceberg.schema_diff('trips')
WHERE change = 'dropped';

SELECT pgiceberg.refresh_schema('trips');

SELECT att.attname
FROM pg_attribute AS att
WHERE att.attrelid = 'trips'::regclass
  AND att.attnum > 0
  AND NOT att.attisdropped
ORDER BY att.attnum;

CREATE FOREIGN TABLE bad_column_option (
  id bigint OPTIONS (unknown_option '1')
)
SERVER iceberg
OPTIONS (namespace 'default', table 'trips');

CREATE FOREIGN TABLE bad_table_field_id (
  id bigint
)
SERVER iceberg
OPTIONS (namespace 'default', table 'trips', field_id '1');

\set VERBOSITY default

DROP FOREIGN TABLE trips;
DROP SERVER iceberg;
DROP EXTENSION pgiceberg;
