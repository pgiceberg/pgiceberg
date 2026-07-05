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
    'register_source_regress',
    'sqlite',
    '/tmp/pgiceberg_catalog_register_source_regress.db',
    '/tmp/pgiceberg_warehouse_register_source_regress'
  );

  PERFORM pgiceberg.add_catalog(
    'register_target_regress',
    'sqlite',
    '/tmp/pgiceberg_catalog_register_target_regress.db',
    '/tmp/pgiceberg_warehouse_register_target_regress'
  );

  PERFORM pgiceberg.create_table(
    'register_source_regress',
    'default',
    'trip_fixture',
    ARRAY['vendorid', 'passenger_count', 'trip_distance', 'store_and_fwd_flag'],
    ARRAY['bigint'::regtype, 'bigint'::regtype, 'double precision'::regtype, 'text'::regtype],
    ARRAY[true, false, false, false],
    true
  );
END $$;

SELECT pgiceberg.register_table_from_location(
  'register_target_regress',
  'default',
  'registered_trip_fixture',
  '/tmp/pgiceberg_warehouse_register_source_regress/default/trip_fixture',
  true
);

CREATE SERVER registered_iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog 'register_target_regress');

CREATE SCHEMA registered;

IMPORT FOREIGN SCHEMA "default"
LIMIT TO (registered_trip_fixture)
FROM SERVER registered_iceberg
INTO registered;

SELECT foreign_table_schema, foreign_table_name
FROM information_schema.foreign_tables
WHERE foreign_table_schema = 'registered'
ORDER BY foreign_table_name;

SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'registered'
  AND table_name = 'registered_trip_fixture'
ORDER BY ordinal_position;

\set VERBOSITY default

DROP SCHEMA registered CASCADE;
DROP SERVER registered_iceberg;
DROP EXTENSION pgiceberg;
