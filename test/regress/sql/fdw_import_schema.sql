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
    'import_regress',
    'sqlite',
    '/tmp/pgiceberg_catalog_import_regress.db',
    '/tmp/pgiceberg_warehouse_import_regress'
  );

  PERFORM pgiceberg.create_table(
    'import_regress',
    'default',
    'trip_fixture',
    ARRAY['vendorid', 'passenger_count', 'trip_distance', 'store_and_fwd_flag', 'fare_amount'],
    ARRAY['bigint'::regtype, 'bigint'::regtype, 'double precision'::regtype, 'text'::regtype, 'numeric'::regtype],
    ARRAY[true, false, false, false, false],
    true
  );
END $$;

CREATE SERVER iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog 'import_regress');

CREATE SCHEMA imported;

IMPORT FOREIGN SCHEMA "default"
LIMIT TO (trip_fixture)
FROM SERVER iceberg
INTO imported;

SELECT foreign_table_schema, foreign_table_name
FROM information_schema.foreign_tables
WHERE foreign_table_schema = 'imported'
ORDER BY foreign_table_name;

SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'imported'
  AND table_name = 'trip_fixture'
ORDER BY ordinal_position;

SELECT column_name, numeric_precision, numeric_scale
FROM information_schema.columns
WHERE table_schema = 'imported'
  AND table_name = 'trip_fixture'
  AND column_name = 'fare_amount';

SELECT att.attname, opt.option_name, opt.option_value
FROM pg_attribute AS att
JOIN LATERAL pg_options_to_table(att.attfdwoptions) AS opt(option_name, option_value)
  ON true
WHERE att.attrelid = 'imported.trip_fixture'::regclass
  AND att.attnum > 0
  AND NOT att.attisdropped
ORDER BY att.attnum, opt.option_name;

IMPORT FOREIGN SCHEMA "default"
FROM SERVER iceberg
INTO public;

\set VERBOSITY default

DROP SCHEMA imported CASCADE;
DROP SERVER iceberg;
DROP EXTENSION pgiceberg;
