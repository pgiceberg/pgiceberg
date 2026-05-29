CREATE EXTENSION pgiceberg;

\pset format unaligned
\set VERBOSITY terse

DO $$
BEGIN
  PERFORM pgiceberg.create_table(
    'sqlite',
    '/tmp/pgiceberg_catalog_import_regress.db',
    '/tmp/pgiceberg_warehouse_import_regress',
    'default',
    'trip_fixture',
    ARRAY['vendorid', 'passenger_count', 'trip_distance', 'store_and_fwd_flag', 'fare_amount'],
    ARRAY['bigint'::regtype, 'bigint'::regtype, 'double precision'::regtype, 'text'::regtype, 'numeric'::regtype],
    ARRAY[true, false, false, false, false],
    true,
    'pgiceberg_regress'
  );
END $$;

CREATE SERVER iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (
  catalog_type 'sqlite',
  catalog_uri '/tmp/pgiceberg_catalog_import_regress.db',
  warehouse '/tmp/pgiceberg_warehouse_import_regress',
  catalog_name 'pgiceberg_regress'
);

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

IMPORT FOREIGN SCHEMA "default"
FROM SERVER iceberg
INTO public;

\set VERBOSITY default

DROP SCHEMA imported CASCADE;
DROP SERVER iceberg;
DROP EXTENSION pgiceberg;
