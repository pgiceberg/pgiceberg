CREATE EXTENSION pgiceberg;

\pset format unaligned
\set VERBOSITY terse

DO $$
BEGIN
  PERFORM pgiceberg.create_table(
    'sqlite',
    '/tmp/pgiceberg_catalog_crud_regress.db',
    '/tmp/pgiceberg_warehouse_crud_regress',
    'default',
    'trip_fixture',
    ARRAY['vendorid', 'passenger_count', 'trip_distance', 'store_and_fwd_flag'],
    ARRAY['bigint'::regtype, 'bigint'::regtype, 'double precision'::regtype, 'text'::regtype],
    ARRAY[true, false, false, false],
    true,
    'pgiceberg_regress'
  );
END $$;

CREATE SERVER iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (
  catalog_type 'sqlite',
  catalog_uri '/tmp/pgiceberg_catalog_crud_regress.db',
  warehouse '/tmp/pgiceberg_warehouse_crud_regress',
  catalog_name 'pgiceberg_regress'
);

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

UPDATE pgiceberg_trip_fixture
SET passenger_count = passenger_count + 10,
    store_and_fwd_flag = 'U'
WHERE vendorid = 2
RETURNING vendorid, passenger_count, trip_distance, store_and_fwd_flag;

SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM pgiceberg_trip_fixture
ORDER BY vendorid;

DELETE FROM pgiceberg_trip_fixture
WHERE vendorid = 1
RETURNING vendorid, passenger_count, trip_distance, store_and_fwd_flag;

SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM pgiceberg_trip_fixture
ORDER BY vendorid;

TRUNCATE pgiceberg_trip_fixture;

\set VERBOSITY default

DROP FOREIGN TABLE pgiceberg_trip_fixture;
DROP SERVER iceberg;
DROP EXTENSION pgiceberg;
