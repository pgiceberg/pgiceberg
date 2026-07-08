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

\! rm -f /tmp/pgiceberg_catalog_tableam_regress.db
\! rm -rf /tmp/pgiceberg_warehouse_tableam_regress

CREATE EXTENSION pgiceberg;

\pset format unaligned
\set VERBOSITY terse

SELECT amname
FROM pg_am
WHERE amname = 'iceberg';

CREATE TABLE missing_catalog (
  id bigint
) USING iceberg;

SELECT pgiceberg.add_catalog(
  'tableam_regress',
  'sqlite',
  '/tmp/pgiceberg_catalog_tableam_regress.db',
  '/tmp/pgiceberg_warehouse_tableam_regress'
);

CREATE TABLE native_with_options (
  id bigint
) USING iceberg
WITH (
  catalog = 'tableam_regress',
  namespace = 'native_options',
  table_name = 'native_with_options_iceberg',
  format_version = 2
);

SELECT catalog,
       namespace,
       table_name
FROM pgiceberg.table_bindings
WHERE relid = 'native_with_options'::regclass;

DROP TABLE native_with_options;

SET pgiceberg.default_catalog = 'tableam_regress';
SET pgiceberg.default_namespace = 'native';

CREATE TABLE native_trip (
  vendorid bigint NOT NULL,
  passenger_count bigint,
  trip_distance double precision,
  store_and_fwd_flag text
) USING iceberg;

SELECT am.amname
FROM pg_class cls
JOIN pg_am am ON am.oid = cls.relam
WHERE cls.oid = 'native_trip'::regclass;

SELECT catalog,
       namespace,
       table_name
FROM pgiceberg.table_bindings
WHERE relid = 'native_trip'::regclass;

SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM native_trip
ORDER BY vendorid;

INSERT INTO native_trip
VALUES
  (1, 2, 3.5, 'N'),
  (2, 1, 8.25, 'Y'),
  (3, NULL, 0.75, NULL)
RETURNING vendorid, passenger_count, trip_distance, store_and_fwd_flag;

SELECT vendorid, passenger_count, trip_distance, store_and_fwd_flag
FROM native_trip
ORDER BY vendorid;

UPDATE native_trip
SET passenger_count = 5
WHERE vendorid = 1;

DELETE FROM native_trip
WHERE vendorid = 1;

CREATE INDEX native_trip_vendorid_idx ON native_trip (vendorid);

TRUNCATE native_trip;

DROP TABLE native_trip;

SELECT count(*) AS remaining_bindings
FROM pgiceberg.table_bindings
WHERE table_name = 'native_trip';

CREATE TABLE native_trip (
  vendorid bigint
) USING iceberg;

DROP TABLE native_trip;

CREATE TABLE native_trip (
  vendorid bigint
) USING iceberg;

DROP TABLE native_trip;

BEGIN;
CREATE TABLE rollback_trip (
  vendorid bigint
) USING iceberg;
ROLLBACK;

CREATE TABLE rollback_trip (
  vendorid bigint
) USING iceberg;

ALTER TABLE rollback_trip ADD COLUMN passenger_count bigint;

CREATE TABLE ctas_trip
USING iceberg
AS SELECT 1::bigint AS vendorid;

SET default_table_access_method = iceberg;

CREATE TABLE ctas_default_trip
AS SELECT 1::bigint AS vendorid;

RESET default_table_access_method;

DROP TABLE rollback_trip;

BEGIN;
CREATE TABLE txn_trip (
  vendorid bigint
) USING iceberg;
DROP TABLE txn_trip;
COMMIT;

CREATE TABLE txn_trip (
  vendorid bigint
) USING iceberg;

DROP TABLE txn_trip;

CREATE FUNCTION create_cached_trip() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  CREATE TABLE cached_trip (
    vendorid bigint
  ) USING iceberg
  WITH (namespace = 'cached', table_name = 'cached_trip_iceberg');
END $$;

SELECT create_cached_trip();

SELECT namespace, table_name
FROM pgiceberg.table_bindings
WHERE relid = 'cached_trip'::regclass;

DROP TABLE cached_trip;

SELECT create_cached_trip();

SELECT namespace, table_name
FROM pgiceberg.table_bindings
WHERE relid = 'cached_trip'::regclass;

DROP TABLE cached_trip;
DROP FUNCTION create_cached_trip();

\set VERBOSITY default

DROP EXTENSION pgiceberg;
