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

SELECT pgiceberg.add_catalog(
  'rest_regress',
  'rest',
  'http://127.0.0.1:8181',
  '/tmp/pgiceberg_rest_warehouse'
);

SELECT catalog_type
FROM pgiceberg.catalogs
WHERE name = 'rest_regress';

SELECT pgiceberg.create_table(
  'rest_regress',
  'default',
  'rest_trip',
  ARRAY['vendorid', 'passenger_count'],
  ARRAY['bigint'::regtype, 'bigint'::regtype],
  ARRAY[true, false],
  true
);

CREATE SERVER rest_iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog 'rest_regress');

CREATE FOREIGN TABLE pgiceberg_rest_trip (
  vendorid bigint,
  passenger_count bigint
)
SERVER rest_iceberg
OPTIONS (
  namespace 'default',
  table 'rest_trip'
);

SELECT vendorid, passenger_count
FROM pgiceberg_rest_trip
ORDER BY vendorid;

INSERT INTO pgiceberg_rest_trip
VALUES (1, 2), (3, 4)
RETURNING vendorid, passenger_count;

SELECT vendorid, passenger_count
FROM pgiceberg_rest_trip
ORDER BY vendorid;

SELECT count(*) AS row_count
FROM pgiceberg_rest_trip;

\set VERBOSITY default

DROP FOREIGN TABLE pgiceberg_rest_trip;
DROP SERVER rest_iceberg;
DROP EXTENSION pgiceberg;
