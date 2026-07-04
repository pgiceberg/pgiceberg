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

SELECT fdwname
FROM pg_foreign_data_wrapper
WHERE fdwname = 'pgiceberg';

\set VERBOSITY terse

CREATE SERVER invalid_option
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (unknown_option 'value');

CREATE SERVER invalid_catalog_type
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog_type 'invalid');

CREATE SERVER rest_catalog
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog_type 'rest');

SELECT pgiceberg.add_catalog(
  'future_catalog_regress',
  'rest',
  'http://127.0.0.1:8181',
  '/tmp/pgiceberg_warehouse_future_catalog_regress'
);

SELECT catalog_type
FROM pgiceberg.catalogs
WHERE name = 'future_catalog_regress';

DO $$
BEGIN
  PERFORM pgiceberg.add_catalog(
    'format_regress',
    'sqlite',
    '/tmp/pgiceberg_catalog_format_regress.db',
    '/tmp/pgiceberg_warehouse_format_regress'
  );

  PERFORM pgiceberg.create_table(
    'format_regress',
    'default',
    'format_v3',
    ARRAY['id'],
    ARRAY['bigint'::regtype],
    ARRAY[true],
    true,
    3
  );
END $$;

SELECT (pg_read_file('/tmp/pgiceberg_warehouse_format_regress/default/format_v3/metadata/' || metadata_file)::jsonb ->> 'format-version')::integer AS format_version
FROM pg_ls_dir('/tmp/pgiceberg_warehouse_format_regress/default/format_v3/metadata') AS metadata_file
WHERE metadata_file LIKE '00000-%.metadata.json'
ORDER BY metadata_file
LIMIT 1;

SELECT pgiceberg.create_table(
  'format_regress',
  'default',
  'format_v1',
  ARRAY['id'],
  ARRAY['bigint'::regtype],
  ARRAY[true],
  true,
  1
);

\set VERBOSITY default

DROP EXTENSION pgiceberg;
