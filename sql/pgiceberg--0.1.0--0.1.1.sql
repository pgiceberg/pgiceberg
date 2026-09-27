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

\echo Use "ALTER EXTENSION pgiceberg UPDATE TO '0.1.1'" to load this file. \quit

ALTER TABLE pgiceberg.catalogs ADD COLUMN credential_server text;
COMMENT ON COLUMN pgiceberg.catalogs.credential_server IS
  'Foreign server holding connection properties and per-role USER MAPPING credentials; no secrets are stored here.';

-- The new optional argument preserves four- and five-argument SQL calls.
DROP FUNCTION pgiceberg.add_catalog(text, text, text, text, text);

CREATE FUNCTION pgiceberg.validate_catalog_uri(uri text)
RETURNS void
AS 'MODULE_PATHNAME', 'pgiceberg_validate_catalog_uri'
LANGUAGE C STRICT;

CREATE FUNCTION pgiceberg.add_catalog(
  name text,
  catalog_type text,
  catalog_uri text,
  warehouse text,
  iceberg_catalog_name text DEFAULT NULL,
  credential_server text DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM pgiceberg.validate_catalog_uri($3);
  PERFORM pgiceberg.validate_catalog_uri($4);
  INSERT INTO pgiceberg.catalogs (
    name,
    catalog_type,
    catalog_uri,
    warehouse,
    iceberg_catalog_name,
    credential_server
  )
  VALUES ($1, $2, $3, $4, COALESCE($5, $1), $6)
  ON CONFLICT ON CONSTRAINT catalogs_pkey DO UPDATE
  SET catalog_type = EXCLUDED.catalog_type,
      catalog_uri = EXCLUDED.catalog_uri,
      warehouse = EXCLUDED.warehouse,
      iceberg_catalog_name = EXCLUDED.iceberg_catalog_name,
      credential_server = EXCLUDED.credential_server;
END;
$$;

COMMENT ON FUNCTION pgiceberg.add_catalog(text, text, text, text, text, text) IS
  'Register or replace a local pgiceberg catalog name and its Iceberg catalog connection details.';
