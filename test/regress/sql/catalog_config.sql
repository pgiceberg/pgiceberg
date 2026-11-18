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
\! rm -f /tmp/pgiceberg_catalog_config=regress.db
\! rm -rf /tmp/pgiceberg_warehouse_config=regress

CREATE SERVER config_bad FOREIGN DATA WRAPPER pgiceberg OPTIONS (rest_password 'hidden');
CREATE SERVER config_bad FOREIGN DATA WRAPPER pgiceberg OPTIONS (rest_auth_type 'invalid');
CREATE SERVER config_bad FOREIGN DATA WRAPPER pgiceberg OPTIONS (s3_connect_timeout_ms '0');
CREATE SERVER config_bad FOREIGN DATA WRAPPER pgiceberg OPTIONS (s3_path_style_access 'yes');
CREATE SERVER config_bad FOREIGN DATA WRAPPER pgiceberg OPTIONS (catalog_uri 'postgresql://user:hidden@localhost/db');
SELECT pgiceberg.add_catalog('config_bad', 'rest', 'https://user:hidden@example.invalid', '/tmp/unused');
SELECT count(*) FROM pgiceberg.catalogs WHERE name = 'config_bad';

CREATE SERVER config_credentials FOREIGN DATA WRAPPER pgiceberg OPTIONS (file_io 'local');
CREATE USER MAPPING FOR CURRENT_USER SERVER config_credentials
OPTIONS (s3_access_key_id 'owner-key', s3_secret_access_key 'owner-secret');
CREATE USER MAPPING FOR PUBLIC SERVER config_credentials OPTIONS (s3_access_key_id 'incomplete');
CREATE ROLE config_reader;
GRANT USAGE ON SCHEMA pgiceberg TO config_reader;
GRANT SELECT ON pgiceberg.catalogs TO config_reader;
GRANT USAGE ON FOREIGN SERVER config_credentials TO config_reader;
CREATE USER MAPPING FOR config_reader SERVER config_credentials
OPTIONS (s3_access_key_id 'reader-key');
ALTER USER MAPPING FOR config_reader SERVER config_credentials OPTIONS (ADD catalog_uri 'invalid');

SELECT pgiceberg.add_catalog('config_regress', 'sqlite', '/tmp/pgiceberg_catalog_config=regress.db',
  'file:///tmp/pgiceberg_warehouse_config=regress', credential_server => 'config_credentials');
SELECT pgiceberg.create_table('config_regress', 'default', 'config_table',
  ARRAY['id'], ARRAY['bigint'::regtype], ARRAY[true], true, 3);
CREATE SERVER config_data FOREIGN DATA WRAPPER pgiceberg OPTIONS (catalog 'config_regress');
GRANT USAGE ON FOREIGN SERVER config_data TO config_reader;
CREATE FOREIGN TABLE config_table (id bigint) SERVER config_data;
GRANT SELECT, INSERT ON config_table TO config_reader;
CREATE FOREIGN TABLE config_bad_table (id bigint) SERVER config_data OPTIONS (s3_endpoint 'http://localhost:9000');
CREATE FOREIGN TABLE config_bad_table (id bigint) SERVER config_data OPTIONS (catalog_uri 'https://example.invalid');
IMPORT FOREIGN SCHEMA "default" FROM SERVER config_data INTO public OPTIONS (catalog_uri 'https://example.invalid', table 'config_table');
INSERT INTO config_table VALUES (1);

-- An incomplete caller mapping must fail, even through SECURITY DEFINER.
SET ROLE config_reader;
SELECT pgiceberg.table_format_version('config_regress', 'default', 'config_table');
SELECT count(*) FROM config_table;
RESET ROLE;
ALTER USER MAPPING FOR config_reader SERVER config_credentials OPTIONS (ADD s3_secret_access_key 'reader-secret');
SET ROLE config_reader;
SELECT pgiceberg.table_format_version('config_regress', 'default', 'config_table');
SELECT count(*) FROM config_table;
INSERT INTO config_table VALUES (2);
SELECT count(*) FROM config_table;
RESET ROLE;

-- Switching roles must not reuse another role's pending writer.
BEGIN;
INSERT INTO config_table VALUES (3);
SET LOCAL ROLE config_reader;
SAVEPOINT role_switch;
INSERT INTO config_table VALUES (4);
ROLLBACK TO role_switch;
RESET ROLE;
COMMIT;
SELECT count(*) FROM config_table;

-- Revocation takes effect on the next catalog open in the same backend.
REVOKE USAGE ON FOREIGN SERVER config_credentials FROM config_reader;
SET ROLE config_reader;
SELECT pgiceberg.table_format_version('config_regress', 'default', 'config_table');
RESET ROLE;
GRANT USAGE ON FOREIGN SERVER config_credentials TO config_reader;
DROP USER MAPPING FOR config_reader SERVER config_credentials;
SET ROLE config_reader;
SELECT pgiceberg.table_format_version('config_regress', 'default', 'config_table');
RESET ROLE;
ALTER USER MAPPING FOR PUBLIC SERVER config_credentials OPTIONS (ADD s3_secret_access_key 'public-secret');
SET ROLE config_reader;
SELECT pgiceberg.table_format_version('config_regress', 'default', 'config_table');
RESET ROLE;
DROP USER MAPPING FOR PUBLIC SERVER config_credentials;
SET ROLE config_reader;
DO $$ BEGIN
  BEGIN
    PERFORM pgiceberg.table_format_version('config_regress', 'default', 'config_table');
    RAISE EXCEPTION 'missing caller mapping was accepted';
  EXCEPTION WHEN undefined_object THEN
    RAISE NOTICE 'missing caller mapping rejected';
  END;
END $$;
RESET ROLE;

DROP FOREIGN TABLE config_table;
DROP SERVER config_data;
DROP USER MAPPING FOR CURRENT_USER SERVER config_credentials;
DROP SERVER config_credentials;
SELECT pgiceberg.drop_catalog('config_regress');
DROP OWNED BY config_reader;
DROP ROLE config_reader;

DROP EXTENSION pgiceberg;
