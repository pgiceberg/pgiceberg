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

\pset format unaligned
\set VERBOSITY terse
CREATE EXTENSION pgiceberg VERSION '0.1.0';
SELECT pgiceberg.add_catalog('upgrade_old', 'sqlite', '/tmp/pgiceberg_upgrade.db',
                            '/tmp/pgiceberg_upgrade_warehouse');
ALTER EXTENSION pgiceberg UPDATE TO '0.1.1';
SELECT extversion FROM pg_extension WHERE extname = 'pgiceberg';
SELECT name, iceberg_catalog_name, credential_server IS NULL AS no_credentials
FROM pgiceberg.catalogs WHERE name = 'upgrade_old';
SELECT pgiceberg.add_catalog('upgrade_old', 'sqlite', '/tmp/pgiceberg_upgrade.db',
                            '/tmp/pgiceberg_upgrade_warehouse', 'renamed');
CREATE SERVER upgrade_credentials FOREIGN DATA WRAPPER pgiceberg
OPTIONS (file_io 'local');
CREATE USER MAPPING FOR CURRENT_USER SERVER upgrade_credentials;
SELECT pgiceberg.add_catalog('upgrade_new', 'sqlite', '/tmp/pgiceberg_upgrade.db',
                            '/tmp/pgiceberg_upgrade_warehouse',
                            credential_server => 'upgrade_credentials');
SELECT name, iceberg_catalog_name, credential_server
FROM pgiceberg.catalogs ORDER BY name;
SELECT pgiceberg.add_catalog('upgrade_bad', 'rest',
                            'https://user:secret@example.invalid', '/tmp/warehouse');
SELECT count(*) FROM pgiceberg.catalogs WHERE name = 'upgrade_bad';
DROP USER MAPPING FOR CURRENT_USER SERVER upgrade_credentials;
DROP SERVER upgrade_credentials;
DROP EXTENSION pgiceberg;
