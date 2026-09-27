<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Catalog, object storage, and credential isolation

All table loading, creation, registration, and deletion use `CreateCatalog`.
SQL and SQLite resolve FileIO through the same registry as REST. `file_io`
selects `auto`, `local`, or `s3`; `auto` chooses from the warehouse URI.
`PGICEBERG_ENABLE_S3` enables the upstream Arrow S3 implementation independently
of `PGICEBERG_ENABLE_REST_CATALOG`. Local directory creation accepts both paths
and `file://` URIs and never applies filesystem operations to S3 locations.

## Configuration boundary

Connection properties belong to a foreign server. Foreign table and IMPORT
options may select only `namespace`, `table`, and `snapshot_id`. In particular,
a table owner with server USAGE cannot redirect a connection to another endpoint
while retaining the server's mapped credentials.
Existing foreign tables with connection options must move those options to their
foreign server before using this version.

A direct foreign server uses its own USER MAPPING when present. A registered
catalog may name a `credential_server` (sixth optional `add_catalog` argument).
The latter requires a mapping and is also used by native tables, logical mirrors,
and SQL metadata/lifecycle helpers. The calling role must have USAGE on it.
The normal PostgreSQL per-role mapping followed by PUBLIC fallback applies.
`GetOuterUserId()` preserves SET ROLE across SECURITY DEFINER helper calls.
Mapping lookups happen at each catalog open; secrets are not cached globally.
Roles that read registered catalogs through foreign tables also need SELECT on
`pgiceberg.catalogs`, which contains connection metadata but no mapping secrets.
Pending transaction table keys also include the calling role and server reference.
Changing credential contexts while modifying the same physical table in one
transaction is rejected; commit or roll back first.

Secrets are accepted only in USER MAPPING:

| Options | Purpose |
| --- | --- |
| `catalog_user`, `catalog_password` | PostgreSQL SQL catalog connection |
| `rest_username`, `rest_password` | REST Basic authentication |
| `rest_token`, `rest_credential` | OAuth2 bearer token or `client_id:client_secret` |
| `s3_access_key_id`, `s3_secret_access_key`, `s3_session_token` | Object storage credentials |

Public server properties include `file_io`, `rest_auth_type` (`none`, `basic`,
`oauth2`), `rest_oauth2_server_uri`, `rest_scope`, `rest_audience`, `rest_resource`,
`rest_token_refresh_enabled`, `s3_region`, `s3_endpoint`, `s3_path_style_access`,
`s3_connect_timeout_ms`, `s3_request_timeout_ms`, `s3_max_retries`,
`rest_connect_timeout_ms`, `rest_request_timeout_ms`, `rest_max_retries`,
`rest_retry_min_wait_ms`, `rest_retry_max_wait_ms`, `catalog_connect_timeout_ms`,
and `catalog_request_timeout_ms`. A static temporary S3 mapping may also specify
`s3_session_token_expires_at_ms`; it then fails closed after expiry and a new
catalog open reads the current mapping. An OAuth2 client credential
requires an explicit token endpoint. Empty values, header line breaks, invalid
booleans, and nonpositive/out-of-range S3 timeouts are rejected without printing
values. Password-bearing catalog URIs are rejected before registry insertion.

S3 does not fall back to the PostgreSQL process's AWS credential chain. REST may
vend table credentials; its initial S3 client has deliberately unusable credentials
until configured credentials or a table-specific credential response replace them.
SQL/SQLite S3 access requires mapped credentials. Non-superuser SQL access requires
both a mapped database user and password, preventing libpq environment/password-file
fallback. Superusers retain the existing SQL connection configuration behavior.

Recovery log version 2 stores the credential-server reference and whether a mapping
is required, never the resolved properties. Version 1 remains readable. Repair
resolves current mappings under the
repair caller's identity, so credentials can rotate without rewriting recovery logs.
The small maintained SQL connector patch passes credentials as separate connection
properties, quotes libpq values (including spaces, quotes, and backslashes),
and suppresses connection exception text. Patch application is idempotent
and fails configuration if the pinned upstream source no longer matches.
CI dependency cache keys include the patch contents and cannot fall back to
another patch revision. After updating patches locally, use a fresh dependency
checkout if configuration reports that an earlier patched source no longer matches.

## Example

After installing the new extension files, upgrade each existing database before
using the new library:

```sql
ALTER EXTENSION pgiceberg UPDATE TO '0.1.1';
```

The upgrade preserves registered catalogs and adds a nullable credential-server
reference. New installations use the same upgrade path automatically.

```sql
CREATE SERVER lake_credentials FOREIGN DATA WRAPPER pgiceberg
OPTIONS (file_io 's3', s3_region 'us-east-1',
         s3_endpoint 'https://s3.example.com', s3_path_style_access 'true');
CREATE USER MAPPING FOR app_user SERVER lake_credentials
OPTIONS (s3_access_key_id '<access-key>', s3_secret_access_key '<secret>');
GRANT USAGE ON FOREIGN SERVER lake_credentials TO app_user;
SELECT pgiceberg.add_catalog(
  'lake', 'sqlite', '/srv/iceberg/catalog.db', 's3://warehouse/tables',
  credential_server => 'lake_credentials');
```

Provision mappings through an appropriately protected administrative connection.
USER MAPPING protects catalog visibility; it does not encrypt PostgreSQL backups
or prevent SQL statement logging by the administrator.

## Network and renewal behavior

REST defaults are a 5-second connection timeout, a 30-second request timeout,
3 retries, and exponential waits from 100 to 2000 milliseconds. Only GET, HEAD,
and OAuth token POSTs retry transient transport failures or HTTP 408, 429, 500,
502, 503, and 504. Authentication/authorization failures do not retry. Numeric
and HTTP-date Retry-After values are honored; a wait beyond the configured
maximum returns the error instead of retrying early or sleeping indefinitely.
Redirects are disabled. All initialization, catalog, OAuth, and credential
endpoint calls use the per-client policy. Mutation POST/DELETE calls have one
attempt; a transport failure maps to service unavailability and commit handlers
preserve the `CommitStateUnknown` outcome. Remote HTTP error messages and unknown
error types are discarded to prevent credential echoes in PostgreSQL diagnostics.
OAuth parsing/validation errors likewise do not quote response content.

S3 defaults are a 5-second connection timeout, a 30-second socket timeout, and
3 retries using the AWS standard retry strategy. The FileIO also rejects empty
credential pairs, tokens without keys, and invalid transport settings received
from REST configuration. The S3 request option maps to
Arrow/AWS socket/low-speed timeout semantics, including platform granularity;
it is not a wall-clock deadline for a continuously progressing multipart transfer.
SQL catalog connections default to 5 seconds and use a 30-second PostgreSQL
statement timeout with a 5-second lock timeout. The connection timeout is rounded
up to seconds by libpq. Mutating SQL catalog operations are not blindly replayed.

A REST `credentials.uri` supplies renewable, prefix-scoped S3 credentials.
The renewal endpoint must have the catalog's origin and uses the table auth
session. Legacy responses without an initial credential array fetch it before
opening table IO. Renewals install only access keys, secrets, session tokens,
and expiration times: they cannot redirect an existing filesystem to another
endpoint. Duplicate prefixes and malformed/expired renewal responses are rejected.

The AWS credentials provider stays attached to the filesystem used by open
streams. It refreshes shortly before expiry, including on range reads and
multipart writes after the Table handle has been released. A mutex coalesces
concurrent refreshes across prefixes. On renewal failure, still-valid credentials
may continue until expiry, with a one-second retry delay. Expired or missing
credentials produce an invalid explicit identity, preventing fallback to ambient
or anonymous credentials (including public buckets). Removed or changed prefixes
fail closed. Prefix matching respects path boundaries, so `events` does not match
`events-other`. Static USER MAPPING credentials are reread on the next catalog open;
only a renewable credential source can mint replacement tokens for active streams.
Each PostgreSQL backend finalizes the S3 subsystem before process exit so AWS
worker threads stop before C++ global objects are destroyed.

## Verification

The `catalog_config` SQL regression covers configuration validation, per-role and
PUBLIC mappings, SECURITY DEFINER calls, role switches, and permission revocation.
`extension_upgrade` checks the 0.1.0-to-0.1.1 migration, existing catalog rows,
legacy helper calls, and the new credential-server argument.

Run the default build's regression suite with:

```sh
ctest --test-dir build --output-on-failure
```

With both REST and S3 enabled, `catalog_network_policy` checks transport bounds
and credential validation. `catalog_network` starts a local REST/S3 protocol
fixture and covers retries, timeouts, redirects, redaction, Basic/OAuth2,
concurrent credential renewal, open streams, prefix isolation, and expiry.
On PostgreSQL 18 it also starts a temporary cluster for SQL/SQLite S3 DML,
passwords containing special characters, caller-specific REST authentication,
and recovery using current caller mappings.

```sh
ctest --test-dir build/catalog-0.4.0 --output-on-failure -R '^catalog_network(_policy)?$'
```

The REST CI job also runs `regress_rest` against the upstream REST catalog fixture.
The four maintained patches in `cmake_modules/patches/` apply to the official
iceberg-cpp 0.4.0 release. They provide behavior beyond that release and must be
reviewed and revalidated whenever the release baseline changes.
