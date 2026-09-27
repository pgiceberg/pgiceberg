// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "common/catalog_config.h"
#include "common/pg_error.h"

#include <charconv>
#include <filesystem>
#include <system_error>

#include <iceberg/file_io_registry.h>

extern "C" {
#include "postgres.h"
#include "catalog/pg_foreign_server_d.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "libpq-fe.h"
}

namespace pgiceberg {
namespace {

struct Property {
  std::string_view option;
  std::string_view key;
  bool secret;
};

constexpr Property kProperties[] = {
    {"file_io", "io-impl", false},
    {"rest_auth_type", "rest.auth.type", false},
    {"rest_username", "rest.auth.basic.username", true},
    {"rest_password", "rest.auth.basic.password", true},
    {"rest_token", "token", true},
    {"rest_credential", "credential", true},
    {"rest_oauth2_server_uri", "oauth2-server-uri", false},
    {"rest_scope", "scope", false},
    {"rest_audience", "audience", false},
    {"rest_resource", "resource", false},
    {"rest_token_refresh_enabled", "token-refresh-enabled", false},
    {"rest_connect_timeout_ms", "rest.connect-timeout-ms", false},
    {"rest_request_timeout_ms", "rest.request-timeout-ms", false},
    {"rest_max_retries", "rest.max-retries", false},
    {"rest_retry_min_wait_ms", "rest.retry-min-wait-ms", false},
    {"rest_retry_max_wait_ms", "rest.retry-max-wait-ms", false},
    {"s3_access_key_id", "s3.access-key-id", true},
    {"s3_secret_access_key", "s3.secret-access-key", true},
    {"s3_session_token", "s3.session-token", true},
    {"s3_session_token_expires_at_ms", "s3.session-token-expires-at-ms", true},
    {"s3_region", "client.region", false},
    {"s3_endpoint", "s3.endpoint", false},
    {"s3_path_style_access", "s3.path-style-access", false},
    {"s3_connect_timeout_ms", "s3.connect-timeout-ms", false},
    {"s3_request_timeout_ms", "s3.socket-timeout-ms", false},
    {"s3_max_retries", "s3.max-retries", false},
    {"catalog_user", "pgiceberg.catalog-user", true},
    {"catalog_password", "pgiceberg.catalog-password", true},
    {"catalog_connect_timeout_ms", "pgiceberg.catalog-connect-timeout-ms", false},
    {"catalog_request_timeout_ms", "pgiceberg.catalog-request-timeout-ms", false},
};

const Property* FindProperty(std::string_view name) {
  for (const auto& property : kProperties) {
    if (property.option == name) return &property;
  }
  return nullptr;
}

Status InvalidProperty(std::string_view name) {
  // Never include values: the validator also handles passwords and tokens.
  return std::unexpected(
      MakeError(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE,
                "invalid pgiceberg option \"" + std::string(name) + "\" value"));
}

}  // namespace

bool IsCatalogProperty(std::string_view name, bool secret) {
  const auto* property = FindProperty(name);
  return property != nullptr && property->secret == secret;
}

Status ValidateCatalogProperty(std::string_view name, std::string_view value) {
  if (value.empty() || value.find_first_of("\r\n") != std::string_view::npos) {
    return InvalidProperty(name);
  }
  if (name == "file_io" && value != "auto" && value != "local" && value != "s3") {
    return InvalidProperty(name);
  }
  if (name == "rest_auth_type" && value != "none" && value != "basic" &&
      value != "oauth2") {
    return InvalidProperty(name);
  }
  if ((name == "s3_path_style_access" || name == "rest_token_refresh_enabled") &&
      value != "true" && value != "false") {
    return InvalidProperty(name);
  }
  if (name.ends_with("_timeout_ms") || name.ends_with("_wait_ms") ||
      name.ends_with("_max_retries")) {
    int timeout = 0;
    const auto [end, ec] =
        std::from_chars(value.data(), value.data() + value.size(), timeout);
    const int min = name.ends_with("_max_retries") ? 0 : 1;
    const int max = name.ends_with("_max_retries")
                        ? 10
                        : (name.ends_with("_wait_ms") ? 60000 : 3600000);
    if (ec != std::errc{} || end != value.data() + value.size() || timeout < min ||
        timeout > max) {
      return InvalidProperty(name);
    }
  }
  if (name == "s3_session_token_expires_at_ms") {
    int64_t expiry = 0;
    const auto [end, ec] =
        std::from_chars(value.data(), value.data() + value.size(), expiry);
    if (ec != std::errc{} || end != value.data() + value.size() || expiry <= 0) {
      return InvalidProperty(name);
    }
  }
  if (name == "rest_username" && value.contains(':')) return InvalidProperty(name);
  if (name == "rest_oauth2_server_uri" || name == "s3_endpoint") {
    PGICEBERG_RETURN_NOT_OK(ValidateCatalogUri(value));
    if (!value.starts_with("http://") && !value.starts_with("https://")) {
      return InvalidProperty(name);
    }
  }
  return Ok();
}

Status ApplyCatalogProperties(CatalogOptions& options, List* properties, bool secret) {
  ListCell* cell = nullptr;
  foreach (cell, properties) {
    auto* def = static_cast<DefElem*>(lfirst(cell));
    const auto* property = FindProperty(def->defname);
    if (property == nullptr || property->secret != secret) continue;
    const std::string value = defGetString(def);
    PGICEBERG_RETURN_NOT_OK(ValidateCatalogProperty(property->option, value));
    options.properties[std::string(property->key)] = value;
  }
  return Ok();
}

Status ValidateCatalogUri(std::string_view uri) {
  // Public server options and recovery records must never contain passwords.
  // libpq parses both PostgreSQL URI syntax and keyword/value conninfo.
  // Explicit filesystem paths can contain '=' without being libpq conninfo.
  if (uri.starts_with('/') || uri.starts_with("./") || uri.starts_with("../")) {
    return Ok();
  }
  if (uri.starts_with("postgresql://") || uri.starts_with("postgres://") ||
      (uri.find("://") == std::string_view::npos && uri.contains('='))) {
    char* error = nullptr;
    PQconninfoOption* info = PQconninfoParse(std::string(uri).c_str(), &error);
    if (info == nullptr) {
      PQfreemem(error);
      return InvalidProperty("catalog_uri");
    }
    bool has_password = false;
    for (auto* entry = info; entry->keyword != nullptr; ++entry) {
      if (std::string_view(entry->keyword) == "password" && entry->val != nullptr &&
          entry->val[0] != '\0')
        has_password = true;
    }
    PQconninfoFree(info);
    if (!has_password) return Ok();
  } else {
    auto authority = uri.find("://");
    if (authority == std::string_view::npos) {
      // The SQL connector also accepts scheme-less user:password@host/db URIs.
      const auto at = uri.find('@');
      const auto colon = uri.find(':');
      if (at == std::string_view::npos || colon == std::string_view::npos || colon > at ||
          uri.substr(0, at).contains('/'))
        return Ok();
      return std::unexpected(MakeError(
          ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE,
          "pgiceberg connection URI must not contain credentials or query parameters",
          "Store credentials in a PostgreSQL USER MAPPING."));
    }
    auto end = uri.find_first_of("/?#", authority + 3);
    if (!uri.substr(authority + 3, end - (authority + 3)).contains('@') &&
        !uri.contains('?') && !uri.contains('#'))
      return Ok();
  }
  return std::unexpected(MakeError(
      ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE,
      "pgiceberg connection URI must not contain credentials or query parameters",
      "Store credentials in a PostgreSQL USER MAPPING."));
}

Result<CatalogOptions> ResolveCatalogCredentials(const CatalogOptions& source) {
  CatalogOptions options = source;
  PGICEBERG_RETURN_NOT_OK(ValidateCatalogUri(options.catalog_uri));
  PGICEBERG_RETURN_NOT_OK(ValidateCatalogUri(options.warehouse));
  if (!options.credential_server.empty()) {
    ForeignServer* server =
        GetForeignServerByName(options.credential_server.c_str(), false);
    // GetOuterUserId honors SET ROLE, including calls through SECURITY DEFINER
    // helpers. Using the extension owner's mapping would cross a trust boundary.
    const Oid user = GetOuterUserId();
    if (object_aclcheck(ForeignServerRelationId, server->serverid, user, ACL_USAGE) !=
        ACLCHECK_OK) {
      return std::unexpected(
          MakeError(ERRCODE_INSUFFICIENT_PRIVILEGE,
                    "permission denied for pgiceberg credential server"));
    }
    const auto* wrapper = GetForeignDataWrapper(server->fdwid);
    if (std::string_view(wrapper->fdwname) != "pgiceberg") {
      return std::unexpected(
          MakeError(ERRCODE_FDW_INVALID_OPTION_NAME,
                    "credential server must use the pgiceberg foreign data wrapper"));
    }
    PGICEBERG_RETURN_NOT_OK(ApplyCatalogProperties(options, server->options, false));
    // PostgreSQL's normal per-user / PUBLIC fallback. No process-wide cache.
    const bool mapped =
        SearchSysCacheExists2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(user),
                              ObjectIdGetDatum(server->serverid)) ||
        SearchSysCacheExists2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(InvalidOid),
                              ObjectIdGetDatum(server->serverid));
    if (mapped || options.credential_mapping_required) {
      UserMapping* mapping = GetUserMapping(user, server->serverid);
      PGICEBERG_RETURN_NOT_OK(ApplyCatalogProperties(options, mapping->options, true));
    }
  }
  auto& p = options.properties;
  const auto has = [&](const char* key) { return p.contains(key) && !p.at(key).empty(); };
  if (options.catalog_type == "sql" && !superuser_arg(GetOuterUserId()) &&
      (!has("pgiceberg.catalog-user") || !has("pgiceberg.catalog-password"))) {
    return std::unexpected(MakeError(ERRCODE_INSUFFICIENT_PRIVILEGE,
                                     "SQL catalog access by non-superusers requires a "
                                     "USER MAPPING user and password"));
  }
  if (has("s3.access-key-id") != has("s3.secret-access-key") ||
      (has("s3.session-token") && !has("s3.access-key-id"))) {
    return std::unexpected(
        MakeError(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE,
                  "S3 credentials require both access key ID and secret access key"));
  }
  p.try_emplace("rest.auth.type", "none");
  if (p["rest.auth.type"] == "basic" &&
      (!has("rest.auth.basic.username") || !has("rest.auth.basic.password"))) {
    return std::unexpected(MakeError(
        ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE,
        "REST Basic authentication requires a USER MAPPING username and password"));
  }
  if (p["rest.auth.type"] == "oauth2" && !has("token") && !has("credential")) {
    return std::unexpected(MakeError(
        ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE,
        "REST OAuth2 authentication requires a USER MAPPING token or credential"));
  }
  if (p["rest.auth.type"] == "oauth2" && has("credential") && !has("oauth2-server-uri")) {
    return std::unexpected(
        MakeError(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE,
                  "REST OAuth2 client credentials require rest_oauth2_server_uri"));
  }
  // A missing mapping must not silently use the PostgreSQL process's AWS role.
  // REST may vend scoped credentials after loading the table; until then these
  // deliberately unusable credentials prevent the ambient provider chain.
  if (!has("s3.access-key-id")) {
    p["s3.access-key-id"] = "pgiceberg-unconfigured";
    p["s3.secret-access-key"] = "pgiceberg-unconfigured";
  }
  return options;
}

bool IsLocalLocation(std::string_view location) {
  return location.find("://") == std::string_view::npos ||
         location.starts_with("file://");
}

Result<std::string> FileIOImplementation(const CatalogOptions& options) {
  std::string requested = "auto";
  if (auto it = options.properties.find("io-impl"); it != options.properties.end()) {
    requested = it->second;
  }
  const auto& location = options.warehouse;
  const bool s3 = location.starts_with("s3://") || location.starts_with("s3a://") ||
                  location.starts_with("s3n://") || location.starts_with("oss://");
  if (requested == "s3" && !s3 && options.catalog_type != "rest") {
    return std::unexpected(
        MakeError(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE,
                  "warehouse location is incompatible with the selected FileIO"));
  }
  if (requested == "s3" || (requested == "auto" && s3)) {
#ifdef PGICEBERG_ENABLE_S3
    return std::string(iceberg::FileIORegistry::kArrowS3FileIO);
#else
    return std::unexpected(MakeError(ERRCODE_FEATURE_NOT_SUPPORTED,
                                     "pgiceberg S3 support is not enabled",
                                     "Rebuild with -DPGICEBERG_ENABLE_S3=ON."));
#endif
  }
  if (s3 || !IsLocalLocation(location)) {
    return std::unexpected(
        MakeError(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE,
                  "warehouse location is incompatible with the selected FileIO"));
  }
  return std::string(iceberg::FileIORegistry::kArrowLocalFileIO);
}

std::string JoinLocation(std::string_view base, std::string_view child) {
  while (base.ends_with('/')) base.remove_suffix(1);
  while (child.starts_with('/')) child.remove_prefix(1);
  return std::string(base) + "/" + std::string(child);
}

Status CreateLocalDirectory(std::string_view location) {
  if (!IsLocalLocation(location)) return Ok();
  if (location.starts_with("file://")) location.remove_prefix(7);
  std::error_code error;
  std::filesystem::create_directories(location, error);
  if (error) {
    return std::unexpected(
        MakeError(ERRCODE_IO_ERROR, "could not create local Iceberg directory"));
  }
  return Ok();
}

}  // namespace pgiceberg

extern "C" {
PG_FUNCTION_INFO_V1(pgiceberg_validate_catalog_uri);

Datum pgiceberg_validate_catalog_uri(PG_FUNCTION_ARGS) {
  pgiceberg::PgStatusGuard([&]() {
    return pgiceberg::ValidateCatalogUri(text_to_cstring(PG_GETARG_TEXT_PP(0)));
  });
  PG_RETURN_VOID();
}
}
