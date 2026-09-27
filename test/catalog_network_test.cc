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

#include <chrono>
#include <atomic>
#include <iostream>
#include <stdexcept>
#include <thread>

#include <arrow/filesystem/s3fs.h>
#include <iceberg/arrow/arrow_io_util.h>
#include <iceberg/arrow/arrow_register.h>
#include <iceberg/catalog.h>
#include <iceberg/catalog/rest/auth/auth_session.h>
#include <iceberg/catalog/rest/error_handlers.h>
#include <iceberg/catalog/rest/http_client.h>
#include <iceberg/catalog/rest/rest_catalog.h>
#include <iceberg/catalog/sql/sql_catalog.h>
#include <iceberg/file_io.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/type.h>

// The upstream S3 tests use this internal configuration entry point as well.
namespace iceberg::arrow {
Result<::arrow::fs::S3Options> ConfigureS3Options(
    const std::unordered_map<std::string, std::string>& properties);
}

namespace {
template <typename T>
T Require(iceberg::Result<T> value) {
  if (!value) throw std::runtime_error(value.error().message);
  if constexpr (!std::is_void_v<T>) return std::move(*value);
}
void Check(bool value, const char* message) {
  if (!value) throw std::runtime_error(message);
}

using Properties = std::unordered_map<std::string, std::string>;

void CheckPolicy() {
  using iceberg::rest::HttpClientOptions;
  Check(!HttpClientOptions::FromProperties({{"rest.max-retries", "11"}}),
        "unbounded HTTP retries accepted");
  Check(!HttpClientOptions::FromProperties({{"rest.connect-timeout-ms", "0"}}),
        "zero HTTP timeout accepted");
  Check(!HttpClientOptions::FromProperties({{"rest.retry-min-wait-ms", "2001"}}),
        "invalid backoff bounds accepted");
  auto configured =
      Require(HttpClientOptions::FromProperties({{"rest.connect-timeout-ms", "17"},
                                                 {"rest.request-timeout-ms", "29"},
                                                 {"rest.max-retries", "0"},
                                                 {"rest.retry-min-wait-ms", "2"},
                                                 {"rest.retry-max-wait-ms", "3"}}));
  Check(configured.connect_timeout_ms == 17 && configured.request_timeout_ms == 29 &&
            configured.max_retries == 0 && configured.retry_min_wait_ms == 2 &&
            configured.retry_max_wait_ms == 3,
        "HTTP client properties were not applied");

  using iceberg::arrow::ConfigureS3Options;
  const Properties credentials{{"s3.access-key-id", "policy-key"},
                               {"s3.secret-access-key", "policy-secret"},
                               {"client.region", "us-east-1"}};
  // Match the upstream test fixture: initialize AWS before configuring S3Options.
  // Creating the filesystem does not issue an object storage request.
  auto initialized = Require(iceberg::arrow::MakeS3FileIO(credentials));
  for (const auto* key : {"s3.connect-timeout-ms", "s3.socket-timeout-ms"}) {
    for (const auto* invalid :
         {"0", "-1", "nan", "inf", "3600001", "1.5", "do-not-leak-secret"}) {
      auto values = credentials;
      values[key] = invalid;
      auto rejected = ConfigureS3Options(values);
      Check(!rejected, "invalid S3 timeout accepted");
      Check(rejected.error().message.find("do-not-leak") == std::string::npos,
            "S3 validation echoed a response value");
    }
  }
  Check(!ConfigureS3Options({{"s3.max-retries", "11"}}), "unbounded S3 retries accepted");
  Check(!ConfigureS3Options({{"s3.access-key-id", ""}, {"s3.secret-access-key", ""}}),
        "empty S3 credentials could permit anonymous access");
  Check(!ConfigureS3Options({{"s3.session-token", "orphan-token"}}),
        "S3 session token could use ambient credentials");
  auto values = credentials;
  values["s3.connect-timeout-ms"] = "17";
  values["s3.socket-timeout-ms"] = "29";
  auto s3 = Require(ConfigureS3Options(values));
  Check(s3.connect_timeout == 0.017 && s3.request_timeout == 0.029,
        "S3 transport timeouts were not applied");
}

void Run(const std::string& endpoint, const std::string& sqlite_path) {
  CheckPolicy();
  iceberg::arrow::RegisterAll();
  Properties storage{
      {"client.region", "us-east-1"},          {"s3.endpoint", endpoint},
      {"s3.path-style-access", "true"},        {"s3.access-key-id", "test-key"},
      {"s3.secret-access-key", "test-secret"}, {"s3.max-retries", "0"}};
  std::shared_ptr<iceberg::FileIO> io = Require(iceberg::arrow::MakeS3FileIO(storage));
  auto sql = Require(
      iceberg::sql::SqlCatalog::MakeSqliteCatalog({.name = "network",
                                                   .uri = sqlite_path,
                                                   .warehouse_location = "s3://bucket",
                                                   .max_connections = 1},
                                                  io));
  iceberg::TableIdentifier identifier{.ns = {.levels = {"default"}}, .name = "events"};
  Require(sql->CreateNamespace(identifier.ns, {}));
  auto schema = std::make_shared<iceberg::Schema>(std::vector<iceberg::SchemaField>{
      iceberg::SchemaField::MakeRequired(1, "id", iceberg::int64())});
  auto table = Require(
      sql->CreateTable(identifier, schema, iceberg::PartitionSpec::Unpartitioned(),
                       iceberg::SortOrder::Unsorted(), "s3://bucket/default/events", {}));
  auto loaded = Require(sql->LoadTable(identifier));
  Check(loaded->metadata_file_location() == table->metadata_file_location(),
        "SQLite S3 catalog did not reload its metadata");
  const std::string blob = "s3://bucket/default/events/data/blob";
  Require(io->WriteFile(blob, "0123456789"));
  Check(Require(io->ReadFile(blob, std::nullopt)) == "0123456789",
        "S3 round trip failed");

  auto slow_storage = storage;
  slow_storage["s3.socket-timeout-ms"] = "1000";
  auto slow_io = Require(iceberg::arrow::MakeS3FileIO(slow_storage));
  auto s3_start = std::chrono::steady_clock::now();
  Check(!slow_io->ReadFile("s3://bucket/slow-s3", std::nullopt),
        "S3 socket timeout was not enforced");
  Check(std::chrono::steady_clock::now() - s3_start < std::chrono::seconds(3),
        "S3 timeout/retry bound exceeded");

  using namespace iceberg::rest;
  HttpClient client({}, {.connect_timeout_ms = 100,
                         .request_timeout_ms = 100,
                         .max_retries = 2,
                         .retry_min_wait_ms = 1,
                         .retry_max_wait_ms = 10});
  auto session = auth::AuthSession::MakeDefault({});
  Require(client.Get(endpoint + "/retry", {}, {}, *DefaultErrorHandler::Instance(),
                     *session));
  auto denied = client.Get(endpoint + "/forbidden", {}, {},
                           *DefaultErrorHandler::Instance(), *session);
  Check(!denied && denied.error().kind == iceberg::ErrorKind::kForbidden,
        "HTTP response body overrode the actual status");
  Check(denied.error().message.find("do-not-leak") == std::string::npos,
        "remote error text leaked credentials");
  Check(!client.Get(endpoint + "/redirect", {}, {}, *DefaultErrorHandler::Instance(),
                    *session),
        "HTTP redirect was followed");
  auto retry_start = std::chrono::steady_clock::now();
  Check(!client.Get(endpoint + "/retry-after", {}, {}, *DefaultErrorHandler::Instance(),
                    *session),
        "oversized Retry-After was ignored");
  Check(std::chrono::steady_clock::now() - retry_start < std::chrono::seconds(1),
        "Retry-After exceeded the wait budget");
  auto commit = client.Post(endpoint + "/commit", "{}", {},
                            *TableCommitErrorHandler::Instance(), *session);
  Check(!commit && commit.error().kind == iceberg::ErrorKind::kCommitStateUnknown,
        "ambiguous mutation lost commit-state-unknown classification");
  auto start = std::chrono::steady_clock::now();
  auto timeout =
      client.Get(endpoint + "/slow", {}, {}, *DefaultErrorHandler::Instance(), *session);
  Check(!timeout, "HTTP request timeout was not enforced");
  Check(std::chrono::steady_clock::now() - start < std::chrono::seconds(3),
        "HTTP retries were not bounded");

  auto invalid_oauth = RestCatalog::Make(
      RestCatalogProperties::FromMap({{"uri", endpoint + "/oauth"},
                                      {"warehouse", "s3://bucket"},
                                      {"rest.auth.type", "oauth2"},
                                      {"credential", "client:test-client-secret"},
                                      {"oauth2-server-uri", endpoint + "/bad-token"}}));
  Check(!invalid_oauth &&
            invalid_oauth.error().message.find("do-not-leak") == std::string::npos,
        "invalid OAuth response leaked remote credentials");

  for (const std::string mode : {"basic", "oauth"}) {
    Properties properties = storage;
    properties["uri"] = endpoint + "/" + mode;
    properties["warehouse"] = "s3://bucket";
    properties["s3.access-key-id"] = "pgiceberg-unconfigured";
    properties["s3.secret-access-key"] = "pgiceberg-unconfigured";
    properties["rest.auth.type"] = mode == "basic" ? "basic" : "oauth2";
    properties["rest.auth.basic.username"] = "reader";
    properties["rest.auth.basic.password"] = "test-password";
    if (mode == "oauth") {
      properties["credential"] = "client:test-client-secret";
      properties["oauth2-server-uri"] = endpoint + "/token";
      properties["token-refresh-enabled"] = "true";
    }
    auto root = Require(RestCatalog::Make(RestCatalogProperties::FromMap(properties)));
    auto rest = Require(root->AsCatalog());
    auto remote = Require(rest->LoadTable(identifier));
    auto table_io = remote->io();
    auto input = Require(table_io->NewInputFile(blob));
    auto stream = Require(input->Open());
    std::byte first;
    Require(stream->ReadFully(0, std::span(&first, 1)));
    Check(first == std::byte{'0'}, "initial vended credential read failed");
    auto output = Require(
        remote->io()->NewOutputFile("s3://bucket/default/events/data/renewed-" + mode));
    auto writer = Require(output->CreateOrOverwrite());
    remote.reset();
    rest.reset();
    root.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(1600));
    std::atomic<bool> parallel_ok{true};
    std::vector<std::thread> readers;
    for (int i = 0; i < 8; ++i) {
      readers.emplace_back([&] {
        auto read = table_io->ReadFile(blob, std::nullopt);
        if (!read || *read != "0123456789") parallel_ok = false;
      });
    }
    for (auto& reader : readers) reader.join();
    Check(parallel_ok, "concurrent credential renewal failed");
    // Both streams were opened before expiration. Their AWS credential providers
    // must renew on the next request, not only when a new FileIO is constructed.
    std::byte later;
    Require(stream->ReadFully(5, std::span(&later, 1)));
    Check(later == std::byte{'5'}, "open input stream did not survive credential expiry");
    Require(writer->Write(std::as_bytes(std::span("renewed", 7))));
    Require(writer->Close());
    Require(stream->Close());
    root = Require(RestCatalog::Make(RestCatalogProperties::FromMap(properties)));
    rest = Require(root->AsCatalog());
    remote = Require(rest->LoadTable(identifier));
    auto outside =
        remote->io()->ReadFile("s3://bucket/default/events-other/private", std::nullopt);
    Check(!outside, "storage credential prefix leaked into a sibling prefix");
    Require(remote->io()->AsSupportsStorageCredentials()->SetStorageCredentialRefresh(
        []() -> iceberg::Result<std::vector<iceberg::StorageCredential>> {
          return iceberg::IOError("credential service unavailable");
        }));
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    Check(!remote->io()->ReadFile(blob, std::nullopt),
          "expired credentials or ambient credentials reused after refresh failure");
  }
  Require(sql->DropTable(identifier, false));
  Require(io->DeleteFile(blob));
  std::cout << "catalog network checks passed\n";
}
}  // namespace

int main(int argc, char** argv) {
  if (argc == 2 && std::string_view(argv[1]) == "--validate-policy") {
    try {
      CheckPolicy();
      Require(iceberg::arrow::FinalizeS3());
    } catch (const std::exception& error) {
      std::cerr << error.what() << '\n';
      (void)iceberg::arrow::FinalizeS3();
      return 1;
    }
    std::cout << "network policy checks passed\n";
    return 0;
  }
  if (argc != 3) return 2;
  try {
    Run(argv[1], argv[2]);
    Require(iceberg::arrow::FinalizeS3());
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    (void)iceberg::arrow::FinalizeS3();
    return 1;
  }
  return 0;
}
