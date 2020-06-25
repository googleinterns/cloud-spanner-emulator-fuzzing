// Copyright 2019 Google LLC
//
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

#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <filesystem>

#include "zetasql/base/logging.h"
#include "frontend/server/server.h"
#include "google/cloud/spanner/client.h"
#include "google/cloud/spanner/create_instance_request_builder.h"
#include "google/cloud/spanner/database_admin_client.h"
#include "google/cloud/spanner/instance_admin_client.h"

using ::google::spanner::emulator::frontend::Server;
using ::google::cloud::future;
using ::google::cloud::StatusOr;
using ::google::cloud::spanner::DatabaseAdminClient;
using ::google::cloud::spanner::v0::ConnectionOptions;

#ifdef __OSS_FUZZ__
#define OVERWRITE 1

bool DoOssFuzzInit() {
  namespace fs = std::filesystem;
  fs::path originDir;
  try {
    originDir = fs::canonical(fs::read_symlink("/proc/self/exe")).parent_path();
  } catch (const std::exception& e) {
    return false;
  }
  // Timezone data in OSS-Fuzz is in a non-standard location. This allows asbl to know where it resides
  fs::path tzdataDir = originDir / "data/zoneinfo/";
  if (setenv("TZDIR", tzdataDir.c_str(), OVERWRITE)) {
    return false;
  }
  return true;
}
#endif

const std::string server_address = "localhost:1234";

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size) {
  #ifdef __OSS_FUZZ__
    static bool Initialized = DoOssFuzzInit();
    if (!Initialized) { std::abort(); }
  #endif

  Server::Options options;
  options.server_address = server_address;
  std::unique_ptr<Server> server = Server::Create(options);
  if (!server) {
    return EXIT_FAILURE;
  }

  LOG(INFO) << "Server Up, Executing Test Query";

  try {
    // This is the connection to the emulator.
    ConnectionOptions emulator_connection;
    emulator_connection.set_endpoint(server_address)
        .set_credentials(grpc::InsecureChannelCredentials());

    // First we create an instance to create our database on.
    google::cloud::spanner::Instance instance("emulator", "emulator");
    google::cloud::spanner::InstanceAdminClient instance_client(
        google::cloud::spanner::MakeInstanceAdminConnection(
            emulator_connection));

    auto instance_or = instance_client.CreateInstance(
            google::cloud::spanner::CreateInstanceRequestBuilder(instance,
                                                                 "emulator")
                .SetDisplayName("emulator")
                .SetNodeCount(1)
                .SetLabels({{"label-key", "label-value"}})
                .Build())
                .get();
    if (!instance_or) {
      throw std::runtime_error(instance_or.status().message());
    }
    LOG(INFO) << "Created instance [" << instance << "]";

    // Then we create a simple database.
    google::cloud::spanner::Database database(instance, "test-db");

    auto admin_client =
        DatabaseAdminClient(google::cloud::spanner::MakeDatabaseAdminConnection(
            emulator_connection));
    auto db_or =
        admin_client.CreateDatabase(database, {R"sdl(
          CREATE TABLE Singers (
              SingerId   INT64 NOT NULL,
              FirstName  STRING(1024),
              LastName   STRING(1024),
              SingerInfo BYTES(MAX)
          ) PRIMARY KEY (SingerId))sdl",
                                               R"sdl(
          CREATE TABLE Albums (
              SingerId     INT64 NOT NULL,
              AlbumId      INT64 NOT NULL,
              AlbumTitle   STRING(MAX)
          ) PRIMARY KEY (SingerId, AlbumId),
              INTERLEAVE IN PARENT Singers ON DELETE CASCADE)sdl"})
            .get();
    if (!db_or) throw std::runtime_error(db_or.status().message());
    LOG(INFO) << "Created database [" << database << "]";

    google::cloud::spanner::Client client(
        google::cloud::spanner::MakeConnection(database, emulator_connection));

    std::string query = absl::Substitute("INSERT INTO Singers (FirstName) VALUES ($0)", std::string((char*)Data, Size));

    client.ExecuteQuery(
        google::cloud::spanner::SqlStatement(query)
    );

    return 0;
  } catch (std::exception const& ex) {
    LOG(ERROR) << "Standard exception raised: " << ex.what();
    return 1;
  }

  server->Shutdown();

  return 0;  
}
