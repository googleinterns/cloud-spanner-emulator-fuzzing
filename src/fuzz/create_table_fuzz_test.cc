//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "libprotobuf_mutator/src/libfuzzer/libfuzzer_macro.h"

#include "src/fuzz/protobufs/create_table.pb.h"
#include "src/fuzz/protobufs/spanner_ddl.pb.h"
#include "src/fuzz/protobufs/utils/spanner_emulator_ddl_statement_proto_to_string.h"

#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include "src/fuzz/oss_fuzz.h"

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
using spanner_ddl::CreateTable;

const std::string server_address = "localhost:1234";

DEFINE_PROTO_FUZZER(const CreateTable& createTable) {
  #ifdef __OSS_FUZZ__
    static bool Initialized = spanner_emulator_fuzzer::DoOssFuzzInit();
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

    std::string createTableDDLStatement = toString(createTable);

    auto admin_client =
        DatabaseAdminClient(google::cloud::spanner::MakeDatabaseAdminConnection(
            emulator_connection));
    try {
        auto db_or =
        admin_client.CreateDatabase(database, {createTableDDLStatement})
            .get();
        if (!db_or) throw std::runtime_error(db_or.status().message());

    } catch (std::exception const& ex) {
        LOG(ERROR) << "Failed to create table with the following DDL statement:";
        LOG(ERROR) << createTableDDLStatement;
        return 1;
    }
    
    LOG(INFO) << "Created database [" << database << "]";
    LOG(INFO) << "Ran following statement: " << createTableDDLStatement;


    return 0;
  } catch (std::exception const& ex) {
    LOG(ERROR) << "Standard exception raised: " << ex.what();
    return 1;
  }

  server->Shutdown();

  return 0;  
}

