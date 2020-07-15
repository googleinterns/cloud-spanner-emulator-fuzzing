

cc_binary(
  name = "simple_fuzz_test",
  srcs = [
    "simple_fuzz_test.cc", 
    "oss_fuzz.h"
  ],
  linkopts = [ "$(LIB_FUZZING_ENGINE)" ],
  deps = [
    "@com_google_cloud_spanner_emulator//frontend/server",
    "@com_github_googleapis_google_cloud_cpp_spanner//google/cloud/spanner:spanner_client",
    "@com_google_absl//absl/strings:strings",
    "@com_google_zetasql//zetasql/base:logging",
  ]
)

cc_library(
  name = "spanner_emulator_ddl_statement_to_string",
  srcs = ["protobufs/utils/spanner_emulator_ddl_statement_proto_to_string.cc",],
  deps = [
    ":spanner_emulator_ddl_statement_cc_proto",
    "@com_google_absl//absl/strings:strings",
    ]
)

cc_proto_library(
  name = "spanner_emulator_ddl_statement_cc_proto",
  deps = [":spanner_emulator_ddl_statement_proto",]
)

proto_library(
  name = "spanner_emulator_ddl_statement_proto",
  srcs = [
    "protobufs/spanner_ddl.proto",
    "protobufs/create_table.proto",
  ]
)