

cc_binary(
  name = "simple_fuzz_test",
  srcs = ["simple_fuzz_test.cc"],
  linkopts = [ "$(LIB_FUZZING_ENGINE)" ],
  deps = [
    "@com_google_cloud_spanner_emulator//frontend/server",
    "@com_github_googleapis_google_cloud_cpp_spanner//google/cloud/spanner:spanner_client",
    "@com_google_absl//absl/strings:strings",
    "@com_google_zetasql//zetasql/base:logging",
    ":oss_fuzz_init"
  ]
)

cc_binary(
  name = "create_table_fuzz_test",
  srcs = ["create_table_fuzz_test.cc"],
  linkopts = [ "$(LIB_FUZZING_ENGINE)" ],
  deps = [
    "@com_google_cloud_spanner_emulator//frontend/server",
    "@com_github_googleapis_google_cloud_cpp_spanner//google/cloud/spanner:spanner_client",
    "@com_google_absl//absl/strings:strings",
    "@com_google_zetasql//zetasql/base:logging",
    "@libprotobuf_mutator//:libprotobuf_mutator",
    ":spanner_emulator_ddl_statement_cc_proto",
    ":spanner_emulator_ddl_statement_to_string",
    ":oss_fuzz_init"
  ]
)

cc_test(
    name = "spanner_emulator_ddl_statement_proto_to_string_test",
    srcs = ["spanner_emulator_ddl_statement_proto_to_string_test.cc"],
    deps = [
        ":spanner_emulator_ddl_statement_cc_proto",
        ":spanner_emulator_ddl_statement_to_string",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
  name = "oss_fuzz_init",
  srcs = ["oss_fuzz.h"]
)

cc_library(
  name = "spanner_emulator_ddl_statement_to_string",
  srcs = ["protobufs/utils/spanner_emulator_ddl_statement_proto_to_string.cc",],
  deps = [
    ":spanner_emulator_ddl_statement_cc_proto",
    "@com_google_absl//absl/strings:strings",
  ],
  hdrs = ["protobufs/utils/spanner_emulator_ddl_statement_proto_to_string.h",]
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