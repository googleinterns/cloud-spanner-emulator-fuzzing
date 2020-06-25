

cc_binary(
  name = "simple_fuzz_test",
  srcs = ["simple_fuzz_test.cc"],
  linkopts = [ "$(LIB_FUZZING_ENGINE)" ],
  deps = [
    "@com_google_cloud_spanner_emulator//frontend/server",
    "@com_github_googleapis_google_cloud_cpp_spanner//google/cloud/spanner:spanner_client",
    "@com_google_absl//absl/strings:strings",
    "@com_google_zetasql//zetasql/base:logging",
  ]
)
