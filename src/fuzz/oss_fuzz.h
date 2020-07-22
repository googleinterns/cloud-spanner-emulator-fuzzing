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

// Utilities for OSS-Fuzz Initialization

#if defined(__OSS_FUZZ__) && !defined(SPANNER_EMULATOR_FUZZING_OSS_FUZZ_H_)
#define SPANNER_EMULATOR_FUZZING_OSS_FUZZ_H_

#include <filesystem>
#include "zetasql/base/logging.h"

const int OVERWRITE = 1;

namespace spanner_emulator_fuzzer {

bool DoOssFuzzInit() {
  namespace fs = std::filesystem;
  fs::path originDir;
  try {
    originDir = fs::canonical(fs::read_symlink("/proc/self/exe")).parent_path();
  } catch (const std::exception& e) {
    LOG(ERROR) << "Error - Cannot initialize OSS-Fuzz";
    return false;
  }
  // Timezone data in OSS-Fuzz is in a non-standard location. This allows asbl to know where it resides
  fs::path tzdataDir = originDir / "data/zoneinfo/";
  if (setenv("TZDIR", tzdataDir.c_str(), OVERWRITE)) {
    LOG(ERROR) << "Error - Timezone data copied incorrectly from OSS-Fuzz build";
    return false;
  }
  return true;
}

}  

#endif  

