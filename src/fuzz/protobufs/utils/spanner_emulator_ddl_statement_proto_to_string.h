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

#include "src/fuzz/protobufs/create_table.pb.h"
#include "src/fuzz/protobufs/spanner_ddl.pb.h"

#include <google/protobuf/repeated_field.h>

#include <string>
#include "absl/strings/substitute.h"
#include "absl/strings/str_cat.h"

using spanner_ddl::SpannerDDLStatement;
using spanner_ddl::CreateTable;
using spanner_ddl::Column;
using spanner_ddl::ColumnDataInfo;

// proto to string methods
std::string toString(SpannerDDLStatement statement);
std::string toString(CreateTable createTable);
std::string toString(const google::protobuf::RepeatedPtrField<Column> columns);
std::string toString(Column column);
std::string toString(ColumnDataInfo columnDataInfo);
std::string toString(ColumnDataInfo::ScalarType type, int length, 
    ColumnDataInfo::LengthType lengthType);
std::string isColumnNotNullToString(bool isNotNull);
std::string columnOptionsToString(bool allowCommitTimestamps);
std::string toPrimaryKeys(const google::protobuf::RepeatedPtrField<Column>& columns);
std::string columnToPrimaryKey(Column column);
std::string toString(Column::Orientation orientation);