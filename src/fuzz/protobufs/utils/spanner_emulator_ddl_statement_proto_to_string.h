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
using spanner_ddl::ColumnDataType;
using google::protobuf::RepeatedPtrField;

// proto to string methods
std::string toString(const SpannerDDLStatement& statement);
std::string toString(const CreateTable& create_table);
std::string tableColumnsToString(const RepeatedPtrField<Column>& primary_keys,
    const RepeatedPtrField<Column>& non_primary_keys);
std::string toString(const RepeatedPtrField<Column>& columns);
std::string toString(const Column& column);
std::string toString(const ColumnDataType& column_data_type);
std::string toString(const ColumnDataType::ScalarType& scalar_type, int length, 
    const ColumnDataType::LengthType& length_type);
std::string isColumnNotNullToString(bool is_not_null);
std::string columnOptionsToString(bool allow_commit_timestamps);
std::string toPrimaryKeys(const RepeatedPtrField<Column>& columns);
std::string columnToPrimaryKey(const Column& column);
std::string toString(const Column::Orientation& orientation);