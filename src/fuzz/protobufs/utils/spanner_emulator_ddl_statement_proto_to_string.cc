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
namespace google::protobuf

#include <string>

using spanner_ddl::SpannerDDLStatement;
using create_table::CreateTable;
using create_table::Column;



std::string toString(SpannerDDLStatement statement) {
    using statementType = SpannerDDLStatement::DDLStatementCase;
    switch (statement.DDLStatement_case()) {
        case statementType::kCreateTable:
            return toString(statement.createTable());
        //TODO: add more cases here for additional APIs
        default:
            return "";
    }
}

std::string toString(CreateTable createTable) {
    return absl::Substitute("CREATE TABLE $0 ($1, $2) PRIMARY KEY ($3)", 
                createTable.tableName(), 
                toString(createTable.primaryKeys()),
                toString(createTable.nonPrimaryKeys()),
                toPrimaryKeys(createTable.primaryKeys()));
}

// converts a list of columns to a string format of [column1], [column2], ...
std::string toString(RepeatedPtrField<Column>& columns) {
    std:string columnsAsString = "";
    for (int i = 0; i < columns.size(); i++) {
        Column currCol = columns.at(i);
        absl::StrAppend(&columnsAsString, toString(currCol));
    }
    // toString(currCol) will have an extra comma at the end so chop it off
    columnAsString = columnAsString.substring(0, columnAsString.size() - 1);
    return columnsAsString;
}

std::string toString(Column& column) {
    return "";
}

std::string toPrimaryKeys(RepeatedPtrField<Column>& columns) {
    return "";
}


