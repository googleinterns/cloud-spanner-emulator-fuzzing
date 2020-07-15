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


// forward declarations
std::string toString(SpannerDDLStatement statement);
std::string toString(CreateTable createTable);
std::string toString(const google::protobuf::RepeatedPtrField<Column> columns);
std::string toString(Column column);
std::string toString(ColumnDataInfo columnDataInfo);
std::string toString(ColumnDataInfo::ScalarType type, int length);
std::string isColumnNotNullToString(bool isNotNull);
std::string columnOptionsToString(bool allowCommitTimestamps);
std::string toPrimaryKeys(const google::protobuf::RepeatedPtrField<Column>& columns);
std::string columnToPrimaryKey(Column column);
std::string toString(Column::Orientation orientation);


// takes any Spanner DDL statement and returns an appropriate string for the emulator
std::string toString(SpannerDDLStatement statement) {
    using statementType = SpannerDDLStatement::DDLStatementCase;
    switch (statement.DDLStatement_case()) {
        case statementType::kCreateTable:
            return toString(statement.createtable());
        //TODO: add more cases here for additional APIs
        default:
            return "";
    }
}

// generates a 'CREATE TABLE ...' statement
std::string toString(CreateTable createTable) {
    return absl::Substitute("CREATE TABLE $0 ([ $1,$2 ]) PRIMARY KEY ( [$3] )", 
                createTable.tablename(), 
                toString(createTable.primarykeys()),
                toString(createTable.nonprimarykeys()),
                toPrimaryKeys(createTable.primarykeys()));
}

// converts a list of columns to a string format of {column1},{column2},...
std::string toString(const google::protobuf::RepeatedPtrField<Column> columns) {
    std::string columnsAsString = "";
    for (int i = 0; i < columns.size(); i++) {
        const Column& currCol = columns.at(i);
        absl::StrAppend(&columnsAsString, toString(currCol));
    }
    // toString(currCol) will have an extra comma at the end so chop it off
    columnsAsString = columnsAsString.substr(0, columnsAsString.size() - 1);
    return columnsAsString;
}

std::string toString(Column column) {
    return absl::Substitute("{ $0 $1 $2 $3 },", 
        column.columnname(), 
        toString(column.columndatainfo()), 
        isColumnNotNullToString(column.isnotnull()),
        columnOptionsToString(column.allowcommittimestamp()));
}

// returns "data_type" or "ARRAY< data_type >" based on ColumnDataInfo
std::string toString(ColumnDataInfo columnDataInfo) {
    return columnDataInfo.isarray() ? 
        absl::Substitute("ARRAY< $0 >", toString(columnDataInfo.type(), columnDataInfo.length())) :
        toString(columnDataInfo.type(), columnDataInfo.length());
}

// returns the column's scalar type as a string
std::string toString(ColumnDataInfo::ScalarType type, int length) {
    switch (type) {
        case ColumnDataInfo::BOOL:
            return "BOOL";
        case ColumnDataInfo::INT64:
            return "INT64";
        case ColumnDataInfo::FLOAT64:
            return "FLOAT64";
        case ColumnDataInfo::STRING:
            return absl::Substitute("STRING( $0 )", std::to_string(length));
        case ColumnDataInfo::BYTES:
            return absl::Substitute("BYTES( $0 )", std::to_string(length));
        case ColumnDataInfo::DATE:
            return "DATE";
        case ColumnDataInfo::TIMESTAMP:
            return "TIMESTAMP";
        default: // never occurs given the protobuf structure
            return "";
    }
}

// if this column should be not null, return the appropriate string
std::string isColumnNotNullToString(bool isNotNull) {
    return isNotNull ? "[ NOT NULL ]" : "";
}

// return the options for this column as a string
// the current api only allows for timestamps
std::string columnOptionsToString(bool allowCommitTimestamps) {
    std::string allowTimestamps = allowCommitTimestamps ? "true" : "null";
    return absl::Substitute("{ OPTIONS ( allow_commit_timestamp = { $0 } ) }", allowTimestamps);
}

std::string toPrimaryKeys(const google::protobuf::RepeatedPtrField<Column>& columns) {
    std::string primaryKeysAsString = "";
    for (int i = 0; i < columns.size(); i++) {
        const Column& currCol = columns.at(i);
        absl::StrAppend(&primaryKeysAsString, columnToPrimaryKey(currCol));
    }
    // chop off an extra "," at the end
    primaryKeysAsString = primaryKeysAsString.substr(0, primaryKeysAsString.size() - 1);

    return primaryKeysAsString;
}

std::string columnToPrimaryKey(Column column) {
    return absl::Substitute("$0 [ { $1 } ],", column.columnname(), toString(column.orientation()));
}

std::string toString(Column::Orientation orientation) {
    switch (orientation) {
        case Column::ASC:
            return "ASC";
        default:
            return "DESC";
    }
}

