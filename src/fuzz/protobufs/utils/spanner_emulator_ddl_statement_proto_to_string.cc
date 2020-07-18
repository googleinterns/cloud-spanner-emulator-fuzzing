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

#include <cmath>
#include <string>
#include <vector>
#include "absl/strings/substitute.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"

using spanner_ddl::SpannerDDLStatement;
using spanner_ddl::CreateTable;
using spanner_ddl::Column;
using spanner_ddl::ColumnDataType;
using google::protobuf::RepeatedPtrField;

// forward declarations
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

// Transforms any Emulator DDL statement into a syntactically valid string
std::string toString(const SpannerDDLStatement& statement) {
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
std::string toString(const CreateTable& create_table) {
    return absl::Substitute("CREATE TABLE $0 ( $1 ) PRIMARY KEY ( $2 )",
                create_table.tablename(), 
                tableColumnsToString(create_table.primarykeys(), 
                    create_table.nonprimarykeys()),
                toPrimaryKeys(create_table.primarykeys()));
}

// returns a string representing all columns of the table depending on
// which exist
std::string tableColumnsToString(const RepeatedPtrField<Column>& primary_keys,
    const RepeatedPtrField<Column>& non_primary_keys) {
        if (primary_keys.size() > 0 && non_primary_keys.size() > 0) {
            return absl::Substitute("$0,$1", 
                toString(primary_keys), 
                toString(non_primary_keys));
        }
        else if (primary_keys.size() == 0 && non_primary_keys.size() > 0) {
            return toString(non_primary_keys);
        }
        else if (primary_keys.size() > 0 && non_primary_keys.size() == 0) {
            return toString(primary_keys);
        }
        else return "";
}

// converts a list of columns to a string format of {column1},{column2},...
// returns an empty string if no columns exist
std::string toString(const RepeatedPtrField<Column>& columns) {

    std::vector<std::string> columns_as_strings;
    for (const Column& curr_col : columns) {
        std::string curr_col_as_string = toString(curr_col);
        columns_as_strings.push_back(curr_col_as_string);
    }
    return absl::StrJoin(columns_as_strings, ",");
}

// A column is { column_name data_type [ NOT NULL ] [ options_def ] }
// where [ NOT NULL ] is optional and [ options_def ] is 
// { OPTIONS ( allow_commit_timestamp = { true | null } ) }
std::string toString(const Column& column) {
    return absl::Substitute("$0 $1 $2 $3", 
        column.columnname(), 
        toString(column.columndatatype()), 
        isColumnNotNullToString(column.isnotnull()),
        columnOptionsToString(column.allowcommittimestamp()));
}

// returns "data_type" or "ARRAY< data_type >" based on ColumnDataInfo
std::string toString(const ColumnDataType& column_data_type) {
    return column_data_type.isarray() ? 
        absl::Substitute("ARRAY< $0 >", toString(column_data_type.scalartype(), 
            column_data_type.length(), column_data_type.lengthtype())) :
        toString(column_data_type.scalartype(), column_data_type.length(), 
            column_data_type.lengthtype());
}

// returns the column's scalar type as a string
std::string toString(const ColumnDataType::ScalarType& scalar_type, int length, 
    const ColumnDataType::LengthType& length_type) {
    switch (scalar_type) {
        case ColumnDataType::BOOL:
            return "BOOL";
        case ColumnDataType::INT64:
            return "INT64";
        case ColumnDataType::FLOAT64:
            return "FLOAT64";
        case ColumnDataType::STRING:
            switch (length_type) {
                case ColumnDataType::BOUND:
                    // ensures length is a positive number
                    length = std::abs(length) + 1;
                    return absl::Substitute("STRING( $0 )", 
                        std::to_string(length % 2621440));
                case ColumnDataType::UNBOUND:
                    return absl::Substitute("STRING( $0 )", 
                        std::to_string(length));
                case ColumnDataType::MAX:
                    return "STRING( MAX )";
                default:
                    return ""; // never triggers
            }
        case ColumnDataType::BYTES:
            switch (length_type) {
                case ColumnDataType::BOUND:
                    // ensures length is a positive number
                    length = std::abs(length) + 1;
                    return absl::Substitute("BYTES( $0 )", 
                        std::to_string(length % 10485760));
                case ColumnDataType::UNBOUND:
                    return absl::Substitute("BYTES( $0 )", 
                        std::to_string(length));
                case ColumnDataType::MAX:
                    return "BYTES( MAX )";
                default:
                    return ""; // never triggers
            }
        case ColumnDataType::DATE:
            return "DATE";
        case ColumnDataType::TIMESTAMP:
            return "TIMESTAMP";
        default: // never occurs given the protobuf structure
            return "";
    }
}

// if this column should be not null, return the appropriate string
std::string isColumnNotNullToString(bool is_not_null) {
    return is_not_null ? "NOT NULL" : "";
}

// return the options for this column as a string
// the current api only allows for timestamps
std::string columnOptionsToString(bool allow_commit_timestamps) {
    std::string allow = allow_commit_timestamps ? "true" : "null";
    return absl::Substitute("OPTIONS ( allow_commit_timestamp = $0 )",
         allow);
}

// generates a list of the primary keys' column names and their orientations
std::string toPrimaryKeys(const RepeatedPtrField<Column>& columns) {
    std::vector<std::string> columns_as_strings;
    for (const Column& curr_col : columns) {
        std::string curr_col_as_string = columnToPrimaryKey(curr_col);
        columns_as_strings.push_back(curr_col_as_string);
    }
    // if there are no columns this will return an empty string
    return absl::StrJoin(columns_as_strings, ",");
}

std::string columnToPrimaryKey(const Column& column) {
    return absl::Substitute("$0 $1", column.columnname(), 
        toString(column.orientation()));
}

std::string toString(const Column::Orientation& orientation) {
    switch (orientation) {
        case Column::ASC:
            return "ASC";
        default:
            return "DESC";
    }
}

