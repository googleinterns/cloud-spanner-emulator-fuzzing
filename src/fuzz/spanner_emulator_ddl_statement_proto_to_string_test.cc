//
// Copyright 2020 Google LLC.
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

#include <string>

#include "src/fuzz/protobufs/create_table.pb.h"
#include "src/fuzz/protobufs/spanner_ddl.pb.h"
#include "src/fuzz/protobufs/utils/spanner_emulator_ddl_statement_proto_to_string.h"
#include "gtest/gtest.h"
#include <google/protobuf/repeated_field.h>

#include "absl/strings/substitute.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"

using spanner_ddl::SpannerFuzzingStatements;
using spanner_ddl::SpannerDDLStatement;
using spanner_ddl::CreateTable;
using spanner_ddl::Column;
using spanner_ddl::ColumnDataType;

//TODO: add more to this test as more APIs are added
TEST(DDLStatementProtoToString, DDLStatementToString) {
    SpannerFuzzingStatements statements;
    SpannerDDLStatement* statement1 = statements.add_statements();

    CreateTable* createTable = statement1->mutable_createtable();
    createTable->set_tablename("testTable");

    Column* column1 = createTable->add_primarykeys();
    column1->set_columnname("testColumn1");
    
    ColumnDataType* columnDataType1 = column1->mutable_columndatatype();
    columnDataType1->set_scalartype(ColumnDataType::STRING);
    columnDataType1->set_length(200);
    columnDataType1->set_lengthtype(ColumnDataType::UNBOUND);
    columnDataType1->set_isarray(false);
    column1->set_isnotnull(true);
    column1->set_allowcommittimestamp(true);
    column1->set_orientation(Column::ASC);

    Column* column2 = createTable->add_primarykeys();
    column2->set_columnname("testColumn2");
    
    ColumnDataType* columnDataType2 = column2->mutable_columndatatype();
    columnDataType2->set_scalartype(ColumnDataType::BOOL);
    columnDataType2->set_length(2);
    columnDataType2->set_lengthtype(ColumnDataType::MAX);
    columnDataType2->set_isarray(true);
    column2->set_isnotnull(false);
    column2->set_allowcommittimestamp(false);
    column2->set_orientation(Column::DESC);

    Column* column3 = createTable->add_nonprimarykeys();
    column3->set_columnname("testColumn3");

    ColumnDataType* columnDataType3 = column3->mutable_columndatatype();
    columnDataType3->set_scalartype(ColumnDataType::TIMESTAMP);
    columnDataType3->set_length(-100);
    columnDataType3->set_lengthtype(ColumnDataType::BOUND);
    columnDataType3->set_isarray(false);
    column3->set_isnotnull(false);
    column3->set_allowcommittimestamp(true);

    EXPECT_EQ(createTable->primarykeys().size(), 2);
    EXPECT_EQ(createTable->nonprimarykeys().size(), 1);

    EXPECT_EQ(toString(*statement1), 
        absl::StrCat("CREATE TABLE testTable ( testColumn1 STRING( 200 )",
        " NOT NULL OPTIONS ( allow_commit_timestamp = true ),",
        "testColumn2 ARRAY< BOOL >  OPTIONS ( allow_commit_timestamp = ",
        "null ),testColumn3 TIMESTAMP  OPTIONS ",
        "( allow_commit_timestamp = true ) ) PRIMARY KEY ( ",
        "testColumn1 ASC,testColumn2 DESC )"));
}

TEST(DDLStatementProtoToString, CreateTableToString) {
    CreateTable createTable;
    createTable.set_tablename("testTable");

    Column* column1 = createTable.add_primarykeys();
    column1->set_columnname("testColumn1");
    
    ColumnDataType* columnDataType1 = column1->mutable_columndatatype();
    columnDataType1->set_scalartype(ColumnDataType::STRING);
    columnDataType1->set_length(200);
    columnDataType1->set_lengthtype(ColumnDataType::UNBOUND);
    columnDataType1->set_isarray(false);
    column1->set_isnotnull(true);
    column1->set_allowcommittimestamp(true);
    column1->set_orientation(Column::ASC);

    Column* column2 = createTable.add_primarykeys();
    column2->set_columnname("testColumn2");
    
    ColumnDataType* columnDataType2 = column2->mutable_columndatatype();
    columnDataType2->set_scalartype(ColumnDataType::BOOL);
    columnDataType2->set_length(2);
    columnDataType2->set_lengthtype(ColumnDataType::MAX);
    columnDataType2->set_isarray(true);
    column2->set_isnotnull(false);
    column2->set_allowcommittimestamp(false);
    column2->set_orientation(Column::DESC);

    Column* column3 = createTable.add_nonprimarykeys();
    column3->set_columnname("testColumn3");

    ColumnDataType* columnDataType3 = column3->mutable_columndatatype();
    columnDataType3->set_scalartype(ColumnDataType::TIMESTAMP);
    columnDataType3->set_length(-100);
    columnDataType3->set_lengthtype(ColumnDataType::BOUND);
    columnDataType3->set_isarray(false);
    column3->set_isnotnull(false);
    column3->set_allowcommittimestamp(true);

    EXPECT_EQ(createTable.primarykeys().size(), 2);
    EXPECT_EQ(createTable.nonprimarykeys().size(), 1);

    EXPECT_EQ(toString(createTable), 
        absl::StrCat("CREATE TABLE testTable ( testColumn1 STRING( ",
        "200 ) NOT NULL OPTIONS ( allow_commit_timestamp = true )",
        ",testColumn2 ARRAY< BOOL >  OPTIONS ( allow_commit_timestamp",
        " = null ),testColumn3 TIMESTAMP  OPTIONS ",
        "( allow_commit_timestamp = true ) ) PRIMARY KEY ( ",
        "testColumn1 ASC,testColumn2 DESC )"));
}

TEST(DDLStatementProtoToString, TableColumnsToStringTest) {
    CreateTable create_table;

    EXPECT_EQ(tableColumnsToString(create_table.primarykeys(), create_table.nonprimarykeys()), "");

    Column* column1 = create_table.add_primarykeys();
    column1->set_columnname("testColumn1");
    
    ColumnDataType* columnDataType1 = column1->mutable_columndatatype();
    columnDataType1->set_scalartype(ColumnDataType::STRING);
    columnDataType1->set_length(200);
    columnDataType1->set_lengthtype(ColumnDataType::UNBOUND);
    columnDataType1->set_isarray(false);
    column1->set_isnotnull(true);
    column1->set_allowcommittimestamp(true);

    EXPECT_EQ(tableColumnsToString(create_table.primarykeys(), create_table.nonprimarykeys()), 
    "testColumn1 STRING( 200 ) NOT NULL OPTIONS ( allow_commit_timestamp = true )");

    create_table.clear_primarykeys();

    Column* column2 = create_table.add_nonprimarykeys();
    column2->set_columnname("testColumn2");
    
    ColumnDataType* columnDataType2 = column2->mutable_columndatatype();
    columnDataType2->set_scalartype(ColumnDataType::BOOL);
    columnDataType2->set_length(2);
    columnDataType2->set_lengthtype(ColumnDataType::MAX);
    columnDataType2->set_isarray(true);
    column2->set_isnotnull(false);
    column2->set_allowcommittimestamp(false);
    
    EXPECT_EQ(tableColumnsToString(create_table.primarykeys(), create_table.nonprimarykeys()), 
    "testColumn2 ARRAY< BOOL >  OPTIONS ( allow_commit_timestamp = null )");

    Column* column3 = create_table.add_primarykeys();
    column1->set_columnname("testColumn3");

    ColumnDataType* columnDataType3 = column3->mutable_columndatatype();
    columnDataType3->set_scalartype(ColumnDataType::STRING);
    columnDataType3->set_length(200);
    columnDataType3->set_lengthtype(ColumnDataType::UNBOUND);
    columnDataType3->set_isarray(false);
    column3->set_isnotnull(true);
    column3->set_allowcommittimestamp(true);

    EXPECT_EQ(tableColumnsToString(create_table.primarykeys(), create_table.nonprimarykeys()),
        absl::StrCat("testColumn3 STRING( 200 ) NOT NULL ", 
        "OPTIONS ( allow_commit_timestamp = true ),",
        "testColumn2 ARRAY< BOOL >  OPTIONS",
        " ( allow_commit_timestamp = null )"));
}

TEST(DDLStatementProtoToString, ColumnsToString) {
    CreateTable createTable;

    EXPECT_EQ(createTable.primarykeys().size(), 0);
    EXPECT_EQ(toString(createTable.primarykeys()), "");

    Column* column1 = createTable.add_primarykeys();
    column1->set_columnname("testColumn1");
    
    ColumnDataType* columnDataType1 = column1->mutable_columndatatype();
    columnDataType1->set_scalartype(ColumnDataType::STRING);
    columnDataType1->set_length(200);
    columnDataType1->set_lengthtype(ColumnDataType::UNBOUND);
    columnDataType1->set_isarray(false);
    column1->set_isnotnull(true);
    column1->set_allowcommittimestamp(true);

    Column* column2 = createTable.add_primarykeys();
    column2->set_columnname("testColumn2");
    
    ColumnDataType* columnDataType2 = column2->mutable_columndatatype();
    columnDataType2->set_scalartype(ColumnDataType::BOOL);
    columnDataType2->set_length(2);
    columnDataType2->set_lengthtype(ColumnDataType::MAX);
    columnDataType2->set_isarray(true);
    column2->set_isnotnull(false);
    column2->set_allowcommittimestamp(false);

    EXPECT_EQ(createTable.primarykeys().size(), 2);

    EXPECT_EQ(toString(createTable.primarykeys()), 
        absl::StrCat("testColumn1 STRING( 200 ) NOT NULL ", 
        "OPTIONS ( allow_commit_timestamp = true ),",
        "testColumn2 ARRAY< BOOL >  OPTIONS",
        " ( allow_commit_timestamp = null )"));
}

TEST(DDLStatementProtoToString, ColumnToStringTest) {
    Column column;
    column.set_columnname("testColumn");
    
    ColumnDataType* columnDataType = column.mutable_columndatatype();

    EXPECT_EQ(columnDataType->has_scalartype(), false);
    EXPECT_EQ(columnDataType->has_length(), false);
    EXPECT_EQ(columnDataType->has_lengthtype(), false);
    EXPECT_EQ(columnDataType->has_isarray(), false);

    columnDataType->set_scalartype(ColumnDataType::STRING);
    columnDataType->set_length(200);
    columnDataType->set_lengthtype(ColumnDataType::BOUND);
    columnDataType->set_isarray(false);

    column.set_isnotnull(true);
    column.set_allowcommittimestamp(true);

    EXPECT_EQ(toString(column), absl::StrCat("testColumn STRING( 201 )",
    " NOT NULL OPTIONS ( allow_commit_timestamp = true )"));
}

TEST(DDLStatementProtoToString, ColumnInfoDataToStringTest) {
    ColumnDataType columnDataType;

    EXPECT_EQ(columnDataType.has_scalartype(), false);
    EXPECT_EQ(columnDataType.has_length(), false);
    EXPECT_EQ(columnDataType.has_lengthtype(), false);
    EXPECT_EQ(columnDataType.has_isarray(), false);

    columnDataType.set_scalartype(ColumnDataType::STRING);
    columnDataType.set_length(200);
    columnDataType.set_lengthtype(ColumnDataType::BOUND);

    columnDataType.set_isarray(false);
    EXPECT_EQ(toString(columnDataType), "STRING( 201 )");  

    columnDataType.set_isarray(true);
    EXPECT_EQ(toString(columnDataType), "ARRAY< STRING( 201 ) >");  
}

TEST(DDLStatementProtoToString, ColumnDataTypeToStringHelperTest) {
    ColumnDataType columnDataType;

    EXPECT_EQ(columnDataType.has_scalartype(), false);
    EXPECT_EQ(columnDataType.has_length(), false);
    EXPECT_EQ(columnDataType.has_lengthtype(), false);

    columnDataType.set_scalartype(ColumnDataType::BOOL);
    columnDataType.set_length(-500);
    columnDataType.set_lengthtype(ColumnDataType::UNBOUND);
    EXPECT_EQ(toString(columnDataType.scalartype(), columnDataType.length(), 
        columnDataType.lengthtype()), "BOOL");

    columnDataType.set_scalartype(ColumnDataType::INT64);
    EXPECT_EQ(toString(columnDataType.scalartype(), columnDataType.length(), 
        columnDataType.lengthtype()), "INT64");
    
    columnDataType.set_scalartype(ColumnDataType::FLOAT64);
    EXPECT_EQ(toString(columnDataType.scalartype(), columnDataType.length(), 
        columnDataType.lengthtype()), "FLOAT64");

    columnDataType.set_scalartype(ColumnDataType::DATE);
    EXPECT_EQ(toString(columnDataType.scalartype(), columnDataType.length(), 
        columnDataType.lengthtype()), "DATE");

    columnDataType.set_scalartype(ColumnDataType::TIMESTAMP);
    EXPECT_EQ(toString(columnDataType.scalartype(), columnDataType.length(), 
        columnDataType.lengthtype()), "TIMESTAMP");

    columnDataType.set_scalartype(ColumnDataType::STRING);
    EXPECT_EQ(toString(columnDataType.scalartype(), columnDataType.length(), 
        columnDataType.lengthtype()), "STRING( -500 )");
    columnDataType.set_lengthtype(ColumnDataType::MAX);
    EXPECT_EQ(toString(columnDataType.scalartype(), columnDataType.length(), 
        columnDataType.lengthtype()), "STRING( MAX )");
    columnDataType.set_lengthtype(ColumnDataType::BOUND);
    EXPECT_EQ(toString(columnDataType.scalartype(), columnDataType.length(), 
        columnDataType.lengthtype()), "STRING( 501 )");
    // test that the max wraps back to 1
    columnDataType.set_length(2621440);
    EXPECT_EQ(toString(columnDataType.scalartype(), columnDataType.length(), 
        columnDataType.lengthtype()), "STRING( 1 )");

    columnDataType.set_length(-500);
    columnDataType.set_lengthtype(ColumnDataType::UNBOUND);
    columnDataType.set_scalartype(ColumnDataType::BYTES);
    EXPECT_EQ(toString(columnDataType.scalartype(), columnDataType.length(), 
        columnDataType.lengthtype()), "BYTES( -500 )");
    columnDataType.set_lengthtype(ColumnDataType::MAX);
    EXPECT_EQ(toString(columnDataType.scalartype(), columnDataType.length(), 
        columnDataType.lengthtype()), "BYTES( MAX )");
    columnDataType.set_lengthtype(ColumnDataType::BOUND);
    EXPECT_EQ(toString(columnDataType.scalartype(), columnDataType.length(), 
        columnDataType.lengthtype()), "BYTES( 501 )");
    // test that the max wraps back to 1
    columnDataType.set_length(10485760);
    EXPECT_EQ(toString(columnDataType.scalartype(), columnDataType.length(), 
        columnDataType.lengthtype()), "BYTES( 1 )");
}

TEST(DDLStatementProtoToString, IsColumnNotNullToStringTest) {
    EXPECT_EQ(isColumnNotNullToString(true), "NOT NULL");
    EXPECT_EQ(isColumnNotNullToString(false), "");
}

TEST(DDLStatementProtoToString, ColumnOptionsToStringTest) {
    EXPECT_EQ(columnOptionsToString(false), "OPTIONS ( allow_commit_timestamp = null )");
    EXPECT_EQ(columnOptionsToString(true), "OPTIONS ( allow_commit_timestamp = true )");
}

TEST(DDLStatementProtoToString, ToPrimaryKeysTest) {
    CreateTable createTable;

    Column* column1 = createTable.add_primarykeys();
    EXPECT_EQ(column1->has_columnname(), false);
    EXPECT_EQ(column1->has_orientation(), false);

    column1->set_orientation(Column::ASC);
    column1->set_columnname("testColumn1");

    Column* column2 = createTable.add_primarykeys();
    EXPECT_EQ(column2->has_columnname(), false);
    EXPECT_EQ(column2->has_orientation(), false);

    column2->set_orientation(Column::DESC);
    column2->set_columnname("testColumn2");

    EXPECT_EQ(createTable.primarykeys().size(), 2);

    EXPECT_EQ(toPrimaryKeys(createTable.primarykeys()), "testColumn1 ASC,testColumn2 DESC");
}

TEST(DDLStatementProtoToString, ColumnToPrimaryKeyTest) {
    Column column;
    EXPECT_EQ(column.has_columnname(), false);
    EXPECT_EQ(column.has_orientation(), false);

    column.set_orientation(Column::ASC);
    column.set_columnname("testColumn");

    EXPECT_EQ(columnToPrimaryKey(column), "testColumn ASC");
}

TEST(DDLStatementProtoToString, ColumnOrientationTest) {
    Column column;
    EXPECT_EQ(column.has_orientation(), false);

    column.set_orientation(Column::ASC);
    EXPECT_EQ(toString(column.orientation()), "ASC");

    column.set_orientation(Column::DESC);
    EXPECT_EQ(toString(column.orientation()), "DESC");
}
