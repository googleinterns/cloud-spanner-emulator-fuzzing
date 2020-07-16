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

using spanner_ddl::SpannerFuzzingStatements;
using spanner_ddl::SpannerDDLStatement;
using spanner_ddl::CreateTable;
using spanner_ddl::Column;
using spanner_ddl::ColumnDataInfo;

//TODO: add more to this test as more APIs are added
TEST(DDLStatementProtoToString, DDLStatementToString) {
    SpannerFuzzingStatements statements;
    SpannerDDLStatement* statement1 = statements.add_statements();

    CreateTable* createTable = statement1->mutable_createtable();
    createTable->set_tablename("testTable");

    Column* column1 = createTable->add_primarykeys();
    column1->set_columnname("testColumn1");
    
    ColumnDataInfo* columnDataInfo1 = column1->mutable_columndatainfo();
    columnDataInfo1->set_scalartype(ColumnDataInfo::STRING);
    columnDataInfo1->set_length(200);
    columnDataInfo1->set_lengthtype(ColumnDataInfo::UNBOUND);
    columnDataInfo1->set_isarray(false);
    column1->set_isnotnull(true);
    column1->set_allowcommittimestamp(true);
    column1->set_orientation(Column::ASC);

    Column* column2 = createTable->add_primarykeys();
    column2->set_columnname("testColumn2");
    
    ColumnDataInfo* columnDataInfo2 = column2->mutable_columndatainfo();
    columnDataInfo2->set_scalartype(ColumnDataInfo::BOOL);
    columnDataInfo2->set_length(2);
    columnDataInfo2->set_lengthtype(ColumnDataInfo::MAX);
    columnDataInfo2->set_isarray(true);
    column2->set_isnotnull(false);
    column2->set_allowcommittimestamp(false);
    column2->set_orientation(Column::DESC);

    Column* column3 = createTable->add_nonprimarykeys();
    column3->set_columnname("testColumn3");

    ColumnDataInfo* columnDataInfo3 = column3->mutable_columndatainfo();
    columnDataInfo3->set_scalartype(ColumnDataInfo::TIMESTAMP);
    columnDataInfo3->set_length(-100);
    columnDataInfo3->set_lengthtype(ColumnDataInfo::BOUND);
    columnDataInfo3->set_isarray(false);
    column3->set_isnotnull(false);
    column3->set_allowcommittimestamp(true);

    EXPECT_EQ(createTable->primarykeys().size(), 2);
    EXPECT_EQ(createTable->nonprimarykeys().size(), 1);

    EXPECT_EQ(toString(*statement1), 
        absl::StrCat("CREATE TABLE testTable ([ { testColumn1 STRING( 200 )",
        " NOT NULL { OPTIONS ( allow_commit_timestamp = { true } ) } },",
        "{ testColumn2 ARRAY< BOOL >  { OPTIONS ( allow_commit_timestamp = ",
        "{ null } ) } },{ testColumn3 TIMESTAMP  { OPTIONS ( ",
        "allow_commit_timestamp = { true } ) } } ]) PRIMARY KEY ( ",
        "[testColumn1 [ { ASC } ],testColumn2 [ { DESC } ]] )"));
}

TEST(DDLStatementProtoToString, CreateTableToString) {
    CreateTable createTable;
    createTable.set_tablename("testTable");

    Column* column1 = createTable.add_primarykeys();
    column1->set_columnname("testColumn1");
    
    ColumnDataInfo* columnDataInfo1 = column1->mutable_columndatainfo();
    columnDataInfo1->set_scalartype(ColumnDataInfo::STRING);
    columnDataInfo1->set_length(200);
    columnDataInfo1->set_lengthtype(ColumnDataInfo::UNBOUND);
    columnDataInfo1->set_isarray(false);
    column1->set_isnotnull(true);
    column1->set_allowcommittimestamp(true);
    column1->set_orientation(Column::ASC);

    Column* column2 = createTable.add_primarykeys();
    column2->set_columnname("testColumn2");
    
    ColumnDataInfo* columnDataInfo2 = column2->mutable_columndatainfo();
    columnDataInfo2->set_scalartype(ColumnDataInfo::BOOL);
    columnDataInfo2->set_length(2);
    columnDataInfo2->set_lengthtype(ColumnDataInfo::MAX);
    columnDataInfo2->set_isarray(true);
    column2->set_isnotnull(false);
    column2->set_allowcommittimestamp(false);
    column2->set_orientation(Column::DESC);

    Column* column3 = createTable.add_nonprimarykeys();
    column3->set_columnname("testColumn3");

    ColumnDataInfo* columnDataInfo3 = column3->mutable_columndatainfo();
    columnDataInfo3->set_scalartype(ColumnDataInfo::TIMESTAMP);
    columnDataInfo3->set_length(-100);
    columnDataInfo3->set_lengthtype(ColumnDataInfo::BOUND);
    columnDataInfo3->set_isarray(false);
    column3->set_isnotnull(false);
    column3->set_allowcommittimestamp(true);

    EXPECT_EQ(createTable.primarykeys().size(), 2);
    EXPECT_EQ(createTable.nonprimarykeys().size(), 1);

    EXPECT_EQ(toString(createTable), 
        absl::StrCat("CREATE TABLE testTable ([ { testColumn1 STRING( ",
        "200 ) NOT NULL { OPTIONS ( allow_commit_timestamp = { true } )",
        " } },{ testColumn2 ARRAY< BOOL >  { OPTIONS ( allow_commit_timestamp",
        " = { null } ) } },{ testColumn3 TIMESTAMP  { OPTIONS ( ",
        "allow_commit_timestamp = { true } ) } } ]) PRIMARY KEY ( ",
        "[testColumn1 [ { ASC } ],testColumn2 [ { DESC } ]] )"));
}

TEST(DDLStatementProtoToString, ColumnsToString) {
    CreateTable createTable;

    Column* column1 = createTable.add_primarykeys();
    column1->set_columnname("testColumn1");
    
    ColumnDataInfo* columnDataInfo1 = column1->mutable_columndatainfo();
    columnDataInfo1->set_scalartype(ColumnDataInfo::STRING);
    columnDataInfo1->set_length(200);
    columnDataInfo1->set_lengthtype(ColumnDataInfo::UNBOUND);
    columnDataInfo1->set_isarray(false);
    column1->set_isnotnull(true);
    column1->set_allowcommittimestamp(true);

    Column* column2 = createTable.add_primarykeys();
    column2->set_columnname("testColumn2");
    
    ColumnDataInfo* columnDataInfo2 = column2->mutable_columndatainfo();
    columnDataInfo2->set_scalartype(ColumnDataInfo::BOOL);
    columnDataInfo2->set_length(2);
    columnDataInfo2->set_lengthtype(ColumnDataInfo::MAX);
    columnDataInfo2->set_isarray(true);
    column2->set_isnotnull(false);
    column2->set_allowcommittimestamp(false);

    EXPECT_EQ(createTable.primarykeys().size(), 2);

    EXPECT_EQ(toString(createTable.primarykeys()), 
        absl::StrCat("{ testColumn1 STRING( 200 ) NOT NULL ", 
        "{ OPTIONS ( allow_commit_timestamp = { true } ) } },",
        "{ testColumn2 ARRAY< BOOL >  { OPTIONS (",
        " allow_commit_timestamp = { null } ) } }"));
}

TEST(DDLStatementProtoToString, ColumnToStringTest) {
    Column column;
    column.set_columnname("testColumn");
    
    ColumnDataInfo* columnDataInfo = column.mutable_columndatainfo();

    EXPECT_EQ(columnDataInfo->has_scalartype(), false);
    EXPECT_EQ(columnDataInfo->has_length(), false);
    EXPECT_EQ(columnDataInfo->has_lengthtype(), false);
    EXPECT_EQ(columnDataInfo->has_isarray(), false);

    columnDataInfo->set_scalartype(ColumnDataInfo::STRING);
    columnDataInfo->set_length(200);
    columnDataInfo->set_lengthtype(ColumnDataInfo::BOUND);
    columnDataInfo->set_isarray(false);

    column.set_isnotnull(true);
    column.set_allowcommittimestamp(true);

    EXPECT_EQ(toString(column), absl::StrCat("{ testColumn STRING( 201 )",
    " NOT NULL { OPTIONS ( allow_commit_timestamp = { true } ) } },"));
}

TEST(DDLStatementProtoToString, ColumnInfoDataToStringTest) {
    ColumnDataInfo columnDataInfo;

    EXPECT_EQ(columnDataInfo.has_scalartype(), false);
    EXPECT_EQ(columnDataInfo.has_length(), false);
    EXPECT_EQ(columnDataInfo.has_lengthtype(), false);
    EXPECT_EQ(columnDataInfo.has_isarray(), false);

    columnDataInfo.set_scalartype(ColumnDataInfo::STRING);
    columnDataInfo.set_length(200);
    columnDataInfo.set_lengthtype(ColumnDataInfo::BOUND);

    columnDataInfo.set_isarray(false);
    EXPECT_EQ(toString(columnDataInfo), "STRING( 201 )");  

    columnDataInfo.set_isarray(true);
    EXPECT_EQ(toString(columnDataInfo), "ARRAY< STRING( 201 ) >");  
}

TEST(DDLStatementProtoToString, ColumnDataInfoToStringHelperTest) {
    ColumnDataInfo columnDataInfo;

    EXPECT_EQ(columnDataInfo.has_scalartype(), false);
    EXPECT_EQ(columnDataInfo.has_length(), false);
    EXPECT_EQ(columnDataInfo.has_lengthtype(), false);

    columnDataInfo.set_scalartype(ColumnDataInfo::BOOL);
    columnDataInfo.set_length(-500);
    columnDataInfo.set_lengthtype(ColumnDataInfo::UNBOUND);
    EXPECT_EQ(toString(columnDataInfo.scalartype(), columnDataInfo.length(), 
        columnDataInfo.lengthtype()), "BOOL");

    columnDataInfo.set_scalartype(ColumnDataInfo::INT64);
    EXPECT_EQ(toString(columnDataInfo.scalartype(), columnDataInfo.length(), 
        columnDataInfo.lengthtype()), "INT64");
    
    columnDataInfo.set_scalartype(ColumnDataInfo::FLOAT64);
    EXPECT_EQ(toString(columnDataInfo.scalartype(), columnDataInfo.length(), 
        columnDataInfo.lengthtype()), "FLOAT64");

    columnDataInfo.set_scalartype(ColumnDataInfo::DATE);
    EXPECT_EQ(toString(columnDataInfo.scalartype(), columnDataInfo.length(), 
        columnDataInfo.lengthtype()), "DATE");

    columnDataInfo.set_scalartype(ColumnDataInfo::TIMESTAMP);
    EXPECT_EQ(toString(columnDataInfo.scalartype(), columnDataInfo.length(), 
        columnDataInfo.lengthtype()), "TIMESTAMP");

    columnDataInfo.set_scalartype(ColumnDataInfo::STRING);
    EXPECT_EQ(toString(columnDataInfo.scalartype(), columnDataInfo.length(), 
        columnDataInfo.lengthtype()), "STRING( -500 )");
    columnDataInfo.set_lengthtype(ColumnDataInfo::MAX);
    EXPECT_EQ(toString(columnDataInfo.scalartype(), columnDataInfo.length(), 
        columnDataInfo.lengthtype()), "STRING( MAX )");
    columnDataInfo.set_lengthtype(ColumnDataInfo::BOUND);
    EXPECT_EQ(toString(columnDataInfo.scalartype(), columnDataInfo.length(), 
        columnDataInfo.lengthtype()), "STRING( 501 )");
    // test that the max wraps back to 1
    columnDataInfo.set_length(2621440);
    EXPECT_EQ(toString(columnDataInfo.scalartype(), columnDataInfo.length(), 
        columnDataInfo.lengthtype()), "STRING( 1 )");

    columnDataInfo.set_length(-500);
    columnDataInfo.set_lengthtype(ColumnDataInfo::UNBOUND);
    columnDataInfo.set_scalartype(ColumnDataInfo::BYTES);
    EXPECT_EQ(toString(columnDataInfo.scalartype(), columnDataInfo.length(), 
        columnDataInfo.lengthtype()), "BYTES( -500 )");
    columnDataInfo.set_lengthtype(ColumnDataInfo::MAX);
    EXPECT_EQ(toString(columnDataInfo.scalartype(), columnDataInfo.length(), 
        columnDataInfo.lengthtype()), "BYTES( MAX )");
    columnDataInfo.set_lengthtype(ColumnDataInfo::BOUND);
    EXPECT_EQ(toString(columnDataInfo.scalartype(), columnDataInfo.length(), 
        columnDataInfo.lengthtype()), "BYTES( 501 )");
    // test that the max wraps back to 1
    columnDataInfo.set_length(10485760);
    EXPECT_EQ(toString(columnDataInfo.scalartype(), columnDataInfo.length(), 
        columnDataInfo.lengthtype()), "BYTES( 1 )");
}

TEST(DDLStatementProtoToString, IsColumnNotNullToStringTest) {
    EXPECT_EQ(isColumnNotNullToString(true), "NOT NULL");
    EXPECT_EQ(isColumnNotNullToString(false), "");
}

TEST(DDLStatementProtoToString, ColumnOptionsToStringTest) {
    EXPECT_EQ(columnOptionsToString(false), "{ OPTIONS ( allow_commit_timestamp = { null } ) }");
    EXPECT_EQ(columnOptionsToString(true), "{ OPTIONS ( allow_commit_timestamp = { true } ) }");
}

TEST(DDLStatementProtoToString, ToPrimaryKeysTest) {
    CreateTable createTable;

    Column* column1 = createTable.add_primarykeys();
    EXPECT_EQ(column1->has_columnname(), false);
    EXPECT_EQ(column1->has_orientation(), false);

    column1->set_orientation(Column::ASC);
    column1->set_columnname("testName1");

    Column* column2 = createTable.add_primarykeys();
    EXPECT_EQ(column2->has_columnname(), false);
    EXPECT_EQ(column2->has_orientation(), false);

    column2->set_orientation(Column::DESC);
    column2->set_columnname("testName2");

    EXPECT_EQ(createTable.primarykeys().size(), 2);

    EXPECT_EQ(toPrimaryKeys(createTable.primarykeys()), "testName1 [ { ASC } ],testName2 [ { DESC } ]");
}

TEST(DDLStatementProtoToString, ColumnToPrimaryKeyTest) {
    Column column;
    EXPECT_EQ(column.has_columnname(), false);
    EXPECT_EQ(column.has_orientation(), false);

    column.set_orientation(Column::ASC);
    column.set_columnname("testName");

    EXPECT_EQ(columnToPrimaryKey(column), "testName [ { ASC } ],");
}

TEST(DDLStatementProtoToString, ColumnOrientationTest) {
    Column column;
    EXPECT_EQ(column.has_orientation(), false);

    column.set_orientation(Column::ASC);
    EXPECT_EQ(toString(column.orientation()), "ASC");

    column.set_orientation(Column::DESC);
    EXPECT_EQ(toString(column.orientation()), "DESC");
}
