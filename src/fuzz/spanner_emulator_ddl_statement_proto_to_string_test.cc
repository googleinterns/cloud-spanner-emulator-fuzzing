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

//TODO: add test that creates a table in the emulator using a protobuf
//TODO: add more to this test as more APIs are added
TEST(DDLStatementProtoToString, DDLStatementToString) {
    SpannerFuzzingStatements statements;
    SpannerDDLStatement* statement1 = statements.add_statements();

    CreateTable* create_table = statement1->mutable_createtable();
    create_table->set_tablename("testTable");

    Column* column1 = create_table->add_primarykeys();
    column1->set_columnname("testColumn1");
    
    ColumnDataType* column_data_type1 = column1->mutable_columndatatype();
    column_data_type1->set_scalartype(ColumnDataType::STRING);
    column_data_type1->set_length(200);
    column_data_type1->set_lengthtype(ColumnDataType::UNBOUND);
    column_data_type1->set_isarray(false);
    column1->set_isnotnull(true);
    column1->set_allowcommittimestamp(true);
    column1->set_orientation(Column::ASC);

    Column* column2 = create_table->add_primarykeys();
    column2->set_columnname("testColumn2");
    
    ColumnDataType* column_data_type2 = column2->mutable_columndatatype();
    column_data_type2->set_scalartype(ColumnDataType::BOOL);
    column_data_type2->set_length(2);
    column_data_type2->set_lengthtype(ColumnDataType::MAX);
    column_data_type2->set_isarray(true);
    column2->set_isnotnull(false);
    column2->set_allowcommittimestamp(false);
    column2->set_orientation(Column::DESC);

    Column* column3 = create_table->add_nonprimarykeys();
    column3->set_columnname("testColumn3");

    ColumnDataType* column_data_type3 = column3->mutable_columndatatype();
    column_data_type3->set_scalartype(ColumnDataType::TIMESTAMP);
    column_data_type3->set_length(-100);
    column_data_type3->set_lengthtype(ColumnDataType::BOUND);
    column_data_type3->set_isarray(false);
    column3->set_isnotnull(false);
    column3->set_allowcommittimestamp(true);

    EXPECT_EQ(create_table->primarykeys().size(), 2);
    EXPECT_EQ(create_table->nonprimarykeys().size(), 1);

    EXPECT_EQ(toString(*statement1), 
        "CREATE TABLE testTable ( "
        "testColumn1 STRING( 200 ) "
        "NOT NULL OPTIONS ( allow_commit_timestamp = true ),"
        "testColumn2 ARRAY< BOOL >  "
        "OPTIONS ( allow_commit_timestamp = null ),"
        "testColumn3 TIMESTAMP  "
        "OPTIONS ( allow_commit_timestamp = true ) ) "
        "PRIMARY KEY ( testColumn1 ASC,testColumn2 DESC )");
}

TEST(DDLStatementProtoToString, CreateTableToString) {
    CreateTable create_table;
    create_table.set_tablename("testTable");

    Column* column1 = create_table.add_primarykeys();
    column1->set_columnname("testColumn1");
    
    ColumnDataType* column_data_type1 = column1->mutable_columndatatype();
    column_data_type1->set_scalartype(ColumnDataType::STRING);
    column_data_type1->set_length(200);
    column_data_type1->set_lengthtype(ColumnDataType::UNBOUND);
    column_data_type1->set_isarray(false);
    column1->set_isnotnull(true);
    column1->set_allowcommittimestamp(true);
    column1->set_orientation(Column::ASC);

    Column* column2 = create_table.add_primarykeys();
    column2->set_columnname("testColumn2");
    
    ColumnDataType* column_data_type2 = column2->mutable_columndatatype();
    column_data_type2->set_scalartype(ColumnDataType::BOOL);
    column_data_type2->set_length(2);
    column_data_type2->set_lengthtype(ColumnDataType::MAX);
    column_data_type2->set_isarray(true);
    column2->set_isnotnull(false);
    column2->set_allowcommittimestamp(false);
    column2->set_orientation(Column::DESC);

    Column* column3 = create_table.add_nonprimarykeys();
    column3->set_columnname("testColumn3");

    ColumnDataType* column_data_type3 = column3->mutable_columndatatype();
    column_data_type3->set_scalartype(ColumnDataType::TIMESTAMP);
    column_data_type3->set_length(-100);
    column_data_type3->set_lengthtype(ColumnDataType::BOUND);
    column_data_type3->set_isarray(false);
    column3->set_isnotnull(false);
    column3->set_allowcommittimestamp(true);

    EXPECT_EQ(create_table.primarykeys().size(), 2);
    EXPECT_EQ(create_table.nonprimarykeys().size(), 1);

    EXPECT_EQ(toString(create_table), 
        "CREATE TABLE testTable ( "
        "testColumn1 STRING( 200 ) "
        "NOT NULL OPTIONS ( allow_commit_timestamp = true ),"
        "testColumn2 ARRAY< BOOL >  "
        "OPTIONS ( allow_commit_timestamp = null ),"
        "testColumn3 TIMESTAMP  "
        "OPTIONS ( allow_commit_timestamp = true ) ) "
        "PRIMARY KEY ( testColumn1 ASC,testColumn2 DESC )"
    );
}

TEST(DDLStatementProtoToString, TableColumnsToStringTest) {
    CreateTable create_table;

    EXPECT_EQ(tableColumnsToString(create_table.primarykeys(), 
        create_table.nonprimarykeys()), "");

    Column* column1 = create_table.add_primarykeys();
    column1->set_columnname("testColumn1");
    
    ColumnDataType* column_data_type1 = column1->mutable_columndatatype();
    column_data_type1->set_scalartype(ColumnDataType::STRING);
    column_data_type1->set_length(200);
    column_data_type1->set_lengthtype(ColumnDataType::UNBOUND);
    column_data_type1->set_isarray(false);
    column1->set_isnotnull(true);
    column1->set_allowcommittimestamp(true);

    EXPECT_EQ(tableColumnsToString(create_table.primarykeys(), 
        create_table.nonprimarykeys()), 
        "testColumn1 STRING( 200 ) NOT NULL "
        "OPTIONS ( allow_commit_timestamp = true )"
        );

    create_table.clear_primarykeys();

    Column* column2 = create_table.add_nonprimarykeys();
    column2->set_columnname("testColumn2");
    
    ColumnDataType* column_data_type2 = column2->mutable_columndatatype();
    column_data_type2->set_scalartype(ColumnDataType::BOOL);
    column_data_type2->set_length(2);
    column_data_type2->set_lengthtype(ColumnDataType::MAX);
    column_data_type2->set_isarray(true);
    column2->set_isnotnull(false);
    column2->set_allowcommittimestamp(false);
    
    EXPECT_EQ(tableColumnsToString(create_table.primarykeys(), 
        create_table.nonprimarykeys()), 
        "testColumn2 ARRAY< BOOL >  "
        "OPTIONS ( allow_commit_timestamp = null )"
    );

    Column* column3 = create_table.add_primarykeys();
    column1->set_columnname("testColumn3");

    ColumnDataType* column_data_type3 = column3->mutable_columndatatype();
    column_data_type3->set_scalartype(ColumnDataType::STRING);
    column_data_type3->set_length(200);
    column_data_type3->set_lengthtype(ColumnDataType::UNBOUND);
    column_data_type3->set_isarray(false);
    column3->set_isnotnull(true);
    column3->set_allowcommittimestamp(true);

    EXPECT_EQ(tableColumnsToString(create_table.primarykeys(), 
        create_table.nonprimarykeys()),
        "testColumn3 STRING( 200 ) NOT NULL "
        "OPTIONS ( allow_commit_timestamp = true ),"
        "testColumn2 ARRAY< BOOL >  "
        "OPTIONS ( allow_commit_timestamp = null )"
        );
}

TEST(DDLStatementProtoToString, ColumnsToString) {
    CreateTable create_table;

    EXPECT_EQ(create_table.primarykeys().size(), 0);
    EXPECT_EQ(toString(create_table.primarykeys()), "");

    Column* column1 = create_table.add_primarykeys();
    column1->set_columnname("testColumn1");
    
    ColumnDataType* column_data_type1 = column1->mutable_columndatatype();
    column_data_type1->set_scalartype(ColumnDataType::STRING);
    column_data_type1->set_length(200);
    column_data_type1->set_lengthtype(ColumnDataType::UNBOUND);
    column_data_type1->set_isarray(false);
    column1->set_isnotnull(true);
    column1->set_allowcommittimestamp(true);

    Column* column2 = create_table.add_primarykeys();
    column2->set_columnname("testColumn2");
    
    ColumnDataType* column_data_type2 = column2->mutable_columndatatype();
    column_data_type2->set_scalartype(ColumnDataType::BOOL);
    column_data_type2->set_length(2);
    column_data_type2->set_lengthtype(ColumnDataType::MAX);
    column_data_type2->set_isarray(true);
    column2->set_isnotnull(false);
    column2->set_allowcommittimestamp(false);

    EXPECT_EQ(create_table.primarykeys().size(), 2);

    EXPECT_EQ(toString(create_table.primarykeys()), 
        "testColumn1 STRING( 200 ) NOT NULL "
        "OPTIONS ( allow_commit_timestamp = true ),"
        "testColumn2 ARRAY< BOOL >  "
        "OPTIONS ( allow_commit_timestamp = null )"
    );
}

TEST(DDLStatementProtoToString, ColumnToStringTest) {
    Column column;
    column.set_columnname("testColumn");
    
    ColumnDataType* column_data_type = column.mutable_columndatatype();

    EXPECT_EQ(column_data_type->has_scalartype(), false);
    EXPECT_EQ(column_data_type->has_length(), false);
    EXPECT_EQ(column_data_type->has_lengthtype(), false);
    EXPECT_EQ(column_data_type->has_isarray(), false);

    column_data_type->set_scalartype(ColumnDataType::STRING);
    column_data_type->set_length(200);
    column_data_type->set_lengthtype(ColumnDataType::BOUND);
    column_data_type->set_isarray(false);

    column.set_isnotnull(true);
    column.set_allowcommittimestamp(true);

    EXPECT_EQ(toString(column), 
        "testColumn STRING( 201 ) NOT NULL "
        "OPTIONS ( allow_commit_timestamp = true )"
    );
}

TEST(DDLStatementProtoToString, ColumnInfoDataToStringTest) {
    ColumnDataType column_data_type;

    EXPECT_EQ(column_data_type.has_scalartype(), false);
    EXPECT_EQ(column_data_type.has_length(), false);
    EXPECT_EQ(column_data_type.has_lengthtype(), false);
    EXPECT_EQ(column_data_type.has_isarray(), false);

    column_data_type.set_scalartype(ColumnDataType::STRING);
    column_data_type.set_length(200);
    column_data_type.set_lengthtype(ColumnDataType::BOUND);

    column_data_type.set_isarray(false);
    EXPECT_EQ(toString(column_data_type), "STRING( 201 )");  

    column_data_type.set_isarray(true);
    EXPECT_EQ(toString(column_data_type), "ARRAY< STRING( 201 ) >");  
}

TEST(DDLStatementProtoToString, ColumnDataTypeToStringHelperTest) {
    ColumnDataType column_data_type;

    EXPECT_EQ(column_data_type.has_scalartype(), false);
    EXPECT_EQ(column_data_type.has_length(), false);
    EXPECT_EQ(column_data_type.has_lengthtype(), false);

    column_data_type.set_scalartype(ColumnDataType::BOOL);
    column_data_type.set_length(-500);
    column_data_type.set_lengthtype(ColumnDataType::UNBOUND);
    EXPECT_EQ(
        toString(
            column_data_type.scalartype(), 
            column_data_type.length(), 
            column_data_type.lengthtype()
        ), 
        "BOOL"
    );

    column_data_type.set_scalartype(ColumnDataType::INT64);
    EXPECT_EQ(
        toString(
            column_data_type.scalartype(), 
            column_data_type.length(), 
            column_data_type.lengthtype()), 
        "INT64"
    );
    
    column_data_type.set_scalartype(ColumnDataType::FLOAT64);
    EXPECT_EQ(
        toString(
            column_data_type.scalartype(), 
            column_data_type.length(), 
            column_data_type.lengthtype()), 
        "FLOAT64"
    );

    column_data_type.set_scalartype(ColumnDataType::DATE);
    EXPECT_EQ(
        toString(
            column_data_type.scalartype(), 
            column_data_type.length(), 
            column_data_type.lengthtype()), 
        "DATE"
    );

    column_data_type.set_scalartype(ColumnDataType::TIMESTAMP);
    EXPECT_EQ(
        toString(
            column_data_type.scalartype(), 
            column_data_type.length(), 
            column_data_type.lengthtype()), 
        "TIMESTAMP"
    );

    column_data_type.set_scalartype(ColumnDataType::STRING);
    EXPECT_EQ(
        toString(
            column_data_type.scalartype(), 
            column_data_type.length(), 
            column_data_type.lengthtype()), 
        "STRING( -500 )"
    );
    column_data_type.set_lengthtype(ColumnDataType::MAX);
    EXPECT_EQ(
        toString(
            column_data_type.scalartype(), 
            column_data_type.length(), 
            column_data_type.lengthtype()), 
        "STRING( MAX )"
    );
    column_data_type.set_lengthtype(ColumnDataType::BOUND);
    EXPECT_EQ(
        toString(
            column_data_type.scalartype(), 
            column_data_type.length(), 
            column_data_type.lengthtype()), 
        "STRING( 501 )"
    );
    // test that the max wraps back to 1
    column_data_type.set_length(2621440);
    EXPECT_EQ(
        toString(
            column_data_type.scalartype(), 
            column_data_type.length(), 
            column_data_type.lengthtype()), 
        "STRING( 1 )"
    );

    column_data_type.set_length(-500);
    column_data_type.set_lengthtype(ColumnDataType::UNBOUND);
    column_data_type.set_scalartype(ColumnDataType::BYTES);
    EXPECT_EQ(
        toString(
            column_data_type.scalartype(), 
            column_data_type.length(), 
            column_data_type.lengthtype()), 
        "BYTES( -500 )"
    );
    column_data_type.set_lengthtype(ColumnDataType::MAX);
    EXPECT_EQ(
        toString(
            column_data_type.scalartype(), 
            column_data_type.length(), 
            column_data_type.lengthtype()), 
        "BYTES( MAX )"
    );
    column_data_type.set_lengthtype(ColumnDataType::BOUND);
    EXPECT_EQ(
        toString(
            column_data_type.scalartype(), 
            column_data_type.length(), 
            column_data_type.lengthtype()), 
        "BYTES( 501 )"
    );
    // test that the max wraps back to 1
    column_data_type.set_length(10485760);
    EXPECT_EQ(
        toString(
            column_data_type.scalartype(), 
            column_data_type.length(), 
            column_data_type.lengthtype()), 
        "BYTES( 1 )"
    );
}

TEST(DDLStatementProtoToString, IsColumnNotNullToStringTest) {
    EXPECT_EQ(isColumnNotNullToString(true), "NOT NULL");
    EXPECT_EQ(isColumnNotNullToString(false), "");
}

TEST(DDLStatementProtoToString, ColumnOptionsToStringTest) {
    EXPECT_EQ(
        columnOptionsToString(false), 
        "OPTIONS ( allow_commit_timestamp = null )"
    );
    EXPECT_EQ(
        columnOptionsToString(true), 
        "OPTIONS ( allow_commit_timestamp = true )"
    );
}

TEST(DDLStatementProtoToString, ToPrimaryKeysTest) {
    CreateTable create_table;

    Column* column1 = create_table.add_primarykeys();
    EXPECT_EQ(column1->has_columnname(), false);
    EXPECT_EQ(column1->has_orientation(), false);

    column1->set_orientation(Column::ASC);
    column1->set_columnname("testColumn1");

    Column* column2 = create_table.add_primarykeys();
    EXPECT_EQ(column2->has_columnname(), false);
    EXPECT_EQ(column2->has_orientation(), false);

    column2->set_orientation(Column::DESC);
    column2->set_columnname("testColumn2");

    EXPECT_EQ(create_table.primarykeys().size(), 2);

    EXPECT_EQ(
        toPrimaryKeys(create_table.primarykeys()), 
        "testColumn1 ASC,testColumn2 DESC");
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
