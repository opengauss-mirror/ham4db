/*
	Copyright 2021 SANGFOR TECHNOLOGIES

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
package db

import (
	"database/sql"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	test "gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
)

func TestGetDBClient(t *testing.T) {
	var err error
	var db *sql.DB
	if db, err = GetDBClient(); err != nil {
		t.Fatalf("%s", err)
	}
	test.S(t).ExpectEquals(db.Stats().MaxOpenConnections, 1)
}

func TestExecSQL(t *testing.T) {
	var err error

	// normal case 1: create table
	if _, err = ExecSQL("create table test_1(id int, name varchar(30), primary key(id))"); err != nil {
		t.Fatalf("%s", err)
	}

	// normal case 2: test sql on exist table
	if _, err = ExecSQL("insert into test_1 values(?, ?)", 1, "A"); err != nil {
		t.Fatalf("%s", err)
	}
	var result sql.Result
	if result, err = ExecSQL("select * from test_1"); result != nil && err == nil {
		var rows int64
		if rows, err = result.RowsAffected(); err == nil {
			test.S(t).ExpectEquals(rows, int64(1))
		}
	}
	if err != nil {
		t.Fatalf("%s", err)
	}

	// failed case 1: wrong sql syntax
	if _, err = ExecSQL("", nil); err != nil {
		t.Fatalf("should have error here")
	}
	if _, err = ExecSQL("select1 current_timestamp", nil); err == nil {
		t.Fatalf("should have error here")
	}

	// failed case 2: test select on not exist table
	if result, err = ExecSQL("select * from test_2"); err != nil {
		test.S(t).ExpectNil(result)
	}
}

func TestExecMultiSQL(t *testing.T) {
	var err error

	// normal case 1: multi sql
	successSQL := &dtstruct.MultiSQL{
		Query: []string{
			"create table test_2(id int, name varchar(30), primary key(id))",
			"insert into test_2 values(?, ?)",
		}, Args: [][]interface{}{
			{},
			{1, "A"},
		},
	}
	test.S(t).ExpectNil(ExecMultiSQL(successSQL))
	var result sql.Result
	if result, err = ExecSQL("select * from test_2"); result != nil && err == nil {
		var rows int64
		if rows, err = result.RowsAffected(); err == nil {
			test.S(t).ExpectEquals(rows, int64(1))
		}
	}
	if err != nil {
		t.Fatalf("%s", err)
	}

	// normal case 2: rollback
	failSQL := &dtstruct.MultiSQL{
		Query: []string{
			"insert into test_2 values(?, ?)",
			"insert into test_3 values(?, ?)",
		}, Args: [][]interface{}{
			{2, "A"},
			{2, "A2"},
		},
	}
	test.S(t).ExpectNotNil(ExecMultiSQL(failSQL))
	if result, err = ExecSQL("select * from test_2"); result != nil && err == nil {
		var rows int64
		if rows, err = result.RowsAffected(); err == nil {
			test.S(t).ExpectEquals(rows, int64(1))
		}
	}
	if err != nil {
		t.Fatalf("%s", err)
	}

	// failed case 1: npe test
	if err = ExecMultiSQL(nil); err == nil {
		t.Fatalf("should have error here")
	}

	// failed case 2: test for query and args length not match
	notMatchSQL := &dtstruct.MultiSQL{
		Query: []string{
			"create table test_3(id int, name varchar(30), primary key(id))",
			"insert into test_2 values(?, ?)",
		}, Args: [][]interface{}{
			{1, "A"},
		},
	}
	test.S(t).ExpectNotNil(ExecMultiSQL(notMatchSQL))
	if result, err = ExecSQL("select * from test_2"); result != nil && err == nil {
		var rows int64
		if rows, err = result.RowsAffected(); err == nil {
			test.S(t).ExpectEquals(rows, int64(1))
		}
	}
	if err != nil {
		t.Fatalf("%s", err)
	}
}

func TestQuery(t *testing.T) {

	// normal case 1: create/insert/select
	successSQL := &dtstruct.MultiSQL{
		Query: []string{
			"create table test_3(id int, age int, primary key(id))",
			"insert into test_3 values(?, ?)",
			"insert into test_3 values(?, ?)",
		}, Args: [][]interface{}{
			{},
			{1, 1},
			{2, 1},
		},
	}
	test.S(t).ExpectNil(ExecMultiSQL(successSQL))
	sum := 0
	test.S(t).ExpectNil(Query("select * from test_3", nil, func(rowMap sqlutil.RowMap) error {
		sum += rowMap.GetInt("age")
		return nil
	}))
	test.S(t).ExpectEquals(sum, 2)

	// failed case 1: npe test
	test.S(t).ExpectNotNil(Query("", nil, nil))
}

func TestQueryBuffered(t *testing.T) {

	// normal case 1: create/insert/select
	successSQL := &dtstruct.MultiSQL{
		Query: []string{
			"create table test_4(id int, age int, primary key(id))",
			"insert into test_4 values(?, ?)",
			"insert into test_4 values(?, ?)",
		}, Args: [][]interface{}{
			{},
			{1, 1},
			{2, 2},
		},
	}
	test.S(t).ExpectNil(ExecMultiSQL(successSQL))
	sum := 0
	test.S(t).ExpectNil(QueryBuffered("select * from test_4 where age > ?", sqlutil.Args(1), func(rowMap sqlutil.RowMap) error {
		sum += rowMap.GetInt("age")
		return nil
	}))
	test.S(t).ExpectEquals(sum, 2)
}
