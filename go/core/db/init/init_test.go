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
package init

import (
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	db2 "gitee.com/opengauss/ham4db/go/test/testdb/db"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"gitee.com/opengauss/ham4db/go/util/tests"
	"strings"
	"testing"
)

func TestInitBackendDB(t *testing.T) {

	// normal case 1: simulate lower version has bee deployed
	dtstruct.RuntimeCLIFlags.ConfiguredVersion = "11.0.0"
	if err := BackendDBInit(); err != nil {
		t.Fatalf("%s", err)
	}
	count := 0
	if err := db.Query("select count(*) as cnt from ham_deployment", nil, func(rowMap sqlutil.RowMap) error {
		count = rowMap.GetInt("cnt")
		return nil
	}); err != nil {
		t.Errorf("%s", err)
	}
	tests.S(t).ExpectEquals(1, count)

	// normal case 2: upgrade
	dtstruct.RuntimeCLIFlags.ConfiguredVersion = "12.0.0"
	db2.HookPatch = "create table testdb_new(id int not null, primary key(id))"
	if err := BackendDBInit(); err != nil {
		t.Fatalf("%s", err)
	}
	count = 0
	if err := db.Query("select count(*) as cnt from testdb_new", nil, func(rowMap sqlutil.RowMap) error {
		count = rowMap.GetInt("cnt")
		return nil
	}); err != nil {
		t.Errorf("%s", err)
	}
	tests.S(t).ExpectEquals(0, count)

	// failed case 1: simulate higher version has bee deployed
	dtstruct.RuntimeCLIFlags.ConfiguredVersion = "9.9.9"
	if err := BackendDBInit(); err == nil {
		t.Fatal("should have error here")
	}
	count = 0
	if err := db.Query("select count(*) as cnt from ham_deployment", nil, func(rowMap sqlutil.RowMap) error {
		count = rowMap.GetInt("cnt")
		return nil
	}); err != nil && !strings.Contains(err.Error(), "no such table") {
		t.Errorf("%s", err)
	}
	tests.S(t).ExpectEquals(2, count)
}
