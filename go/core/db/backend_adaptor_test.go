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
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/db/backend"
	"gitee.com/opengauss/ham4db/go/core/db/backend/mysql"
	"gitee.com/opengauss/ham4db/go/core/db/backend/sqlite"
	"gitee.com/opengauss/ham4db/go/core/db/backend/testdb"
	test "gitee.com/opengauss/ham4db/go/util/tests"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestGetBackendDBHandler(t *testing.T) {

	// normal case 1: exist database type
	test.S(t).ExpectEquals(reflect.TypeOf(GetBackendDBHandler(constant.BackendDBMysql)), reflect.TypeOf(&mysql.MysqlBackend{}))
	test.S(t).ExpectEquals(reflect.TypeOf(GetBackendDBHandler(constant.BackendDBOpengauss)), reflect.TypeOf(&backend.OpenGaussBackend{}))
	test.S(t).ExpectEquals(reflect.TypeOf(GetBackendDBHandler(constant.BackendDBSqlite)), reflect.TypeOf(&sqlite.SQLiteBackend{}))
	test.S(t).ExpectEquals(reflect.TypeOf(GetBackendDBHandler(constant.BackendDBTestDB)), reflect.TypeOf(&testdb.TestDBBackend{}))

	// failed case 1: not exist database type
	test.S(t).ExpectTrue(GetBackendDBHandler(strconv.Itoa(time.Now().Nanosecond())) == nil)
}
