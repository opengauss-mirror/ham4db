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
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/db/backend"
	"gitee.com/opengauss/ham4db/go/core/db/backend/mysql"
	"gitee.com/opengauss/ham4db/go/core/db/backend/sqlite"
	"gitee.com/opengauss/ham4db/go/core/db/backend/testdb"
	"gitee.com/opengauss/ham4db/go/core/log"
)

// BackendDBHandler interface to adapt different backend database
type BackendDBHandler interface {

	// DBHandler get db client connected backend db to process crud, if err is not nil, db should be nil
	DBHandler() (db *sql.DB, err error)

	// CreateDatabase create database specified in config
	CreateDatabase() error

	// DeployCheck check if given version has already been deployed
	// return true means continue to deploy
	DeployCheck() (string, bool)

	// SchemaInit init all schema and apply patch ddl
	SchemaInit(schema []string)

	// StatementDialect refactor statement using dialect
	StatementDialect(statement string) (string, error)

	// Config config database using option param
	Config(option map[string]interface{})
}

// GetBackendDBHandler return backend db handler according to database type
func GetBackendDBHandler(dbType string) BackendDBHandler {
	switch dbType {
	case constant.BackendDBMysql:
		return &mysql.MysqlBackend{}
	case constant.BackendDBSqlite:
		return &sqlite.SQLiteBackend{}
	case constant.BackendDBOpengauss:
		return &backend.OpenGaussBackend{}
	case constant.BackendDBTestDB:
		return testdb.NewTestDB()
	default:
		log.Fatalf("backend database type %s is not supported now", dbType)
		return nil
	}
}
