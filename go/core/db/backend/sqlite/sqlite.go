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
package sqlite

import (
	"database/sql"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
)

type SQLiteBackend struct {
}

// DBHandler create a new sqlite3 in-memory database
func (s *SQLiteBackend) DBHandler() (db *sql.DB, err error) {
	return sqlite3Client()
}

// CreateDatabase do nothing for sqlite3
func (s *SQLiteBackend) CreateDatabase() error {
	return nil
}

// DeployCheck do nothing for sqlite3
func (s *SQLiteBackend) DeployCheck() (string, bool) {
	return "", true
}

// SchemaInit init database schema
func (s *SQLiteBackend) SchemaInit(schema []string) {
	var db *sql.DB
	var err error

	// check sqlite3
	if config.Config.SQLite3DataFile == "" {
		log.Fatal("miss data file for sqlite3")
	}

	// get db client
	if db, err = sqlite3Client(); db != nil && err == nil {

		// config connection
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		// init schema
		var tx *sql.Tx
		if tx, err = db.Begin(); err != nil {
			log.Fatale(err)
		}
		defer func() {
			if err != nil {
				if err = tx.Rollback(); err != nil {
					log.Fatale(err)
				}
				return
			}
			if err = tx.Commit(); err != nil {
				log.Fatale(err)
			}
		}()
		for _, ddl := range schema {
			if _, err = tx.Exec(sqlutil.ToSqlite3Dialect(ddl)); err != nil {
				log.Error("%+v; query=%+v", err, ddl)
				return
			}
		}
		return
	}
	log.Fatal("can not connect to backend sqlite on %v, %s", config.Config.SQLite3DataFile, err)
}

// StatementDialect dialect sqlite3 statement
func (s *SQLiteBackend) StatementDialect(statement string) (string, error) {
	return sqlutil.ToSqlite3Dialect(statement), nil
}

// Config nothing to do here
func (s *SQLiteBackend) Config(map[string]interface{}) {
	panic("implement me")
}

// sqlite3Client return db clientForTestDB for sqlite3 backend db
func sqlite3Client() (db *sql.DB, err error) {
	if db, _, err = sqlutil.GetGenericDB(constant.BackendDBSqlite, config.Config.SQLite3DataFile); db == nil || err != nil {
		log.Fatal("can not connect to backend sqlite on %v, %s", config.Config.SQLite3DataFile, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return
}
