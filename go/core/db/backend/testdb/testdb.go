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
package testdb

import (
	"database/sql"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"os"
	"strings"
)

type TestDBBackend struct {
	isSimulateError bool
	path            string // database path
}

// DBHandler create a new sqlite3 in-memory database
func (t *TestDBBackend) DBHandler() (db *sql.DB, err error) {
	if t.isSimulateError {
		return nil, log.Errorf("get error when create testdb client")
	}
	return t.ClientForTestDB()
}

// CreateDatabase do nothing for test db
func (t *TestDBBackend) CreateDatabase() error {
	if t.isSimulateError {
		return log.Errorf("failed to create database")
	}
	return nil
}

// DeployCheck check version
func (t *TestDBBackend) DeployCheck() (msg string, ok bool) {
	_, ok = util.IsGTEVersion(dtstruct.RuntimeCLIFlags.ConfiguredVersion, constant.DefaultVersion)
	return
}

// SchemaInit init database schema
func (t *TestDBBackend) SchemaInit(schema []string) {
	var db *sql.DB
	var err error
	if db, err = t.ClientForTestDB(); db != nil && err == nil {
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
		for _, sqls := range schema {
			if _, err = tx.Exec(sqlutil.ToSqlite3Dialect(sqls)); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
				log.Errorf("%+v; query=%+v", err, sqls)
				return
			}
		}
	}
}

// StatementDialect dialect sqlite3 statement
func (t *TestDBBackend) StatementDialect(statement string) (string, error) {
	if t.isSimulateError {
		return "", log.Errorf("dialect get error")
	}
	return sqlutil.ToSqlite3Dialect(statement), nil
}

// Config config simulate error
func (t *TestDBBackend) Config(option map[string]interface{}) {
	if val, ok := option[constant.TestDBOptionSimulateError]; ok {
		t.isSimulateError = val.(bool)
	}
}

// ClientForTestDB return sqlite3 client for test db
func (t *TestDBBackend) ClientForTestDB() (db *sql.DB, err error) {
	if db, _, err = sqlutil.GetGenericDB(constant.BackendDBSqlite, t.path); db == nil || err != nil {
		log.Fatalf("can not connect to backend sqlite on %v, %s", t.path, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return
}

// for purpose to reuse in same test
var path = strings.Join([]string{constant.TestDBDir, util.RandomHash()}, string(os.PathSeparator))

// NewTestDB create test db
func NewTestDB() *TestDBBackend {
	return &TestDBBackend{path: path}
}
