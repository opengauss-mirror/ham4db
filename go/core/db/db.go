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
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
)

// GetDBClient get backend db client from cache or create it if not exist in cache.
func GetDBClient() (db *sql.DB, err error) {
	return GetBackendDBHandler(config.Config.BackendDB).DBHandler()
}

// ExecSQL will execute given sql on the backend database.
func ExecSQL(query string, args ...interface{}) (result sql.Result, err error) {

	// dialect statement
	if query, err = GetBackendDBHandler(config.Config.BackendDB).StatementDialect(query); err != nil {
		return nil, err
	}

	//  get client db and exec statement
	var db *sql.DB
	if db, err = GetDBClient(); db != nil && err == nil {
		return sqlutil.ExecNoPrepare(db, query, args...)
	}
	return nil, err
}

// ExecMultiSQL exec multi sql in one transaction.
func ExecMultiSQL(multiSQL *dtstruct.MultiSQL) (err error) {

	// check if MultiSQL is nil
	if multiSQL == nil {
		return log.Errorf("param multiSQL is nil, please check")
	}

	// check if all queries have args
	if len(multiSQL.Query) != len(multiSQL.Args) {
		return log.Errorf("query(%d):args(%d) should be 1:1", len(multiSQL.Query), len(multiSQL.Args))
	}

	// get sql client
	var db *sql.DB
	if db, err = GetDBClient(); db == nil || err != nil {
		return err
	}
	// begin transaction and exec statement, rollback or commit when done
	var tx *sql.Tx
	if tx, err = db.Begin(); err == nil {
		commit := true
		defer func() {
			if commit {
				err = tx.Commit()
			} else {
				if errr := tx.Rollback(); errr != nil {
					log.Errore(errr)
				}
			}
		}()
		for i, query := range multiSQL.Query {

			// dialect statement
			if query, err = GetBackendDBHandler(config.Config.BackendDB).StatementDialect(query); err != nil {
				commit = false
				return err
			}

			// exec statement
			if _, err = tx.Exec(query, multiSQL.Args[i]...); err != nil {
				commit = false
				return log.Errore(err)
			}
		}
		return
	}
	return log.Errore(err)
}

// Query exec query and process result using function `funcOnRow`.
func Query(query string, argsArray []interface{}, funcOnRow func(sqlutil.RowMap) error) (err error) {

	// dialect statement
	if query, err = GetBackendDBHandler(config.Config.BackendDB).StatementDialect(query); err != nil {
		return err
	}

	// get sql client
	var db *sql.DB
	if db, err = GetDBClient(); err != nil {
		return err
	}

	return sqlutil.QueryRowsMap(db, query, funcOnRow, argsArray...)
}

// QueryBuffered reads data from the database into a buffer, and only then applies the given function per row.
func QueryBuffered(query string, argsArray []interface{}, funcOnRow func(sqlutil.RowMap) error) (err error) {

	// dialect statement
	if query, err = GetBackendDBHandler(config.Config.BackendDB).StatementDialect(query); err != nil {
		return err
	}

	// get sql client
	var db *sql.DB
	if db, err = GetDBClient(); err != nil {
		return err
	}

	return sqlutil.QueryRowsMapBuffered(db, query, funcOnRow, argsArray...)
}
