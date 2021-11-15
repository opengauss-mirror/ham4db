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
package mysql

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/core/security/ssl"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"github.com/go-sql-driver/mysql"
	"time"
)

type MysqlBackend struct {
}

// errorMap key is mysql error code, if error is tolerable, value is true, otherwise is false
var errorMap map[uint16]bool

func init() {
	errorMap = make(map[uint16]bool)
	errorMap[1061] = true  // duplicate key name
	errorMap[1064] = false // you have an error in your sql syntax
	errorMap[1146] = false // table doesn't exist
}

// DBHandler get client for mysql.
func (m *MysqlBackend) DBHandler() (db *sql.DB, err error) {
	return clientForMysql()
}

// CreateDatabase create database given in config.
func (m *MysqlBackend) CreateDatabase() (err error) {
	var db *sql.DB
	if db, _, err = sqlutil.GetGenericDB(constant.BackendDBMysql, uriInit()); db != nil && err == nil {
		query := fmt.Sprintf("create database if not exists %s character set utf8mb4", config.Config.BackendDatabase)
		if _, err = db.Exec(query); err != nil {
			return log.Fatale(err)
		}
		return nil
	}
	return log.Fatale(err)
}

// DeployCheck checks if given version has already been deployed
// If there's another error to this, like DB gone bad, then we're about to find out anyway.
func (m *MysqlBackend) DeployCheck() (string, bool) {
	var db *sql.DB
	var err error
	if db, err = clientForMysql(); db != nil && err == nil {
		version := ""
		_ = db.QueryRow(
			`select deployed_version as version from ham_deployment where deployed_version = ?`,
			dtstruct.RuntimeCLIFlags.ConfiguredVersion,
		).Scan(&version)
		if version != "" && config.Config.PanicIfDifferentDatabaseDeploy && dtstruct.RuntimeCLIFlags.ConfiguredVersion != version {
			log.Fatal("PanicIfDifferentDatabaseDeploy is set. Configured version %s is not the version found in the database", dtstruct.RuntimeCLIFlags.ConfiguredVersion)
		}
		msg := version + "->" + dtstruct.RuntimeCLIFlags.ConfiguredVersion
		if _, ok := util.IsGTEVersion(dtstruct.RuntimeCLIFlags.ConfiguredVersion, version); ok {
			return msg, true
		}
		return msg, false
	}
	log.Fatale(err)
	return "", false
}

// SchemaInit will issue given sql queries that are not already known to be deployed.
// This iterates both lists (to-run and already-deployed) and also verifies no contraditions.
func (m *MysqlBackend) SchemaInit(schema []string) {
	var db *sql.DB
	var err error
	if db, err = clientForMysql(); db != nil && err == nil {
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
			if _, err = tx.Exec(ddl); err != nil {
				// use white list to avoid unknown error
				if val, ok := errorMap[err.(*mysql.MySQLError).Number]; !ok || !val {
					log.Errorf("%+v; query=%+v", err, ddl)
					return
				}
			}
		}
	}
}

// StatementDialect nothing to do for mysql
func (m *MysqlBackend) StatementDialect(statement string) (string, error) {
	return statement, nil
}

// Config nothing to do here
func (m *MysqlBackend) Config(option map[string]interface{}) {
	panic("implement me")
}

// clientForMysql return db clientForTestDB for mysql backend db
func clientForMysql() (db *sql.DB, err error) {
	fromCache := false
	if db, fromCache, err = sqlutil.GetGenericDB(constant.BackendDBMysql, uri()); db == nil || err != nil {
		return db, log.Errore(err)
	}
	if !fromCache {
		if config.Config.MaxPoolConnections > 0 {
			db.SetMaxOpenConns(config.Config.MaxPoolConnections)
		}
		if config.Config.ConnectionLifetimeSeconds > 0 {
			db.SetConnMaxLifetime(time.Duration(config.Config.ConnectionLifetimeSeconds) * time.Second)
		}
		// A low value here will trigger reconnects which could
		// make the number of backend connections hit the tcp
		// limit. That's bad.  I could make this setting dynamic
		// but then people need to know which value to use. For now
		// allow up to 25% of MySQLMaxPoolConnections
		// to be idle.  That should provide a good number which
		// does not keep the maximum number of connections open but
		// at the same time does not trigger disconnections and
		// reconnections too frequently.
		maxIdleConns := config.Config.MaxPoolConnections * 25 / 100
		if maxIdleConns < 10 {
			maxIdleConns = 10
		}
		db.SetMaxIdleConns(maxIdleConns)
	}
	return
}

// Track if a TLS has already been configured
var tlsConfigured = false

// uri get mysql uri from config using specified database
func uri() string {
	mysqlURI := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=%ds&readTimeout=%ds&rejectReadOnly=%t&interpolateParams=true",
		config.Config.BackendDBUser,
		config.Config.BackendDBPassword,
		config.Config.BackendDBHost,
		config.Config.BackendDBPort,
		config.Config.BackendDatabase,
		config.Config.ConnectTimeoutSeconds,
		config.Config.MySQLReadTimeoutSeconds,
		config.Config.MySQLRejectReadOnly,
	)
	if config.Config.MySQLUseMutualTLS {
		mysqlURI, _ = setupTls(mysqlURI)
	}
	return mysqlURI
}

// uriInit get mysql uri from config
func uriInit() string {
	mysqlURI := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=%ds&readTimeout=%ds&interpolateParams=true",
		config.Config.BackendDBUser,
		config.Config.BackendDBPassword,
		config.Config.BackendDBHost,
		config.Config.BackendDBPort,
		config.Config.ConnectTimeoutSeconds,
		config.Config.MySQLReadTimeoutSeconds,
	)
	if config.Config.MySQLUseMutualTLS {
		mysqlURI, _ = setupTls(mysqlURI)
	}
	return mysqlURI
}

// Create a TLS configuration from the config supplied CA, Certificate, and Private key.
// Register the TLS config with the mysql drivers as the "ham4db" config
// Modify the supplied URI to call the TLS config
func setupTls(uri string) (string, error) {
	if !tlsConfigured {
		tlsConfig, err := ssl.NewTLSConfig(config.Config.MySQLSSLCAFile, !config.Config.MySQLSSLSkipVerify)
		if err != nil {
			return "", log.Fatalf("can't create TLS configuration for connection %s: %s", uri, err)
		}
		// Drop to TLS 1.0 for talking to MySQL
		tlsConfig.MinVersion = tls.VersionTLS10
		tlsConfig.InsecureSkipVerify = config.Config.MySQLSSLSkipVerify
		if (!config.Config.MySQLSSLSkipVerify) &&
			config.Config.MySQLSSLCertFile != "" &&
			config.Config.MySQLSSLPrivateKeyFile != "" {
			if err = ssl.AppendKeyPair(tlsConfig, config.Config.MySQLSSLCertFile, config.Config.MySQLSSLPrivateKeyFile); err != nil {
				return "", log.Fatalf("can't setup tls key pairs for %s: %s", uri, err)
			}
		}
		if err = mysql.RegisterTLSConfig(constant.WhoAmI, tlsConfig); err != nil {
			return "", log.Fatalf("can't register mysql tls config, error: %s", err)
		}
		tlsConfigured = true
	}
	return fmt.Sprintf("%s&tls="+constant.WhoAmI, uri), nil
}
