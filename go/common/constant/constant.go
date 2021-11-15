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
package constant

import (
	"time"
)

const (
	// app config
	WhoAmI = "ham4db"

	// default config
	DefaultBackendDB         = BackendDBTestDB // used test db as default backend db
	DefaultEnabledAdaptor    = TestDB          // used test db as default enabled adaptor
	DefaultVersion           = "10.10.10"      // default ham4db version for test db
	DefaultStatusAPIEndpoint = "/api/status"

	// backend database type
	BackendDBMysql     = "mysql"
	BackendDBOpengauss = "opengauss"
	BackendDBSqlite    = "sqlite3"
	BackendDBTestDB    = TestDB

	// test backend database
	TestDB                    = "testdb"        // backend database name
	TestDBDir                 = "/tmp/ham4db"   // tmp database data file directory
	TestDBOptionSimulateError = "simulateError" // setting used to decide to simulate error or not

	// retry
	RetryInterval = 500 * time.Millisecond // retry interval
	RetryFunction = 5                      // max retry times when function exec failed

	// concurrency
	ConcurrencyBackendDBWrite    = 20  // max concurrency for back end db write
	ConcurrencyBackendDBRead     = 20  // max concurrency for back end db read
	ConcurrencyTopologyOperation = 128 // Max concurrency for bulk topology operations

	//
	ipv4Regexp         = "^([0-9]+)[.]([0-9]+)[.]([0-9]+)[.]([0-9]+)$"
	ipv4HostPortRegexp = "^([^:]+):([0-9]+)$"
	ipv4HostRegexp     = "^([^:]+)$"
	ipv6HostPortRegexp = "^\\[([:0-9a-fA-F]+)\\]:([0-9]+)$" // e.g. [2001:db8:1f70::999:de8:7648:6e8]:3308
	ipv6HostRegexp     = "^([:0-9a-fA-F]+)$"                // e.g. 2001:db8:1f70::999:de8:7648:6e8

	// grpc
	GrpcTimeout = 20 //second

	// date format
	DateFormat         = "2006-01-02 15:04:05"  // format date data when get from database
	DateFormatTimeZone = "2006-01-02T15:04:05Z" // format date data when get from database
	DateFormatLog      = "2006-01-02 15:04:05"

	// os
	OSTempPath       = "/tmp/ham4db"
	OSTempFilePatten = "ham4db-temp-" // temp file patten: prefix as the part before "*" and suffix as the part after "*"

	// query
	DefaultQuery = `
		select
			*,
			unix_timestamp() - unix_timestamp(last_checked_timestamp) as seconds_since_last_checked,
			last_checked_timestamp <= last_seen_timestamp as is_last_check_valid,
			unix_timestamp() - unix_timestamp(last_seen_timestamp) as seconds_since_last_seen,
			(
				ham_database_instance_candidate.last_suggested_timestamp is not null and 
				ham_database_instance_candidate.promotion_rule in ('must', 'prefer') 
			) as is_candidate,
			ifnull(nullif(ham_database_instance_candidate.promotion_rule, ''), 'neutral') as promotion_rule,
			ifnull(unresolved_hostname, '') as unresolved_hostname,
			(ham_database_instance_downtime.downtime_active is not null and ifnull(ham_database_instance_downtime.end_timestamp, now()) > now()) as is_downtimed,
			ifnull(ham_database_instance_downtime.reason, '') as downtime_reason,
			ifnull(ham_database_instance_downtime.owner, '') as downtime_owner,
			unix_timestamp() - unix_timestamp(begin_timestamp) as elapsed_downtime_seconds,
			ifnull(ham_database_instance_downtime.end_timestamp, '') as downtime_end_timestamp
		from
			ham_database_instance
			left join ham_database_instance_candidate using (hostname, port, cluster_id, db_type)
			left join ham_hostname_unresolved using (hostname)
			left join ham_database_instance_downtime using (hostname, port)
	`
	DefaultQueryFuzzy       = `select db_type, hostname, port, cluster_id from ham_database_instance`
	DefaultQueryClusterName = `select cluster_name from ham_database_instance`

	// downtime
	DowntimeReasonLostInRecovery     = "lost-in-recovery"
	DowntimeSecond               int = 60 * 60 * 24 * 365

	// token
	TokenShortLength = 8 // used to get short token

	// random string
	RandomChars = "0123456789abcdefghijklmnopqurstuvwxyzABCDEFGHIJKLMNOPQURSTUVWXYZ"
)

type LogType int

const (
	BinaryLog LogType = iota
	RelayLog
)

type StopReplicationMethod string

const (
	NoStopReplication     StopReplicationMethod = "NoStopReplication"
	StopReplicationNormal                       = "StopReplicationNormal"
	StopReplicationNice                         = "StopReplicationNice"
)
