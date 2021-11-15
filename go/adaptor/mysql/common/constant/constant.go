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

const (
	Error3159 = "Error 3159:"
	Error1045 = "Access denied for user"
)

type MasterRecoveryType string

const DBTMysql = "mysql"

const (
	Error1045AccessDenied  = "Error 1045: Access denied for user"
	ErrorConnectionRefused = "getsockopt: connection refused"
	ErrorNoSuchHost        = "no such host"
	ErrorIOTimeout         = "i/o timeout"
)

const (
	NotMasterRecovery          MasterRecoveryType = "NotMasterRecovery"
	MasterRecoveryGTID                            = "MasterRecoveryGTID"
	MasterRecoveryPseudoGTID                      = "MasterRecoveryPseudoGTID"
	MasterRecoveryBinlogServer                    = "MasterRecoveryBinlogServer"
)

const MysqlDefaultQuery = `
		select
			*,
			unix_timestamp() - unix_timestamp(last_checked_timestamp) as seconds_since_last_checked,
			ifnull(last_checked_timestamp <= last_seen_timestamp, 0) as is_last_check_valid,
			unix_timestamp() - unix_timestamp(last_seen_timestamp) as seconds_since_last_seen,
			ham_database_instance_candidate.last_suggested_timestamp is not null
				 and ham_database_instance_candidate.promotion_rule in ('must', 'prefer') as is_candidate,
			ifnull(nullif(ham_database_instance_candidate.promotion_rule, ''), 'neutral') as promotion_rule,
			ifnull(unresolved_hostname, '') as unresolved_hostname,
			(ham_database_instance_downtime.downtime_active is not null and ifnull(ham_database_instance_downtime.end_timestamp, now()) > now()) as is_downtimed,
			ifnull(ham_database_instance_downtime.reason, '') as downtime_reason,
			ifnull(ham_database_instance_downtime.owner, '') as downtime_owner,
			ifnull(unix_timestamp() - unix_timestamp(begin_timestamp), 0) as elapsed_downtime_seconds,
			ifnull(ham_database_instance_downtime.end_timestamp, '') as downtime_end_timestamp
		from
			ham_database_instance
			left join mysql_database_instance on (ham_database_instance.hostname = mysql_database_instance.di_hostname and ham_database_instance.port = mysql_database_instance.di_port)
			left join ham_database_instance_candidate using (hostname, port, db_type, cluster_id)
			left join ham_hostname_unresolved using (hostname)
			left join ham_database_instance_downtime using (hostname, port)
`

const (
	// Group member roles
	GroupReplicationMemberRolePrimary   = "PRIMARY"
	GroupReplicationMemberRoleSecondary = "SECONDARY"
	// Group member states
	GroupReplicationMemberStateOnline     = "ONLINE"
	GroupReplicationMemberStateRecovering = "RECOVERING"
	GroupReplicationMemberStateOffline    = "OFFLINE"
	GroupReplicationMemberStateError      = "ERROR"
)

type OperationGTIDHint string

const (
	GTIDHintDeny    OperationGTIDHint = "NoGTID"
	GTIDHintNeutral                   = "GTIDHintNeutral"
	GTIDHintForce                     = "GTIDHintForce"
)

const (
	Error1201CouldnotInitializeMasterInfoStructure = "Error 1201:"
)

const (
	BackendDBConcurrency = 20
)

type ReplicationThreadState int

const (
	ReplicationThreadStateNoThread ReplicationThreadState = -1
	ReplicationThreadStateStopped                         = 0
	ReplicationThreadStateRunning                         = 1
	ReplicationThreadStateOther                           = 2
)

// The event type to filter out
const AnonymousGTIDNextEvent = "SET @@SESSION.GTID_NEXT= 'ANONYMOUS'"
