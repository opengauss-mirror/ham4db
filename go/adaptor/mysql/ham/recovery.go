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
package ham

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	mconstant "gitee.com/opengauss/ham4db/go/adaptor/mysql/common/constant"
	mdtstruct "gitee.com/opengauss/ham4db/go/adaptor/mysql/dtstruct"
	mutil "gitee.com/opengauss/ham4db/go/adaptor/mysql/util"
	cconstant "gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/base"
	cache2 "gitee.com/opengauss/ham4db/go/core/cache"
	orcraft "gitee.com/opengauss/ham4db/go/core/consensus/raft"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/kv"
	"gitee.com/opengauss/ham4db/go/core/limiter"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/core/system/osp"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"github.com/go-sql-driver/mysql"
	"github.com/patrickmn/go-cache"
	"github.com/rcrowley/go-metrics"
	"github.com/sjmudd/stopwatch"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"time"
)

var clusterInjectedPseudoGTIDCache = cache.New(time.Minute, time.Second)
var countRetries = 5

var recoverDeadReplicationGroupMemberCounter = metrics.NewCounter()
var recoverDeadReplicationGroupMemberSuccessCounter = metrics.NewCounter()
var recoverDeadReplicationGroupMemberFailureCounter = metrics.NewCounter()
var recoverDeadIntermediateMasterSuccessCounter = metrics.NewCounter()
var recoverDeadIntermediateMasterFailureCounter = metrics.NewCounter()
var recoverDeadCoMasterCounter = metrics.NewCounter()
var recoverDeadCoMasterSuccessCounter = metrics.NewCounter()
var recoverDeadCoMasterFailureCounter = metrics.NewCounter()
var recoverDeadMasterCounter = metrics.NewCounter()
var recoverDeadMasterSuccessCounter = metrics.NewCounter()
var recoverDeadMasterFailureCounter = metrics.NewCounter()

var emergencyReadTopologyInstanceMap = cache.New(time.Second, time.Millisecond*250)
var emergencyOperationGracefulPeriodMap = cache.New(time.Second*5, time.Millisecond*500)
var emergencyRestartReplicaTopologyInstanceMap = cache.New(time.Second*30, time.Second)

// We use this map to identify whether the query failed because the server does not support group replication or due
// to a different reason.
var GroupReplicationNotSupportedErrors = map[uint16]bool{
	// If either the group replication global variables are not known or the
	// performance_schema.replication_group_members table does not exist, the host does not support group
	// replication, at least in the form supported here.
	1193: true, // ERROR: 1193 (HY000): Unknown system variable 'group_replication_group_name'
	1146: true, // ERROR: 1146 (42S02): Table 'performance_schema.replication_group_members' doesn't exist
}

func init() {
	metrics.Register("mysql.recover.dead_replication_group_member.start", recoverDeadReplicationGroupMemberCounter)
	metrics.Register("mysql.recover.dead_replication_group_member.success", recoverDeadReplicationGroupMemberSuccessCounter)
	metrics.Register("mysql.recover.dead_replication_group_member.fail", recoverDeadReplicationGroupMemberFailureCounter)
	metrics.Register("mysql.recover.dead_intermediate_master.success", recoverDeadIntermediateMasterSuccessCounter)
	metrics.Register("mysql.recover.dead_intermediate_master.fail", recoverDeadIntermediateMasterFailureCounter)
	metrics.Register("mysql.recover.dead_co_master.start", recoverDeadCoMasterCounter)
	metrics.Register("mysql.recover.dead_co_master.success", recoverDeadCoMasterSuccessCounter)
	metrics.Register("mysql.recover.dead_co_master.fail", recoverDeadCoMasterFailureCounter)
	metrics.Register("mysql.recover.dead_master.start", recoverDeadMasterCounter)
	metrics.Register("mysql.recover.dead_master.success", recoverDeadMasterSuccessCounter)
	metrics.Register("mysql.recover.dead_master.fail", recoverDeadMasterFailureCounter)
}

// GetReplicationAnalysis will check for replication problems (dead master; unreachable master; etc)
func GetReplicationAnalysis(clusterName, clusterId string, hints *dtstruct.ReplicationAnalysisHints) ([]dtstruct.ReplicationAnalysis, error) {
	result := []dtstruct.ReplicationAnalysis{}
	analysisQueryClusterClause := ""
	if clusterId != "" {
		analysisQueryClusterClause = fmt.Sprintf("AND master_instance.cluster_id = '%s'", clusterId)
	}
	analysisQueryReductionClause := ``
	if config.Config.ReduceReplicationAnalysisCount {
		analysisQueryReductionClause = `
			HAVING
				(
					MIN(
						master_instance.last_checked_timestamp <= master_instance.last_seen_timestamp
				and master_instance.last_attempted_check_timestamp <= master_instance.last_seen_timestamp + interval ? second
				) = 1
				/* AS is_last_check_valid */
				) = 0
				OR (
					IFNULL(
						SUM(
							replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp
				AND replica_instance.slave_io_running = 0
				AND replica_instance.last_io_error like '%error %connecting to master%'
				AND replica_instance.slave_sql_running = 1
				),
				0
				)
				/* AS count_replicas_failing_to_connect_to_master */
				> 0
				)
				OR (
					IFNULL(
						SUM(
							replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp
						),
						0
					)
				/* AS count_valid_replicas */
				< COUNT(replica_instance.db_id)
				/* AS count_replicas */
				)
				OR (
					IFNULL(
						SUM(
							replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp
				AND replica_instance.slave_io_running != 0
				AND replica_instance.slave_sql_running != 0
				),
				0
				)
				/* AS count_valid_replicating_replicas */
				< COUNT(replica_instance.db_id)
				/* AS count_replicas */
				)
				OR (
					MIN(
						master_instance.slave_sql_running = 1
				AND master_instance.slave_io_running = 0
				AND master_instance.last_io_error like '%error %connecting to master%'
				)
				/* AS is_failing_to_connect_to_master */
			)
				OR (
					COUNT(replica_instance.db_id)
				/* AS count_replicas */
				> 0
				)
`
	}
	// "OR count_replicas > 0" above is a recent addition, which, granted, makes some previous conditions redundant.
	// It gives more output, and more "NoProblem" messages that I am now interested in for purpose of auditing in database_instance_analysis_changelog
	query := fmt.Sprintf(`
	SELECT
		master_instance.db_type,
		master_instance.hostname,
		master_instance.port,
		master_instance.cluster_id,
		master_instance.is_read_only AS read_only,
		MIN(master_instance.pl_data_center) AS pl_data_center,
		MIN(master_instance.pl_region) AS pl_region,
		MIN(master_instance.upstream_host) AS upstream_host,
		MIN(master_instance.upstream_port) AS upstream_port,
		MIN(master_instance.cluster_name) AS cluster_name,
		MIN(master_instance.binary_log_file) AS binary_log_file,
		MIN(master_instance.binary_log_pos) AS binary_log_pos,
		MIN(
			IFNULL(
				master_instance.binary_log_file = mysql_database_instance_stale_binlog_coordinate.binary_log_file
				AND master_instance.binary_log_pos = mysql_database_instance_stale_binlog_coordinate.binary_log_pos
				AND mysql_database_instance_stale_binlog_coordinate.first_seen_timestamp < NOW() - interval ? second,
				0
			)
		) AS is_stale_binlog_coordinates,
		MIN(
			IFNULL(
				ham_cluster_alias.alias,
				master_instance.cluster_name
			)
		) AS cluster_alias,
		MIN(
			IFNULL(
				ham_cluster_domain_name.domain_name,
				master_instance.cluster_name
			)
		) AS cluster_domain,
		MIN(
			master_instance.last_checked_timestamp <= master_instance.last_seen_timestamp
			and master_instance.last_attempted_check_timestamp <= master_instance.last_seen_timestamp + interval ? second
		) = 1 AS is_last_check_valid,
		/* To be considered a master, traditional async replication must not be present/valid AND the host should either */
		/* not be a replication group member OR be the primary of the replication group */
		MIN(master_instance.is_last_check_partial_success) as is_last_check_partial_success,
		MIN(
			(
				master_instance.upstream_host IN ('', '_')
				OR master_instance.upstream_port = 0
				OR substr(master_instance.upstream_host, 1, 2) = '//'
			)
			AND (
				master_instance.replication_group_name = ''
				OR master_instance.replication_group_member_role = 'PRIMARY'
			)
		) AS is_master,
		-- A host is not a group member if it has no replication group name OR if it does, but its state in the group is
		-- OFFLINE (e.g. some GR configuration is in place but the host has not actually joined a group yet. Notice that
		-- we DO consider it a group member if its state is ERROR (which is what happens when it gets expelled from the
		-- group)
		MIN(
			master_instance.replication_group_name != ''
			AND master_instance.replication_group_member_state != 'OFFLINE'
		) AS is_replication_group_member,
		MIN(master_instance.is_co_master) AS is_co_master,
		MIN(
			CONCAT(
				master_instance.hostname,
				':',
				master_instance.port
			) = master_instance.cluster_name
		) AS is_cluster_master,
		MIN(master_instance.gtid_mode) AS gtid_mode,
		COUNT(replica_instance.db_id) AS count_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp
			),
			0
		) AS count_valid_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp
				AND replica_instance.slave_io_running != 0
				AND replica_instance.slave_sql_running != 0
			),
			0
		) AS count_valid_replicating_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp
				AND replica_instance.slave_io_running = 0
				AND replica_instance.last_io_error like '%%error %%connecting to master%%'
				AND replica_instance.slave_sql_running = 1
			),
			0
		) AS count_replicas_failing_to_connect_to_master,
		MIN(master_instance.replication_depth) AS replication_depth,
		GROUP_CONCAT(
			concat(
				replica_instance.Hostname,
				':',
				replica_instance.Port
			)
		) as downstream_hosts,
		MIN(
			master_instance.slave_sql_running = 1
			AND master_instance.slave_io_running = 0
			AND master_instance.last_io_error like '%%error %%connecting to master%%'
		) AS is_failing_to_connect_to_master,
		MIN(
			master_downtime.downtime_active is not null
			and ifnull(master_downtime.end_timestamp, now()) > now()
		) AS is_downtimed,
		MIN(
			IFNULL(master_downtime.end_timestamp, '')
		) AS downtime_end_timestamp,
		MIN(
			IFNULL(
				unix_timestamp() - unix_timestamp(master_downtime.end_timestamp),
				0
			)
		) AS downtime_remaining_seconds,
		MIN(
			master_instance.is_binlog_server
		) AS is_binlog_server,
		MIN(master_instance.pseudo_gtid) AS is_pseudo_gtid,
		MIN(
			master_instance.supports_oracle_gtid
		) AS supports_oracle_gtid,
		MIN(
			master_instance.semi_sync_master_enabled
		) AS semi_sync_master_enabled,
		MIN(
			master_instance.semi_sync_master_wait_for_slave_count
		) AS semi_sync_master_wait_for_slave_count,
		MIN(
			master_instance.semi_sync_master_clients
		) AS semi_sync_master_clients,
		MIN(
			master_instance.semi_sync_master_status
		) AS semi_sync_master_status,
		SUM(replica_instance.is_co_master) AS count_co_master_replicas,
		SUM(replica_instance.oracle_gtid) AS count_oracle_gtid_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp
				AND replica_instance.oracle_gtid != 0
			),
			0
		) AS count_valid_oracle_gtid_replicas,
		SUM(
			replica_instance.is_binlog_server
		) AS count_binlog_server_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp
				AND replica_instance.is_binlog_server != 0
			),
			0
		) AS count_valid_binlog_server_replicas,
		SUM(
			replica_instance.semi_sync_replica_enabled
		) AS count_semi_sync_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp
				AND replica_instance.semi_sync_replica_enabled != 0
			),
			0
		) AS count_valid_semi_sync_replicas,
		MIN(
			master_instance.mariadb_gtid
		) AS is_mariadb_gtid,
		SUM(replica_instance.mariadb_gtid) AS count_mariadb_gtid_replicas,
		IFNULL(
			SUM(
				replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp
				AND replica_instance.mariadb_gtid != 0
			),
			0
		) AS count_valid_mariadb_gtid_replicas,
		IFNULL(
			SUM(
				replica_instance.log_bin
				AND replica_instance.log_slave_updates
			),
			0
		) AS count_logging_replicas,
		IFNULL(
			SUM(
				replica_instance.log_bin
				AND replica_instance.log_slave_updates
				AND replica_instance.binlog_format = 'STATEMENT'
			),
			0
		) AS count_statement_based_logging_replicas,
		IFNULL(
			SUM(
				replica_instance.log_bin
				AND replica_instance.log_slave_updates
				AND replica_instance.binlog_format = 'MIXED'
			),
			0
		) AS count_mixed_based_logging_replicas,
		IFNULL(
			SUM(
				replica_instance.log_bin
				AND replica_instance.log_slave_updates
				AND replica_instance.binlog_format = 'ROW'
			),
			0
		) AS count_row_based_logging_replicas,
		IFNULL(
			SUM(replica_instance.sql_delay > 0),
			0
		) AS count_delayed_replicas,
		IFNULL(
			SUM(replica_instance.replication_downstream_lag > ?),
			0
		) AS count_lagging_replicas,
		IFNULL(MIN(replica_instance.gtid_mode), '') AS min_replica_gtid_mode,
		IFNULL(MAX(replica_instance.gtid_mode), '') AS max_replica_gtid_mode,
		IFNULL(
			MAX(
				case when replica_downtime.downtime_active is not null
				and ifnull(replica_downtime.end_timestamp, now()) > now() then '' else replica_instance.gtid_errant end
			),
			''
		) AS max_replica_gtid_errant,
		IFNULL(
			SUM(
				replica_downtime.downtime_active is not null
				and ifnull(replica_downtime.end_timestamp, now()) > now()
			),
			0
		) AS count_downtimed_replicas,
		COUNT(
			DISTINCT case when replica_instance.log_bin
			AND replica_instance.log_slave_updates then replica_instance.version_major else NULL end
		) AS count_distinct_logging_major_versions
	FROM
		((select * from ham_database_instance LEFT JOIN mysql_database_instance ON (
			ham_database_instance.hostname = mysql_database_instance.di_hostname
			AND ham_database_instance.port = mysql_database_instance.di_port
		)) as master_instance)
		LEFT JOIN ham_hostname_resolve ON (
			master_instance.hostname = ham_hostname_resolve.hostname
		)
		LEFT JOIN 		((select * from ham_database_instance LEFT JOIN mysql_database_instance ON (
			ham_database_instance.hostname = mysql_database_instance.di_hostname
			AND ham_database_instance.port = mysql_database_instance.di_port
		)) as replica_instance) ON (
			COALESCE(
				ham_hostname_resolve.resolved_hostname,
				master_instance.hostname
			) = replica_instance.upstream_host
			AND master_instance.port = replica_instance.upstream_port
		)
		LEFT JOIN ham_database_instance_maintenance ON (
			master_instance.hostname = ham_database_instance_maintenance.hostname
			AND master_instance.port = ham_database_instance_maintenance.port
			AND ham_database_instance_maintenance.maintenance_active = 1
		)
		LEFT JOIN mysql_database_instance_stale_binlog_coordinate ON (
			master_instance.hostname = mysql_database_instance_stale_binlog_coordinate.hostname
			AND master_instance.port = mysql_database_instance_stale_binlog_coordinate.port
		)
		LEFT JOIN ham_database_instance_downtime as master_downtime ON (
			master_instance.hostname = master_downtime.hostname
			AND master_instance.port = master_downtime.port
			AND master_downtime.downtime_active = 1
		)
		LEFT JOIN ham_database_instance_downtime as replica_downtime ON (
			replica_instance.hostname = replica_downtime.hostname
			AND replica_instance.port = replica_downtime.port
			AND replica_downtime.downtime_active = 1
		)
		LEFT JOIN ham_cluster_alias ON (
			ham_cluster_alias.cluster_name = master_instance.cluster_name
		)
		LEFT JOIN ham_cluster_domain_name ON (
			ham_cluster_domain_name.cluster_name = master_instance.cluster_name
		)
	WHERE
		ham_database_instance_maintenance.maintenance_id IS NULL
		AND ? IN ('', master_instance.cluster_name)
		AND master_instance.db_type = 'mysql'
		%s
	GROUP BY
		master_instance.db_type,
		master_instance.hostname,
		master_instance.port
		%s
	ORDER BY
		is_master DESC,
		is_cluster_master DESC,
		count_replicas DESC
	`,
		analysisQueryClusterClause, analysisQueryReductionClause)

	args := sqlutil.Args(config.Config.ReasonableReplicationLagSeconds, config.ValidSecondsFromSeenToLastAttemptedCheck(), config.Config.ReasonableReplicationLagSeconds, clusterName)
	if config.Config.ReduceReplicationAnalysisCount {
		args = append(args, config.ValidSecondsFromSeenToLastAttemptedCheck())
	}

	err := db.Query(query, args, func(m sqlutil.RowMap) error {
		a := dtstruct.ReplicationAnalysis{
			Analysis:               dtstruct.NoProblem,
			ProcessingNodeHostname: osp.GetHostname(),
			ProcessingNodeToken:    dtstruct.ProcessToken.Hash,
		}

		a.IsMaster = m.GetBool("is_master")
		a.IsReplicationGroupMember = m.GetBool("is_replication_group_member")
		countCoMasterReplicas := m.GetUint("count_co_master_replicas")
		a.IsCoMaster = m.GetBool("is_co_master") || (countCoMasterReplicas > 0)
		a.AnalyzedInstanceKey = dtstruct.InstanceKey{DBType: dtstruct.GetDatabaseType(m.GetString("db_type")), Hostname: m.GetString("hostname"), Port: m.GetInt("port"), ClusterId: m.GetString("cluster_id")}
		a.AnalyzedInstanceUpstreamKey = dtstruct.InstanceKey{DBType: dtstruct.GetDatabaseType(m.GetString("db_type")), Hostname: m.GetString("upstream_host"), Port: m.GetInt("upstream_port"), ClusterId: m.GetString("cluster_id")}
		a.AnalyzedInstanceDataCenter = m.GetString("pl_data_center")
		a.AnalyzedInstanceRegion = m.GetString("pl_region")
		a.AnalyzedInstanceBinlogCoordinates = dtstruct.LogCoordinates{
			LogFile: m.GetString("binary_log_file"),
			LogPos:  m.GetInt64("binary_log_pos"),
			Type:    cconstant.BinaryLog,
		}
		isStaleBinlogCoordinates := m.GetBool("is_stale_binlog_coordinates")
		a.ClusterDetails.ClusterName = m.GetString("cluster_name")
		a.ClusterDetails.ClusterAlias = m.GetString("cluster_alias")
		a.ClusterDetails.ClusterDomain = m.GetString("cluster_domain")
		a.GTIDMode = m.GetString("gtid_mode")
		a.LastCheckValid = m.GetBool("is_last_check_valid")
		a.LastCheckPartialSuccess = m.GetBool("is_last_check_partial_success")
		a.CountReplicas = m.GetUint("count_replicas")
		a.CountValidReplicas = m.GetUint("count_valid_replicas")
		a.CountValidReplicatingReplicas = m.GetUint("count_valid_replicating_replicas")
		a.CountReplicasFailingToConnectToMaster = m.GetUint("count_replicas_failing_to_connect_to_master")
		a.CountDowntimedReplicas = m.GetUint("count_downtimed_replicas")
		a.ReplicationDepth = m.GetUint("replication_depth")
		a.IsFailingToConnectToMaster = m.GetBool("is_failing_to_connect_to_master")
		a.IsDowntimed = m.GetBool("is_downtimed")
		a.DowntimeEndTimestamp = m.GetString("downtime_end_timestamp")
		a.DowntimeRemainingSeconds = m.GetInt("downtime_remaining_seconds")
		a.IsReplicaServer = m.GetBool("is_binlog_server")
		a.ClusterDetails.ReadRecoveryInfo()

		a.Downstreams = *dtstruct.NewInstanceKeyMap()
		base.ReadCommaDelimitedList(&a.Downstreams, m.GetString("db_type"), m.GetString("downstream_hosts"))

		countValidOracleGTIDReplicas := m.GetUint("count_valid_oracle_gtid_replicas")
		a.OracleGTIDImmediateTopology = countValidOracleGTIDReplicas == a.CountValidReplicas && a.CountValidReplicas > 0
		countValidMariaDBGTIDReplicas := m.GetUint("count_valid_mariadb_gtid_replicas")
		a.MariaDBGTIDImmediateTopology = countValidMariaDBGTIDReplicas == a.CountValidReplicas && a.CountValidReplicas > 0
		countValidBinlogServerReplicas := m.GetUint("count_valid_binlog_server_replicas")
		a.BinlogServerImmediateTopology = countValidBinlogServerReplicas == a.CountValidReplicas && a.CountValidReplicas > 0
		a.PseudoGTIDImmediateTopology = m.GetBool("is_pseudo_gtid")
		a.SemiSyncMasterEnabled = m.GetBool("semi_sync_master_enabled")
		a.SemiSyncMasterStatus = m.GetBool("semi_sync_master_status")
		a.CountSemiSyncReplicasEnabled = m.GetUint("count_semi_sync_replicas")
		//countValidSemiSyncReplicasEnabled := m.GetUint("count_valid_semi_sync_replicas")
		a.SemiSyncMasterWaitForReplicaCount = m.GetUint("semi_sync_master_wait_for_slave_count")
		a.SemiSyncMasterClients = m.GetUint("semi_sync_master_clients")

		a.MinReplicaGTIDMode = m.GetString("min_replica_gtid_mode")
		a.MaxReplicaGTIDMode = m.GetString("max_replica_gtid_mode")
		a.MaxReplicaGTIDErrant = m.GetString("max_replica_gtid_errant")

		a.CountLoggingReplicas = m.GetUint("count_logging_replicas")
		a.CountStatementBasedLoggingReplicas = m.GetUint("count_statement_based_logging_replicas")
		a.CountMixedBasedLoggingReplicas = m.GetUint("count_mixed_based_logging_replicas")
		a.CountRowBasedLoggingReplicas = m.GetUint("count_row_based_logging_replicas")
		a.CountDistinctMajorVersionsLoggingReplicas = m.GetUint("count_distinct_logging_major_versions")

		a.CountDelayedReplicas = m.GetUint("count_delayed_replicas")
		a.CountLaggingReplicas = m.GetUint("count_lagging_replicas")

		a.IsReadOnly = m.GetUint("read_only") == 1

		if !a.LastCheckValid {
			analysisMessage := fmt.Sprintf("analysis: ClusterName: %+v, IsMaster: %+v, LastCheckValid: %+v, LastCheckPartialSuccess: %+v, CountReplicas: %+v, CountValidReplicas: %+v, CountValidReplicatingReplicas: %+v, CountLaggingReplicas: %+v, CountDelayedReplicas: %+v, CountReplicasFailingToConnectToMaster: %+v",
				a.ClusterDetails.ClusterName, a.IsMaster, a.LastCheckValid, a.LastCheckPartialSuccess, a.CountReplicas, a.CountValidReplicas, a.CountValidReplicatingReplicas, a.CountLaggingReplicas, a.CountDelayedReplicas, a.CountReplicasFailingToConnectToMaster,
			)
			if cache2.ClearToLog("analysis_dao", analysisMessage) {
				log.Debugf(analysisMessage)
			}
		}
		if !a.IsReplicationGroupMember /* Traditional Async/Semi-sync replication issue detection */ {
			if a.IsMaster && !a.LastCheckValid && a.CountReplicas == 0 {
				a.Analysis = dtstruct.DeadMasterWithoutReplicas
				a.Description = "Master cannot be reached by ham4db and has no replica"
				//
			} else if a.IsMaster && !a.LastCheckValid && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = dtstruct.DeadMaster
				a.Description = "Master cannot be reached by ham4db and none of its replicas is replicating"
				//
			} else if a.IsMaster && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = dtstruct.DeadMasterAndReplicas
				a.Description = "Master cannot be reached by ham4db and none of its replicas is replicating"
				//
			} else if a.IsMaster && !a.LastCheckValid && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = dtstruct.DeadMasterAndSomeReplicas
				a.Description = "Master cannot be reached by ham4db; some of its replicas are unreachable and none of its reachable replicas is replicating"
				//
			} else if a.IsMaster && !a.LastCheckValid && a.CountLaggingReplicas == a.CountReplicas && a.CountDelayedReplicas < a.CountReplicas && a.CountValidReplicatingReplicas > 0 {
				a.Analysis = dtstruct.UnreachableMasterWithLaggingReplicas
				a.Description = "Master cannot be reached by ham4db and all of its replicas are lagging"
				//
			} else if a.IsMaster && !a.LastCheckValid && !a.LastCheckPartialSuccess && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas > 0 {
				// partial success is here to redice noise
				a.Analysis = dtstruct.UnreachableMaster
				a.Description = "Master cannot be reached by ham4db but it has replicating replicas; possibly a network/host issue"
				//
			} else if a.IsMaster && !a.LastCheckValid && a.LastCheckPartialSuccess && a.CountReplicasFailingToConnectToMaster > 0 && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas > 0 {
				// there's partial success, but also at least one replica is failing to connect to master
				a.Analysis = dtstruct.UnreachableMaster
				a.Description = "Master cannot be reached by ham4db but it has replicating replicas; possibly a network/host issue"
				//
			} else if a.IsMaster && a.SemiSyncMasterEnabled && a.SemiSyncMasterStatus && a.SemiSyncMasterWaitForReplicaCount > 0 && a.SemiSyncMasterClients < a.SemiSyncMasterWaitForReplicaCount {
				if isStaleBinlogCoordinates {
					a.Analysis = dtstruct.LockedSemiSyncMaster
					a.Description = "Semi sync master is locked since it doesn't get enough replica acknowledgements"
				} else {
					a.Analysis = dtstruct.LockedSemiSyncMasterHypothesis
					a.Description = "Semi sync master seems to be locked, more samplings needed to validate"
				}
				//
			} else if a.IsMaster && a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = dtstruct.MasterSingleReplicaNotReplicating
				a.Description = "Master is reachable but its single replica is not replicating"
				//
			} else if a.IsMaster && a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == 0 {
				a.Analysis = dtstruct.MasterSingleReplicaDead
				a.Description = "Master is reachable but its single replica is dead"
				//
			} else if a.IsMaster && a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = dtstruct.AllMasterReplicasNotReplicating
				a.Description = "Master is reachable but none of its replicas is replicating"
				//
			} else if a.IsMaster && a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = dtstruct.AllMasterReplicasNotReplicatingOrDead
				a.Description = "Master is reachable but none of its replicas is replicating"
				//
			} else /* co-master */ if a.IsCoMaster && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = dtstruct.DeadCoMaster
				a.Description = "Co-master cannot be reached by ham4db and none of its replicas is replicating"
				//
			} else if a.IsCoMaster && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = dtstruct.DeadCoMasterAndSomeReplicas
				a.Description = "Co-master cannot be reached by ham4db; some of its replicas are unreachable and none of its reachable replicas is replicating"
				//
			} else if a.IsCoMaster && !a.LastCheckValid && !a.LastCheckPartialSuccess && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas > 0 {
				a.Analysis = dtstruct.UnreachableCoMaster
				a.Description = "Co-master cannot be reached by ham4db but it has replicating replicas; possibly a network/host issue"
				//
			} else if a.IsCoMaster && a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = dtstruct.AllCoMasterReplicasNotReplicating
				a.Description = "Co-master is reachable but none of its replicas is replicating"
				//
			} else /* intermediate-master */ if !a.IsMaster && !a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == a.CountReplicas && a.CountReplicasFailingToConnectToMaster == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = dtstruct.DeadIntermediateMasterWithSingleReplicaFailingToConnect
				a.Description = "Intermediate master cannot be reached by ham4db and its (single) replica is failing to connect"
				//
			} else if !a.IsMaster && !a.LastCheckValid && a.CountReplicas == 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = dtstruct.DeadIntermediateMasterWithSingleReplica
				a.Description = "Intermediate master cannot be reached by ham4db and its (single) replica is not replicating"
				//
			} else if !a.IsMaster && !a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicas == a.CountReplicas && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = dtstruct.DeadIntermediateMaster
				a.Description = "Intermediate master cannot be reached by ham4db and none of its replicas is replicating"
				//
			} else if !a.IsMaster && !a.LastCheckValid && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = dtstruct.DeadIntermediateMasterAndSomeReplicas
				a.Description = "Intermediate master cannot be reached by ham4db; some of its replicas are unreachable and none of its reachable replicas is replicating"
				//
			} else if !a.IsMaster && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == 0 {
				a.Analysis = dtstruct.DeadIntermediateMasterAndReplicas
				a.Description = "Intermediate master cannot be reached by ham4db and all of its replicas are unreachable"
				//
			} else if !a.IsMaster && !a.LastCheckValid && a.CountLaggingReplicas == a.CountReplicas && a.CountDelayedReplicas < a.CountReplicas && a.CountValidReplicatingReplicas > 0 {
				a.Analysis = dtstruct.UnreachableIntermediateMasterWithLaggingReplicas
				a.Description = "Intermediate master cannot be reached by ham4db and all of its replicas are lagging"
				//
			} else if !a.IsMaster && !a.LastCheckValid && !a.LastCheckPartialSuccess && a.CountValidReplicas > 0 && a.CountValidReplicatingReplicas > 0 {
				a.Analysis = dtstruct.UnreachableIntermediateMaster
				a.Description = "Intermediate master cannot be reached by ham4db but it has replicating replicas; possibly a network/host issue"
				//
			} else if !a.IsMaster && a.LastCheckValid && a.CountReplicas > 1 && a.CountValidReplicatingReplicas == 0 &&
				a.CountReplicasFailingToConnectToMaster > 0 && a.CountReplicasFailingToConnectToMaster == a.CountValidReplicas {
				// All replicas are either failing to connect to master (and at least one of these have to exist)
				// or completely dead.
				// Must have at least two replicas to reach such conclusion -- do note that the intermediate master is still
				// reachable to ham4db, so we base our conclusion on replicas only at this point.
				a.Analysis = dtstruct.AllIntermediateMasterReplicasFailingToConnectOrDead
				a.Description = "Intermediate master is reachable but all of its replicas are failing to connect"
				//
			} else if !a.IsMaster && a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = dtstruct.AllIntermediateMasterReplicasNotReplicating
				a.Description = "Intermediate master is reachable but none of its replicas is replicating"
				//
			} else if a.IsReplicaServer && a.IsFailingToConnectToMaster {
				a.Analysis = dtstruct.BinlogServerFailingToConnectToMaster
				a.Description = "Binlog server is unable to connect to its master"
				//
			} else if a.ReplicationDepth == 1 && a.IsFailingToConnectToMaster {
				a.Analysis = dtstruct.FirstTierReplicaFailingToConnectToMaster
				a.Description = "1st tier replica (directly replicating from topology master) is unable to connect to the master"
				//
			}
			//else if a.IsMaster && a.CountReplicas == 0 {
			//	a.Analysis = MasterWithoutReplicas
			//	a.Description = "Master has no replicas"
			//}

		} else /* Group replication issue detection */ {
			// Group member is not reachable, has replicas, and none of its reachable replicas can replicate from it
			if !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicatingReplicas == 0 {
				a.Analysis = dtstruct.DeadReplicationGroupMemberWithReplicas
				a.Description = "Group member is unreachable and all its reachable replicas are not replicating"
			}

		}
		appendAnalysis := func(analysis *dtstruct.ReplicationAnalysis) {
			if a.Analysis == dtstruct.NoProblem && len(a.StructureAnalysis) == 0 && !hints.IncludeNoProblem {
				return
			}
			for _, filter := range config.Config.RecoveryIgnoreHostnameFilters {
				if matched, _ := regexp.MatchString(filter, a.AnalyzedInstanceKey.Hostname); matched {
					return
				}
			}
			if a.IsDowntimed {
				a.SkippableDueToDowntime = true
			}
			if a.CountReplicas == a.CountDowntimedReplicas {
				switch a.Analysis {
				case dtstruct.AllMasterReplicasNotReplicating,
					dtstruct.AllMasterReplicasNotReplicatingOrDead,
					dtstruct.MasterSingleReplicaDead,
					dtstruct.AllCoMasterReplicasNotReplicating,
					dtstruct.DeadIntermediateMasterWithSingleReplica,
					dtstruct.DeadIntermediateMasterWithSingleReplicaFailingToConnect,
					dtstruct.DeadIntermediateMasterAndReplicas,
					dtstruct.DeadIntermediateMasterAndSomeReplicas,
					dtstruct.AllIntermediateMasterReplicasFailingToConnectOrDead,
					dtstruct.AllIntermediateMasterReplicasNotReplicating:
					a.IsReplicasDowntimed = true
					a.SkippableDueToDowntime = true
				}
			}
			if a.SkippableDueToDowntime && !hints.IncludeDowntimed {
				return
			}
			result = append(result, a)
		}

		{
			// Moving on to structure analysis
			// We also do structural checks. See if there's potential danger in promotions
			if a.IsMaster && a.CountLoggingReplicas == 0 && a.CountReplicas > 1 {
				a.StructureAnalysis = append(a.StructureAnalysis, dtstruct.NoLoggingReplicasStructureWarning)
			}
			if a.IsMaster && a.CountReplicas > 1 &&
				!a.OracleGTIDImmediateTopology &&
				!a.MariaDBGTIDImmediateTopology &&
				!a.BinlogServerImmediateTopology &&
				!a.PseudoGTIDImmediateTopology {
				a.StructureAnalysis = append(a.StructureAnalysis, dtstruct.NoFailoverSupportStructureWarning)
			}
			if a.IsMaster && a.CountStatementBasedLoggingReplicas > 0 && a.CountMixedBasedLoggingReplicas > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, dtstruct.StatementAndMixedLoggingReplicasStructureWarning)
			}
			if a.IsMaster && a.CountStatementBasedLoggingReplicas > 0 && a.CountRowBasedLoggingReplicas > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, dtstruct.StatementAndRowLoggingReplicasStructureWarning)
			}
			if a.IsMaster && a.CountMixedBasedLoggingReplicas > 0 && a.CountRowBasedLoggingReplicas > 0 {
				a.StructureAnalysis = append(a.StructureAnalysis, dtstruct.MixedAndRowLoggingReplicasStructureWarning)
			}
			if a.IsMaster && a.CountDistinctMajorVersionsLoggingReplicas > 1 {
				a.StructureAnalysis = append(a.StructureAnalysis, dtstruct.MultipleMajorVersionsLoggingReplicasStructureWarning)
			}

			if a.CountReplicas > 0 && (a.GTIDMode != a.MinReplicaGTIDMode || a.GTIDMode != a.MaxReplicaGTIDMode) {
				a.StructureAnalysis = append(a.StructureAnalysis, dtstruct.DifferentGTIDModesStructureWarning)
			}
			if a.MaxReplicaGTIDErrant != "" {
				a.StructureAnalysis = append(a.StructureAnalysis, dtstruct.ErrantGTIDStructureWarning)
			}

			if a.IsMaster && a.IsReadOnly {
				a.StructureAnalysis = append(a.StructureAnalysis, dtstruct.NoWriteableMasterStructureWarning)
			}

			if a.IsMaster && a.SemiSyncMasterEnabled && !a.SemiSyncMasterStatus && a.SemiSyncMasterWaitForReplicaCount > 0 && a.SemiSyncMasterClients < a.SemiSyncMasterWaitForReplicaCount {
				a.StructureAnalysis = append(a.StructureAnalysis, dtstruct.NotEnoughValidSemiSyncReplicasStructureWarning)
			}
		}
		appendAnalysis(&a)

		if a.CountReplicas > 0 && hints.AuditAnalysis {
			// Interesting enough for analysis
			go base.AuditInstanceAnalysisInChangelog(&a.AnalyzedInstanceKey, a.Analysis)
		}
		return nil
	})

	if err != nil {
		return result, log.Errore(err)
	}
	// TODO: result, err = getConcensusReplicationAnalysis(result)
	return result, log.Errore(err)
}

func IsInEmergencyOperationGracefulPeriod(instanceKey *dtstruct.InstanceKey) bool {
	_, found := emergencyOperationGracefulPeriodMap.Get(instanceKey.StringCode())
	return found
}

func GetCheckAndRecoverFunction(analysisCode dtstruct.AnalysisCode, analyzedInstanceKey *dtstruct.InstanceKey) (
	checkAndRecoverFunction func(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *dtstruct.TopologyRecovery, err error),
	isActionableRecovery bool,
) {
	switch analysisCode {
	// master
	case dtstruct.DeadMaster, dtstruct.DeadMasterAndSomeReplicas:
		if IsInEmergencyOperationGracefulPeriod(analyzedInstanceKey) {
			return CheckAndRecoverGenericProblem, false
		} else {
			return CheckAndRecoverDeadMaster, true
		}
	case dtstruct.LockedSemiSyncMaster:
		if IsInEmergencyOperationGracefulPeriod(analyzedInstanceKey) {
			return CheckAndRecoverGenericProblem, false
		} else {
			return checkAndRecoverLockedSemiSyncMaster, true
		}
	// intermediate master
	case dtstruct.DeadIntermediateMaster:
		return checkAndRecoverDeadIntermediateMaster, true
	case dtstruct.DeadIntermediateMasterAndSomeReplicas:
		return checkAndRecoverDeadIntermediateMaster, true
	case dtstruct.DeadIntermediateMasterWithSingleReplicaFailingToConnect:
		return checkAndRecoverDeadIntermediateMaster, true
	case dtstruct.AllIntermediateMasterReplicasFailingToConnectOrDead:
		return checkAndRecoverDeadIntermediateMaster, true
	case dtstruct.DeadIntermediateMasterAndReplicas:
		return CheckAndRecoverGenericProblem, false
	// co-master
	case dtstruct.DeadCoMaster:
		return checkAndRecoverDeadCoMaster, true
	case dtstruct.DeadCoMasterAndSomeReplicas:
		return checkAndRecoverDeadCoMaster, true
	// master, non actionable
	case dtstruct.DeadMasterAndReplicas:
		return CheckAndRecoverGenericProblem, false
	case dtstruct.UnreachableMaster:
		return CheckAndRecoverGenericProblem, false
	case dtstruct.UnreachableMasterWithLaggingReplicas:
		return CheckAndRecoverGenericProblem, false
	case dtstruct.AllMasterReplicasNotReplicating:
		return CheckAndRecoverGenericProblem, false
	case dtstruct.AllMasterReplicasNotReplicatingOrDead:
		return CheckAndRecoverGenericProblem, false
	case dtstruct.UnreachableIntermediateMasterWithLaggingReplicas:
		return CheckAndRecoverGenericProblem, false
	// replication group members
	case dtstruct.DeadReplicationGroupMemberWithReplicas:
		return checkAndRecoverDeadGroupMemberWithReplicas, true
	}
	// Right now this is mostly causing noise with no clear action.
	// Will revisit this in the future.
	// case inst.AllMasterReplicasStale:
	//   return logic.CheckAndRecoverGenericProblem, false

	return nil, false
}

// CheckAndRecoverGenericProblem is a general-purpose recovery function
func CheckAndRecoverGenericProblem(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *dtstruct.TopologyRecovery, error) {
	return false, nil, nil
}

// checkAndRecoverDeadCoMaster checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func checkAndRecoverDeadCoMaster(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *dtstruct.TopologyRecovery, error) {
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedMasterRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err := base.AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another RecoverDeadCoMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	// That's it! We must do recovery!
	recoverDeadCoMasterCounter.Inc(1)
	promotedReplica, lostReplicas, err := RecoverDeadCoMaster(topologyRecovery, skipProcesses)
	base.ResolveRecovery(topologyRecovery, promotedReplica)
	if promotedReplica == nil {
		base.AuditOperation("recover-dead-co-master", failedInstanceKey, "", "Failure: no replica promoted.")
	} else {
		base.AuditOperation("recover-dead-co-master", failedInstanceKey, "", fmt.Sprintf("promoted: %+v", promotedReplica.Key))
	}
	mutil.AddInstances(&topologyRecovery.LostReplicas, lostReplicas)
	if promotedReplica != nil {
		if config.Config.FailMasterPromotionIfSQLThreadNotUpToDate && !promotedReplica.SQLThreadUpToDate() {
			return false, nil, log.Errorf("Promoted replica %+v: sql thread is not up to date (relay logs still unapplied). Aborting promotion", promotedReplica.Key)
		}
		// success
		recoverDeadCoMasterSuccessCounter.Inc(1)

		if config.Config.ApplyMySQLPromotionAfterMasterFailover {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: will apply MySQL changes to promoted master"))
			SetReadOnly(&promotedReplica.Key, false)
		}
		if !skipProcesses {
			// Execute post intermediate-master-failover processes
			topologyRecovery.SuccessorKey = &promotedReplica.Key
			topologyRecovery.SuccessorAlias = promotedReplica.InstanceAlias
			base.ExecuteProcesses(config.Config.PostMasterFailoverProcesses, "PostMasterFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadCoMasterFailureCounter.Inc(1)
	}
	return true, topologyRecovery, err
}

// RecoverDeadCoMaster recovers a dead co-master, complete logic inside
func RecoverDeadCoMaster(topologyRecovery *dtstruct.TopologyRecovery, skipProcesses bool) (promotedReplica *mdtstruct.MysqlInstance, lostReplicas [](*mdtstruct.MysqlInstance), err error) {
	topologyRecovery.Type = dtstruct.CoMasterRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	otherCoMasterKey := &analysisEntry.AnalyzedInstanceUpstreamKey
	otherCoMaster, found, _ := ReadFromBackendDB(otherCoMasterKey)
	if otherCoMaster == nil || !found {
		return nil, lostReplicas, topologyRecovery.AddError(log.Errorf("RecoverDeadCoMaster: could not read info for co-master %+v of %+v", *otherCoMasterKey, *failedInstanceKey))
	}
	base.AuditOperation("recover-dead-co-master", failedInstanceKey, "", "problem found; will recover")
	if !skipProcesses {
		if err := base.ExecuteProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, lostReplicas, topologyRecovery.AddError(err)
		}
	}

	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMaster: will recover %+v", *failedInstanceKey))

	var coMasterRecoveryType = mconstant.MasterRecoveryPseudoGTID
	if analysisEntry.OracleGTIDImmediateTopology || analysisEntry.MariaDBGTIDImmediateTopology {
		coMasterRecoveryType = mconstant.MasterRecoveryGTID
	}

	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMaster: coMasterRecoveryType=%+v", coMasterRecoveryType))

	var cannotReplicateReplicas [](*mdtstruct.MysqlInstance)
	switch coMasterRecoveryType {
	case mconstant.MasterRecoveryGTID:
		{
			lostReplicas, _, cannotReplicateReplicas, promotedReplica, err = RegroupReplicasGTID(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer, nil)
		}
	case mconstant.MasterRecoveryPseudoGTID:
		{
			lostReplicas, _, _, cannotReplicateReplicas, promotedReplica, err = RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer, nil)
		}
	}
	topologyRecovery.AddError(err)
	lostReplicas = append(lostReplicas, cannotReplicateReplicas...)

	mustPromoteOtherCoMaster := config.Config.CoMasterRecoveryMustPromoteOtherCoMaster
	if !otherCoMaster.ReadOnly {
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMaster: other co-master %+v is writeable hence has to be promoted", otherCoMaster.Key))
		mustPromoteOtherCoMaster = true
	}
	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMaster: mustPromoteOtherCoMaster? %+v", mustPromoteOtherCoMaster))

	if promotedReplica != nil {
		topologyRecovery.ParticipatingInstanceKeys.AddKey(promotedReplica.Key)
		var pr dtstruct.InstanceAdaptor
		if mustPromoteOtherCoMaster {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadCoMaster: mustPromoteOtherCoMaster. Verifying that %+v is/can be promoted", *otherCoMasterKey))
			pr, err = ReplacePromotedReplicaWithCandidate(topologyRecovery, failedInstanceKey, promotedReplica, otherCoMasterKey)
		} else {
			// We are allowed to promote any server
			pr, err = ReplacePromotedReplicaWithCandidate(topologyRecovery, failedInstanceKey, promotedReplica, nil)
		}
		promotedReplica = pr.(*mdtstruct.MysqlInstance)
		topologyRecovery.AddError(err)
	}
	if promotedReplica != nil {
		if mustPromoteOtherCoMaster && !promotedReplica.Key.Equals(otherCoMasterKey) {
			topologyRecovery.AddError(log.Errorf("RecoverDeadCoMaster: could not manage to promote other-co-master %+v; was only able to promote %+v; mustPromoteOtherCoMaster is true (either CoMasterRecoveryMustPromoteOtherCoMaster is true, or co-master is writeable), therefore failing", *otherCoMasterKey, promotedReplica.Key))
			promotedReplica = nil
		}
	}
	if promotedReplica != nil {
		if config.Config.DelayMasterPromotionIfSQLThreadNotUpToDate {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Waiting to ensure the SQL thread catches up on %+v", promotedReplica.Key))
			if _, err := WaitForSQLThreadUpToDate(&promotedReplica.Key, 0, 0); err != nil {
				return promotedReplica, lostReplicas, err
			}
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("SQL thread caught up on %+v", promotedReplica.Key))
		}
		topologyRecovery.ParticipatingInstanceKeys.AddKey(promotedReplica.Key)
	}

	// OK, we may have someone promoted. Either this was the other co-master or another replica.
	// Noting down that we DO NOT attempt to set a new co-master topology. We are good with remaining with a single master.
	// I tried solving the "let's promote a replica and create a new co-master setup" but this turns so complex due to various factors.
	// I see this as risky and not worth the questionable benefit.
	// Maybe future me is a smarter person and finds a simple solution. Unlikely. I'm getting dumber.
	//
	// ...
	// Now that we're convinved, take a look at what we can be left with:
	// Say we started with M1<->M2<-S1, with M2 failing, and we promoted S1.
	// We now have M1->S1 (because S1 is promoted), S1->M2 (because that's what it remembers), M2->M1 (because that's what it remembers)
	// !! This is an evil 3-node circle that must be broken.
	// config.Config.ApplyMySQLPromotionAfterMasterFailover, if true, will cause it to break, because we would RESET SLAVE on S1
	// but we want to make sure the circle is broken no matter what.
	// So in the case we promoted not-the-other-co-master, we issue a detach-replica-master-host, which is a reversible operation
	if promotedReplica != nil && !promotedReplica.Key.Equals(otherCoMasterKey) {
		_, err = DetachMaster(&promotedReplica.Key)
		topologyRecovery.AddError(log.Errore(err))
	}

	if promotedReplica != nil && len(lostReplicas) > 0 && config.Config.DetachLostReplicasAfterMasterFailover {
		postponedFunction := func() error {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadCoMaster: lost %+v replicas during recovery process; detaching them", len(lostReplicas)))
			for _, replica := range lostReplicas {
				replica := replica
				DetachMaster(&replica.Key)
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadCoMaster, detaching %+v replicas", len(lostReplicas)))
	}

	func() error {
		base.BeginDowntime(dtstruct.NewDowntime(failedInstanceKey, dtstruct.GetMaintenanceOwner(), cconstant.DowntimeReasonLostInRecovery, time.Duration(cconstant.DowntimeSecond)*time.Second))
		base.AcknowledgeInstanceFailureDetection(&analysisEntry.AnalyzedInstanceKey)
		for _, replica := range lostReplicas {
			replica := replica
			base.BeginDowntime(dtstruct.NewDowntime(&replica.Key, dtstruct.GetMaintenanceOwner(), cconstant.DowntimeReasonLostInRecovery, time.Duration(cconstant.DowntimeSecond)*time.Second))
		}
		return nil
	}()

	return promotedReplica, lostReplicas, err
}

// replacePromotedReplicaWithCandidate is called after a master (or co-master)
// died and was replaced by some promotedReplica.
// But, is there an even better replica to promote?
// if candidateInstanceKey is given, then it is forced to be promoted over the promotedReplica
// Otherwise, search for the best to promote!
func ReplacePromotedReplicaWithCandidate(topologyRecovery *dtstruct.TopologyRecovery, deadInstanceKey *dtstruct.InstanceKey, promotedReplica dtstruct.InstanceAdaptor, candidateInstanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	candidateInstance, actionRequired, err := SuggestReplacementForPromotedReplica(topologyRecovery, deadInstanceKey, promotedReplica, candidateInstanceKey)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	if !actionRequired {
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("replace-promoted-replica-with-candidate: promoted instance %+v requires no further action", promotedReplica.GetInstance().Key))
		return promotedReplica, nil
	}

	// Try and promote suggested candidate, if applicable and possible
	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("replace-promoted-replica-with-candidate: promoted instance %+v is not the suggested candidate %+v. Will see what can be done", promotedReplica.GetInstance().Key, candidateInstance.GetInstance().Key))

	if candidateInstance.GetInstance().UpstreamKey.Equals(&promotedReplica.GetInstance().Key) {
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("replace-promoted-replica-with-candidate: suggested candidate %+v is replica of promoted instance %+v. Will try and take its master", candidateInstance.GetInstance().Key, promotedReplica.GetInstance().Key))
		candidateInstance, err = TakeMaster(&candidateInstance.GetInstance().Key, topologyRecovery.Type == dtstruct.CoMasterRecovery)
		if err != nil {
			return promotedReplica, log.Errore(err)
		}
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("success promoting %+v over %+v", candidateInstance.GetInstance().Key, promotedReplica.GetInstance().Key))

		// As followup to taking over, let's relocate all the rest of the replicas under the candidate instance
		relocateReplicasFunc := func() error {
			log.Debugf("replace-promoted-replica-with-candidate: relocating replicas of %+v below %+v", promotedReplica.GetInstance().Key, candidateInstance.GetInstance().Key)

			relocatedReplicas, _, err, _ := RelocateReplicas(&promotedReplica.GetInstance().Key, &candidateInstance.GetInstance().Key, "")
			log.Debugf("replace-promoted-replica-with-candidate: + relocated %+v replicas of %+v below %+v", len(relocatedReplicas), promotedReplica.GetInstance().Key, candidateInstance.GetInstance().Key)
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("relocated %+v replicas of %+v below %+v", len(relocatedReplicas), promotedReplica.GetInstance().Key, candidateInstance.GetInstance().Key))
			return log.Errore(err)
		}
		postponedFunctionsContainer := &topologyRecovery.PostponedFunctionsContainer
		if postponedFunctionsContainer != nil {
			postponedFunctionsContainer.AddPostponedFunction(relocateReplicasFunc, fmt.Sprintf("replace-promoted-replica-with-candidate: relocate replicas of %+v", promotedReplica.GetInstance().Key))
		} else {
			_ = relocateReplicasFunc()
			// We do not propagate the error. It is logged, but otherwise should not fail the entire failover operation
		}
		return candidateInstance, nil
	}

	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("could not manage to promoted suggested candidate %+v", candidateInstance.GetInstance().Key))
	return promotedReplica, nil
}

// ReadClusterCandidateInstances reads cluster instances which are also marked as candidates
func ReadClusterCandidateInstances(clusterName string) ([]dtstruct.InstanceAdaptor, error) {
	condition := `
			cluster_name = ?
			and concat(hostname, ':', port) in (
				select concat(hostname, ':', port)
					from ham_database_instance_candidate
					where promotion_rule in ('must', 'prefer')
			)
			`
	mih, err := ReadInstancesByCondition(mconstant.MysqlDefaultQuery, condition, sqlutil.Args(clusterName), "")
	return mutil.ToInstanceHander(mih), err
}

// SuggestReplacementForPromotedReplica returns a server to take over the already
// promoted replica, if such server is found and makes an improvement over the promoted replica.
func SuggestReplacementForPromotedReplica(topologyRecovery *dtstruct.TopologyRecovery, deadInstanceKey *dtstruct.InstanceKey, promotedReplica dtstruct.InstanceAdaptor, candidateInstanceKey *dtstruct.InstanceKey) (replacement dtstruct.InstanceAdaptor, actionRequired bool, err error) {
	candidateReplicas, _ := ReadClusterCandidateInstances(promotedReplica.GetInstance().ClusterName)
	candidateReplicas = dtstruct.RemoveInstance(candidateReplicas, deadInstanceKey)
	deadInstance, _, err := ReadFromBackendDB(deadInstanceKey)
	if err != nil {
		deadInstance = nil
	}
	// So we've already promoted a replica.
	// However, can we improve on our choice? Are there any replicas marked with "is_candidate"?
	// Maybe we actually promoted such a replica. Does that mean we should keep it?
	// Maybe we promoted a "neutral", and some "prefer" server is available.
	// Maybe we promoted a "prefer_not"
	// Maybe we promoted a server in a different DC than the master
	// There's many options. We may wish to replace the server we promoted with a better one.
	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("checking if should replace promoted replica with a better candidate"))
	if candidateInstanceKey == nil {
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ checking if promoted replica is the ideal candidate"))
		if deadInstance != nil {
			for _, candidateReplica := range candidateReplicas {
				if promotedReplica.GetInstance().Key.Equals(&candidateReplica.GetInstance().Key) &&
					promotedReplica.GetInstance().DataCenter == deadInstance.GetInstance().DataCenter &&
					promotedReplica.GetInstance().Environment == deadInstance.GetInstance().Environment {
					// Seems like we promoted a candidate in the same DC & ENV as dead IM! Ideal! We're happy!
					base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("promoted replica %+v is the ideal candidate", promotedReplica.GetInstance().Key))
					return promotedReplica, false, nil
				}
			}
		}
	}
	// We didn't pick the ideal candidate; let's see if we can replace with a candidate from same DC and ENV
	if candidateInstanceKey == nil {
		// Try a candidate replica that is in same DC & env as the dead instance
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for an ideal candidate"))
		if deadInstance != nil {
			for _, candidateReplica := range candidateReplicas {
				if canTakeOverPromotedServerAsMaster(candidateReplica, promotedReplica) &&
					candidateReplica.GetInstance().DataCenter == deadInstance.GetInstance().DataCenter &&
					candidateReplica.GetInstance().Environment == deadInstance.GetInstance().Environment {
					// This would make a great candidate
					candidateInstanceKey = &candidateReplica.GetInstance().Key
					base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but ham4db picks %+v as candidate replacement, based on being in same DC & env as failed instance", *deadInstanceKey, candidateReplica.GetInstance().Key))
				}
			}
		}
	}
	if candidateInstanceKey == nil {
		// We cannot find a candidate in same DC and ENV as dead master
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ checking if promoted replica is an OK candidate"))
		for _, candidateReplica := range candidateReplicas {
			if promotedReplica.GetInstance().Key.Equals(&candidateReplica.GetInstance().Key) {
				// Seems like we promoted a candidate replica (though not in same DC and ENV as dead master)
				if satisfied, reason := base.MasterFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, candidateReplica); satisfied {
					// Good enough. No further action required.
					base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("promoted replica %+v is a good candidate", promotedReplica.GetInstance().Key))
					return promotedReplica, false, nil
				} else {
					base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v; %s", candidateReplica.GetInstance().Key, reason))
				}
			}
		}
	}
	// Still nothing?
	if candidateInstanceKey == nil {
		// Try a candidate replica that is in same DC & env as the promoted replica (our promoted replica is not an "is_candidate")
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for a candidate"))
		for _, candidateReplica := range candidateReplicas {
			if canTakeOverPromotedServerAsMaster(candidateReplica, promotedReplica) &&
				promotedReplica.GetInstance().DataCenter == candidateReplica.GetInstance().DataCenter &&
				promotedReplica.GetInstance().Environment == candidateReplica.GetInstance().Environment {
				// OK, better than nothing
				candidateInstanceKey = &candidateReplica.GetInstance().Key
				base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but ham4db picks %+v as candidate replacement, based on being in same DC & env as promoted instance", promotedReplica.GetInstance().Key, candidateReplica.GetInstance().Key))
			}
		}
	}
	// Still nothing?
	if candidateInstanceKey == nil {
		// Try a candidate replica (our promoted replica is not an "is_candidate")
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for a candidate"))
		for _, candidateReplica := range candidateReplicas {
			if canTakeOverPromotedServerAsMaster(candidateReplica, promotedReplica) {
				if satisfied, reason := base.MasterFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, candidateReplica); satisfied {
					// OK, better than nothing
					candidateInstanceKey = &candidateReplica.GetInstance().Key
					base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but ham4db picks %+v as candidate replacement", promotedReplica.GetInstance().Key, candidateReplica.GetInstance().Key))
				} else {
					base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v; %s", candidateReplica.GetInstance().Key, reason))
				}
			}
		}
	}

	keepSearchingHint := ""
	if satisfied, reason := base.MasterFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, promotedReplica); !satisfied {
		keepSearchingHint = fmt.Sprintf("Will keep searching; %s", reason)
	} else if promotedReplica.GetInstance().PromotionRule == dtstruct.PreferNotPromoteRule {
		keepSearchingHint = fmt.Sprintf("Will keep searching because we have promoted a server with prefer_not rule: %+v", promotedReplica.GetInstance().Key)
	}
	if keepSearchingHint != "" {
		base.AuditTopologyRecovery(topologyRecovery, keepSearchingHint)
		neutralReplicas, _ := ReadClusterNeutralPromotionRuleInstances(promotedReplica.GetInstance().ClusterName)

		if candidateInstanceKey == nil {
			// Still nothing? Then we didn't find a replica marked as "candidate". OK, further down the stream we have:
			// find neutral instance in same dv&env as dead master
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for a neutral server to replace promoted server, in same DC and env as dead master"))
			for _, neutralReplica := range neutralReplicas {
				if canTakeOverPromotedServerAsMaster(neutralReplica, promotedReplica) &&
					deadInstance.GetInstance().DataCenter == neutralReplica.GetInstance().DataCenter &&
					deadInstance.GetInstance().Environment == neutralReplica.GetInstance().Environment {
					candidateInstanceKey = &neutralReplica.GetInstance().Key
					base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but ham4db picks %+v as candidate replacement, based on being in same DC & env as dead master", promotedReplica.GetInstance().Key, neutralReplica.GetInstance().Key))
				}
			}
		}
		if candidateInstanceKey == nil {
			// find neutral instance in same dv&env as promoted replica
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for a neutral server to replace promoted server, in same DC and env as promoted replica"))
			for _, neutralReplica := range neutralReplicas {
				if canTakeOverPromotedServerAsMaster(neutralReplica, promotedReplica) &&
					promotedReplica.GetInstance().DataCenter == neutralReplica.GetInstance().DataCenter &&
					promotedReplica.GetInstance().Environment == neutralReplica.GetInstance().Environment {
					candidateInstanceKey = &neutralReplica.GetInstance().Key
					base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but ham4db picks %+v as candidate replacement, based on being in same DC & env as promoted instance", promotedReplica.GetInstance().Key, neutralReplica.GetInstance().Key))
				}
			}
		}
		if candidateInstanceKey == nil {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ searching for a neutral server to replace a prefer_not"))
			for _, neutralReplica := range neutralReplicas {
				if canTakeOverPromotedServerAsMaster(neutralReplica, promotedReplica) {
					if satisfied, reason := base.MasterFailoverGeographicConstraintSatisfied(&topologyRecovery.AnalysisEntry, neutralReplica); satisfied {
						// OK, better than nothing
						candidateInstanceKey = &neutralReplica.GetInstance().Key
						base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("no candidate was offered for %+v but ham4db picks %+v as candidate replacement, based on promoted instance having prefer_not promotion rule", promotedReplica.GetInstance().Key, neutralReplica.GetInstance().Key))
					} else {
						base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("skipping %+v; %s", neutralReplica.GetInstance().Key, reason))
					}
				}
			}
		}
	}

	// So do we have a candidate?
	if candidateInstanceKey == nil {
		// Found nothing. Stick with promoted replica
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ found no server to promote on top promoted replica"))
		return promotedReplica, false, nil
	}
	if promotedReplica.GetInstance().Key.Equals(candidateInstanceKey) {
		// Sanity. It IS the candidate, nothing to promote...
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("+ sanity check: found our very own server to promote; doing nothing"))
		return promotedReplica, false, nil
	}
	replacement, _, err = ReadFromBackendDB(candidateInstanceKey)
	return replacement, true, err
}

func canTakeOverPromotedServerAsMaster(wantToTakeOver dtstruct.InstanceAdaptor, toBeTakenOver dtstruct.InstanceAdaptor) bool {
	if !isGenerallyValidAsWouldBeMaster(wantToTakeOver, true) {
		return false
	}
	if !wantToTakeOver.GetInstance().UpstreamKey.Equals(&toBeTakenOver.GetInstance().Key) {
		return false
	}
	if canReplicate, _ := CanReplicateFrom(toBeTakenOver, wantToTakeOver); !canReplicate {
		return false
	}
	return true
}

func isGenerallyValidAsWouldBeMaster(replica dtstruct.InstanceAdaptor, requireLogReplicationUpdates bool) bool {

	inst := replica.(*mdtstruct.MysqlInstance)

	if !inst.IsLastCheckValid {
		// something wrong with this replica right now. We shouldn't hope to be able to promote it
		return false
	}
	if !inst.LogBinEnabled {
		return false
	}
	if requireLogReplicationUpdates && !inst.LogReplicationUpdatesEnabled {
		return false
	}
	if inst.IsReplicaServer() {
		return false
	}
	if dtstruct.IsBannedFromBeingCandidateReplica(inst) {
		return false
	}

	return true
}

// checkAndRecoverGenericProblem is a general-purpose recovery function
func checkAndRecoverLockedSemiSyncMaster(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *dtstruct.TopologyRecovery, err error) {

	topologyRecovery, err = base.AttemptRecoveryRegistration(&analysisEntry, true, true)
	if topologyRecovery == nil {
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another RecoverLockedSemiSyncMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	return false, nil, nil
}

// checkAndRecoverDeadGroupMemberWithReplicas checks whether action needs to be taken for an analysis involving a dead
// replication group member, and takes the action if applicable. Notice that under our view of the world, a primary
// replication group member is akin to a master in traditional async/semisync replication; whereas secondary group
// members are akin to intermediate masters. Considering also that a failed group member can always be considered as a
// secondary (even if it was primary, the group should have detected its failure and elected a new primary), then
// failure of a group member with replicas is akin to failure of an intermediate master.
func checkAndRecoverDeadGroupMemberWithReplicas(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *dtstruct.TopologyRecovery, error) {
	// Don't proceed with recovery unless it was forced or automatic intermediate source recovery is enabled.
	// We consider failed group members akin to failed intermediate masters, so we re-use the configuration for
	// intermediates.
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedIntermediateMasterRecovery) {
		return false, nil, nil
	}
	// Try to record the recovery. It it fails to be recorded, it because it is already being dealt with.
	topologyRecovery, err := base.AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if err != nil {
		return false, nil, err
	}
	// Proceed with recovery
	recoverDeadReplicationGroupMemberCounter.Inc(1)

	recoveredToGroupMember, err := RecoverDeadReplicationGroupMemberWithReplicas(topologyRecovery, skipProcesses)

	if recoveredToGroupMember != nil {
		// success
		recoverDeadReplicationGroupMemberSuccessCounter.Inc(1)

		if !skipProcesses {
			// Execute post failover processes
			topologyRecovery.SuccessorKey = &recoveredToGroupMember.Key
			topologyRecovery.SuccessorAlias = recoveredToGroupMember.InstanceAlias
			// For the same reasons that were mentioned above, we re-use the post intermediate master fail-over hooks
			base.ExecuteProcesses(config.Config.PostIntermediateMasterFailoverProcesses, "PostIntermediateMasterFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadReplicationGroupMemberFailureCounter.Inc(1)
	}
	return true, topologyRecovery, err
}

// RecoverDeadReplicationGroupMemberWithReplicas performs dead group member recovery. It does so by finding members of
// the same replication group of the one of the failed instance, picking a random one and relocating replicas to it.
func RecoverDeadReplicationGroupMemberWithReplicas(topologyRecovery *dtstruct.TopologyRecovery, skipProcesses bool) (successorInstance *mdtstruct.MysqlInstance, err error) {
	topologyRecovery.Type = dtstruct.ReplicationGroupMemberRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedGroupMemberInstanceKey := &analysisEntry.AnalyzedInstanceKey
	base.AuditOperation("recover-dead-replication-group-member-with-replicas", failedGroupMemberInstanceKey, "", "problem found; will recover")
	if !skipProcesses {
		if err := base.ExecuteProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, topologyRecovery.AddError(err)
		}
	}
	failedGroupMember, _, err := ReadFromBackendDB(failedGroupMemberInstanceKey)
	if err != nil {
		return nil, topologyRecovery.AddError(err)
	}
	// Find a group member under which we can relocate the replicas of the failed one.
	//mysqlInstance := failedGroupMember.InstanceAdaptor.(*mdtstruct.MysqlInstance)
	groupMembers := failedGroupMember.ReplicationGroupMembers.GetInstanceKeys()
	if len(groupMembers) == 0 {
		return nil, topologyRecovery.AddError(errors.New("RecoverDeadReplicationGroupMemberWithReplicas: unable to find a candidate group member to relocate replicas to"))
	}
	// We have a group member to move replicas to, go ahead and do that
	base.AuditTopologyRecovery(topologyRecovery, "Finding a candidate group member to relocate replicas to")
	candidateGroupMemberInstanceKey := &groupMembers[rand.Intn(len(failedGroupMember.ReplicationGroupMembers.GetInstanceKeys()))]
	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Found group member %+v", candidateGroupMemberInstanceKey))
	relocatedReplicas, successorInstance, err, errs := RelocateReplicas(failedGroupMemberInstanceKey, candidateGroupMemberInstanceKey, "")
	topologyRecovery.AddErrors(errs)
	if len(relocatedReplicas) != len(failedGroupMember.DownstreamKeyMap.GetInstanceKeys()) {
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadReplicationGroupMemberWithReplicas: failed to move all replicas to candidate group member (%+v)", candidateGroupMemberInstanceKey))
		return nil, topologyRecovery.AddError(errors.New(fmt.Sprintf("RecoverDeadReplicationGroupMemberWithReplicas: Unable to relocate replicas to +%v", candidateGroupMemberInstanceKey)))
	}
	base.AuditTopologyRecovery(topologyRecovery, "All replicas successfully relocated")
	base.ResolveRecovery(topologyRecovery, successorInstance)
	return successorInstance, err
}

// checkAndRecoverDeadIntermediateMaster checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func checkAndRecoverDeadIntermediateMaster(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *dtstruct.TopologyRecovery, error) {
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedIntermediateMasterRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err := base.AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: found an active or recent recovery on %+v. Will not issue another RecoverDeadIntermediateMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	// That's it! We must do recovery!
	//recoverDeadIntermediateMasterCounter.Inc(1)
	promotedReplica, err := RecoverDeadIntermediateMaster(topologyRecovery, skipProcesses)
	if promotedReplica != nil {
		// success
		recoverDeadIntermediateMasterSuccessCounter.Inc(1)

		if !skipProcesses {
			// Execute post intermediate-master-failover processes
			topologyRecovery.SuccessorKey = &promotedReplica.Key
			topologyRecovery.SuccessorAlias = promotedReplica.InstanceAlias
			base.ExecuteProcesses(config.Config.PostIntermediateMasterFailoverProcesses, "PostIntermediateMasterFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadIntermediateMasterFailureCounter.Inc(1)
	}
	return true, topologyRecovery, err
}

func RunEmergentOperations(analysisEntry *dtstruct.ReplicationAnalysis) {
	switch analysisEntry.Analysis {
	case dtstruct.DeadMasterAndReplicas:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceUpstreamKey, analysisEntry.Analysis)
	case dtstruct.UnreachableMaster:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
		go emergentlyReadTopologyInstanceReplicas(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case dtstruct.UnreachableMasterWithLaggingReplicas:
		go emergentlyRestartReplicationOnTopologyInstanceReplicas(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case dtstruct.LockedSemiSyncMasterHypothesis:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
		go emergentlyRecordStaleBinlogCoordinates(&analysisEntry.AnalyzedInstanceKey, &analysisEntry.AnalyzedInstanceBinlogCoordinates)
	case dtstruct.UnreachableIntermediateMasterWithLaggingReplicas:
		go emergentlyRestartReplicationOnTopologyInstanceReplicas(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case dtstruct.AllMasterReplicasNotReplicating:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case dtstruct.AllMasterReplicasNotReplicatingOrDead:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceKey, analysisEntry.Analysis)
	case dtstruct.FirstTierReplicaFailingToConnectToMaster:
		go emergentlyReadTopologyInstance(&analysisEntry.AnalyzedInstanceUpstreamKey, analysisEntry.Analysis)
	}
}

// ReplicationConfirm
func ReplicationConfirm(failedKey *dtstruct.InstanceKey, streamKey *dtstruct.InstanceKey, upstream bool) bool {
	return false
}

// Force a re-read of a topology instance; this is done because we need to substantiate a suspicion
// that we may have a failover scenario. we want to speed up reading the complete picture.
func emergentlyReadTopologyInstance(instanceKey *dtstruct.InstanceKey, analysisCode dtstruct.AnalysisCode) (instance *mdtstruct.MysqlInstance, err error) {
	if existsInCacheError := emergencyReadTopologyInstanceMap.Add(instanceKey.StringCode(), true, cache.DefaultExpiration); existsInCacheError != nil {
		// Just recently attempted
		return nil, nil
	}
	instance, err = GetInfoFromInstance(instanceKey, false, false, nil, "")
	base.AuditOperation("emergently-read-topology-instance", instanceKey, "", string(analysisCode))
	return instance, err
}

// Force reading of replicas of given instance. This is because we suspect the instance is dead, and want to speed up
// detection of replication failure from its replicas.
func emergentlyReadTopologyInstanceReplicas(instanceKey *dtstruct.InstanceKey, analysisCode dtstruct.AnalysisCode) {
	replicas, err := ReadReplicaInstancesIncludingBinlogServerSubReplicas(instanceKey)
	if err != nil {
		return
	}
	for _, replica := range replicas {
		go emergentlyReadTopologyInstance(&replica.Key, analysisCode)
	}
}

// ReadReplicaInstancesIncludingBinlogServerSubReplicas returns a list of direct slves including any replicas
// of a binlog server replica
func ReadReplicaInstancesIncludingBinlogServerSubReplicas(masterKey *dtstruct.InstanceKey) ([]*mdtstruct.MysqlInstance, error) {
	replicas, err := ReadReplicaInstances(masterKey)
	if err != nil {
		return replicas, err
	}
	for _, replica := range replicas {
		replica := replica
		if replica.IsReplicaServer() {
			binlogServerReplicas, err := ReadReplicaInstancesIncludingBinlogServerSubReplicas(&replica.GetInstance().Key)
			if err != nil {
				return replicas, err
			}
			replicas = append(replicas, binlogServerReplicas...)
		}
	}
	return replicas, err
}

// emergentlyRestartReplicationOnTopologyInstanceReplicas forces a stop slave + start slave on
// replicas of a given instance, in an attempt to cause them to re-evaluate their replication state.
// This can be useful in scenarios where the master has Too Many Connections, but long-time connected
// replicas are not seeing this; when they stop+start replication, they need to re-authenticate and
// that's where we hope they realize the master is bad.
func emergentlyRestartReplicationOnTopologyInstanceReplicas(instanceKey *dtstruct.InstanceKey, analysisCode dtstruct.AnalysisCode) {
	if existsInCacheError := emergencyRestartReplicaTopologyInstanceMap.Add(instanceKey.StringCode(), true, cache.DefaultExpiration); existsInCacheError != nil {
		// While each replica's RestartReplication() is throttled on its own, it's also wasteful to
		// iterate all replicas all the time. This is the reason why we do grand-throttle check.
		return
	}
	beginEmergencyOperationGracefulPeriod(instanceKey)

	replicas, err := ReadReplicaInstancesIncludingBinlogServerSubReplicas(instanceKey)
	if err != nil {
		return
	}
	for _, replica := range replicas {
		replicaKey := &replica.Key
		go emergentlyRestartReplicationOnTopologyInstance(replicaKey, analysisCode)
	}
}

func beginEmergencyOperationGracefulPeriod(instanceKey *dtstruct.InstanceKey) {
	emergencyOperationGracefulPeriodMap.Set(instanceKey.StringCode(), true, cache.DefaultExpiration)
}

func emergentlyRecordStaleBinlogCoordinates(instanceKey *dtstruct.InstanceKey, binlogCoordinates *dtstruct.LogCoordinates) {
	err := base.RecordStaleInstanceBinlogCoordinates(instanceKey, binlogCoordinates)
	log.Errore(err)
}

// recoverDeadMaster recovers a dead master, complete logic inside
func RecoverDeadMaster(topologyRecovery *dtstruct.TopologyRecovery, candidateInstanceKey *dtstruct.InstanceKey, skipProcesses bool) (recoveryAttempted bool, promotedReplica *mdtstruct.MysqlInstance, lostReplicas []*mdtstruct.MysqlInstance, err error) {
	topologyRecovery.Type = dtstruct.MasterRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	var cannotReplicateReplicas [](*mdtstruct.MysqlInstance)
	postponedAll := false

	base.AuditOperation("recover-dead-master", failedInstanceKey, "", "problem found; will recover")
	if !skipProcesses {
		if err := base.ExecuteProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return false, nil, lostReplicas, topologyRecovery.AddError(err)
		}
	}

	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: will recover %+v", *failedInstanceKey))

	topologyRecovery.RecoveryType = GetMasterRecoveryType(analysisEntry)
	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: masterRecoveryType=%+v", topologyRecovery.RecoveryType))

	promotedReplicaIsIdeal := func(promoted dtstruct.InstanceAdaptor, hasBestPromotionRule bool) bool {
		if promoted == nil {
			return false
		}
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: promotedReplicaIsIdeal(%+v)", promoted.GetInstance().Key))
		if candidateInstanceKey != nil { //explicit request to promote a specific server
			return promoted.GetInstance().Key.Equals(candidateInstanceKey)
		}
		if promoted.GetInstance().DataCenter == topologyRecovery.AnalysisEntry.AnalyzedInstanceDataCenter {
			if promoted.GetInstance().PromotionRule == dtstruct.MustPromoteRule || promoted.GetInstance().PromotionRule == dtstruct.PreferPromoteRule ||
				(hasBestPromotionRule && promoted.GetInstance().PromotionRule != dtstruct.MustNotPromoteRule) {
				base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: found %+v to be ideal candidate; will optimize recovery", promoted.GetInstance().Key))
				postponedAll = true
				return true
			}
		}
		return false
	}

	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: %s", topologyRecovery.RecoveryType))

	lostReplicas, _, _, cannotReplicateReplicas, promotedReplica, err = CategorizeReplication(topologyRecovery, failedInstanceKey, promotedReplicaIsIdeal)

	topologyRecovery.AddError(err)
	lostReplicas = append(lostReplicas, cannotReplicateReplicas...)
	for _, replica := range lostReplicas {
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: - lost replica: %+v", replica.Key))
	}

	if promotedReplica != nil && len(lostReplicas) > 0 && config.Config.DetachLostReplicasAfterMasterFailover {
		postponedFunction := func() error {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: lost %+v replicas during recovery process; detaching them", len(lostReplicas)))
			for _, replica := range lostReplicas {
				replica := replica
				DetachMaster(&replica.Key)
			}
			return nil
		}
		topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadMaster, detach %+v lost replicas", len(lostReplicas)))
	}

	func() error {
		base.BeginDowntime(dtstruct.NewDowntime(failedInstanceKey, dtstruct.GetMaintenanceOwner(), cconstant.DowntimeReasonLostInRecovery, time.Duration(cconstant.DowntimeSecond)*time.Second))
		base.AcknowledgeInstanceFailureDetection(&analysisEntry.AnalyzedInstanceKey)
		for _, replica := range lostReplicas {
			replica := replica
			base.BeginDowntime(dtstruct.NewDowntime(&replica.Key, dtstruct.GetMaintenanceOwner(), cconstant.DowntimeReasonLostInRecovery, time.Duration(cconstant.DowntimeSecond)*time.Second))
		}
		return nil
	}()

	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))

	if promotedReplica != nil && !postponedAll {
		pr, err := ReplacePromotedReplicaWithCandidate(topologyRecovery, &analysisEntry.AnalyzedInstanceKey, promotedReplica, candidateInstanceKey)
		promotedReplica = pr.(*mdtstruct.MysqlInstance)
		topologyRecovery.AddError(err)
	}

	if promotedReplica == nil {
		message := "Failure: no replica promoted."
		base.AuditTopologyRecovery(topologyRecovery, message)
		base.AuditOperation("recover-dead-master", failedInstanceKey, "", message)
	} else {
		message := fmt.Sprintf("promoted replica: %+v", promotedReplica.Key)
		base.AuditTopologyRecovery(topologyRecovery, message)
		base.AuditOperation("recover-dead-master", failedInstanceKey, "", message)
	}
	return true, promotedReplica, lostReplicas, err
}

func CategorizeReplication(topologyRecovery *dtstruct.TopologyRecovery, failedInstanceKey *dtstruct.InstanceKey, promotedRepIsIdeal func(dtstruct.InstanceAdaptor, bool) bool) (aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas []*mdtstruct.MysqlInstance, promotedReplica *mdtstruct.MysqlInstance, err error) {
	switch topologyRecovery.RecoveryType {
	case mconstant.MasterRecoveryGTID:
		aheadReplicas, _, cannotReplicateReplicas, promotedReplica, err = RegroupReplicasGTID(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer, promotedRepIsIdeal)
	case mconstant.MasterRecoveryPseudoGTID:
		aheadReplicas, _, _, cannotReplicateReplicas, promotedReplica, err = RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers(failedInstanceKey, true, nil, &topologyRecovery.PostponedFunctionsContainer, promotedRepIsIdeal)
	case mconstant.MasterRecoveryBinlogServer:
		promotedReplica, err = RecoverDeadMasterInBinlogServerTopology(topologyRecovery)
	}
	return
}

// DetachMaster detaches a replica from its master by corrupting the Master_Host (in such way that is reversible)
func DetachMaster(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", *instanceKey)
	}
	if instance.UpstreamKey.IsDetached() {
		return instance, fmt.Errorf("instance already detached: %+v", *instanceKey)
	}
	detachedMasterKey := instance.UpstreamKey.DetachedKey()

	log.Infof("Will detach master host on %+v. Detached key is %+v", *instanceKey, *detachedMasterKey)

	if maintenanceToken, merr := base.BeginMaintenance(instanceKey, dtstruct.GetMaintenanceOwner(), "detach-replica-master-host"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer base.EndMaintenance(maintenanceToken)
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	instance, err = ChangeMasterTo(instanceKey, detachedMasterKey, &instance.ExecBinlogCoordinates, true, mconstant.GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(context.TODO(), instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	base.AuditOperation("repoint", instanceKey, instance.ClusterName, fmt.Sprintf("replica %+v detached from master into %+v", *instanceKey, *detachedMasterKey))

	return instance, err
}

// GetCandidateSiblingOfIntermediateMaster chooses the best sibling of a dead intermediate master
// to whom the IM's replicas can be moved.
func GetCandidateSiblingOfIntermediateMaster(topologyRecovery *dtstruct.TopologyRecovery, intermediateMasterInstance *mdtstruct.MysqlInstance) (*mdtstruct.MysqlInstance, error) {

	siblings, err := ReadReplicaInstances(&intermediateMasterInstance.UpstreamKey)
	if err != nil {
		return nil, err
	}
	if len(siblings) <= 1 {
		return nil, log.Errorf("topology_recovery: no siblings found for %+v", intermediateMasterInstance.Key)
	}

	sort.Sort(sort.Reverse(dtstruct.InstancesByCountReplicas(mutil.ToInstanceHander(siblings))))

	// In the next series of steps we attempt to return a good replacement.
	// None of the below attempts is sure to pick a winning server. Perhaps picked server is not enough up-todate -- but
	// this has small likelihood in the general case, and, well, it's an attempt. It's a Plan A, but we have Plan B & C if this fails.

	// At first, we try to return an "is_candidate" server in same dc & env
	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("searching for the best candidate sibling of dead intermediate master %+v", intermediateMasterInstance.Key))
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) &&
			sibling.IsCandidate &&
			sibling.DataCenter == intermediateMasterInstance.DataCenter &&
			sibling.Environment == intermediateMasterInstance.Environment {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as the ideal candidate", sibling.Key))
			return sibling, nil
		}
	}
	// No candidate in same DC & env, let's search for a candidate anywhere
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) && sibling.IsCandidate {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as a replacement for %+v [candidate sibling]", sibling.Key, intermediateMasterInstance.Key))
			return sibling, nil
		}
	}
	// Go for some valid in the same DC & ENV
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) &&
			sibling.DataCenter == intermediateMasterInstance.DataCenter &&
			sibling.Environment == intermediateMasterInstance.Environment {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as a replacement for %+v [same dc & environment]", sibling.Key, intermediateMasterInstance.Key))
			return sibling, nil
		}
	}
	// Just whatever is valid.
	for _, sibling := range siblings {
		sibling := sibling
		if isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance, sibling) {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found %+v as a replacement for %+v [any sibling]", sibling.Key, intermediateMasterInstance.Key))
			return sibling, nil
		}
	}
	return nil, log.Errorf("topology_recovery: cannot find candidate sibling of %+v", intermediateMasterInstance.Key)
}

// isGenerallyValidAsCandidateSiblingOfIntermediateMaster sees that basic server configuration and state are valid
func isGenerallyValidAsCandidateSiblingOfIntermediateMaster(sibling *mdtstruct.MysqlInstance) bool {
	if !sibling.LogBinEnabled {
		return false
	}
	if !sibling.LogReplicationUpdatesEnabled {
		return false
	}
	if !sibling.ReplicaRunning() {
		return false
	}
	if !sibling.IsLastCheckValid {
		return false
	}
	return true
}

// isValidAsCandidateSiblingOfIntermediateMaster checks to see that the given sibling is capable to take over instance's replicas
func isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance *mdtstruct.MysqlInstance, sibling *mdtstruct.MysqlInstance) bool {
	if sibling.Key.Equals(&intermediateMasterInstance.Key) {
		// same instance
		return false
	}
	if !isGenerallyValidAsCandidateSiblingOfIntermediateMaster(sibling) {
		return false
	}
	if dtstruct.IsBannedFromBeingCandidateReplica(sibling) {
		return false
	}
	if sibling.HasReplicationFilters != intermediateMasterInstance.HasReplicationFilters {
		return false
	}
	if sibling.IsReplicaServer() != intermediateMasterInstance.IsReplicaServer() {
		// When both are binlog servers, failover is trivial.
		// When failed IM is binlog server, its sibling is still valid, but we catually prefer to just repoint the replica up -- simplest!
		return false
	}
	if sibling.ExecBinlogCoordinates.SmallerThan(&intermediateMasterInstance.ExecBinlogCoordinates) {
		return false
	}
	return true
}

func getGracefulMasterTakeoverDesignatedInstance(clusterMasterKey *dtstruct.InstanceKey, designatedKey *dtstruct.InstanceKey, clusterMasterDirectReplicas []*mdtstruct.MysqlInstance, auto bool) (designatedInstance *mdtstruct.MysqlInstance, err error) {
	if designatedKey == nil {
		// User did not specify a replica to promote
		if len(clusterMasterDirectReplicas) == 1 {
			// Single replica. That's the one we'll promote
			return clusterMasterDirectReplicas[0], nil
		}
		// More than one replica.
		if !auto {
			return nil, fmt.Errorf("GracefulMasterTakeover: target instance not indicated, auto=false, and master %+v has %+v replicas. ham4db cannot choose where to failover to. Aborting", *clusterMasterKey, len(clusterMasterDirectReplicas))
		}
		log.Debugf("GracefulMasterTakeover: request takeover for master %+v, no designated replica indicated. ham4db will attempt to auto deduce replica.", *clusterMasterKey)
		designatedInstance, _, _, _, _, err = GetCandidateReplica(clusterMasterKey, false)
		if err != nil || designatedInstance == nil {
			return nil, fmt.Errorf("GracefulMasterTakeover: no target instance indicated, failed to auto-detect candidate replica for master %+v. Aborting", *clusterMasterKey)
		}
		log.Debugf("GracefulMasterTakeover: candidateReplica=%+v", designatedInstance.Key)
		if _, err := StartReplication(context.TODO(), &designatedInstance.Key); err != nil {
			return nil, fmt.Errorf("GracefulMasterTakeover:cannot start replication on designated replica %+v. Aborting", designatedKey)
		}
		log.Infof("GracefulMasterTakeover: designated master deduced to be %+v", designatedInstance.Key)
		return designatedInstance, nil
	}

	// Verify designated instance is a direct replica of master
	for _, directReplica := range clusterMasterDirectReplicas {
		if directReplica.Key.Equals(designatedKey) {
			designatedInstance = directReplica
		}
	}
	if designatedInstance == nil {
		return nil, fmt.Errorf("GracefulMasterTakeover: indicated designated instance %+v must be directly replicating from the master %+v", *designatedKey, *clusterMasterKey)
	}
	log.Infof("GracefulMasterTakeover: designated master instructed to be %+v", designatedInstance.Key)
	return designatedInstance, nil
}

// TODO
//===========================
//===========================
//===========================
//===========================
//===========================
//===========================
//===========================

func RecoverDeadMasterInBinlogServerTopology(topologyRecovery *dtstruct.TopologyRecovery) (promotedReplica *mdtstruct.MysqlInstance, err error) {
	failedMasterKey := &topologyRecovery.AnalysisEntry.AnalyzedInstanceKey

	var promotedBinlogServer *mdtstruct.MysqlInstance

	_, promotedBinlogServer, err = RegroupReplicasBinlogServers(failedMasterKey, true)
	if err != nil {
		return nil, log.Errore(err)
	}
	//proMIns := promotedBinlogServer.InstanceAdaptor.(*mdtstruct.MysqlInstance)
	promotedBinlogServer, err = StopReplication(&promotedBinlogServer.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Find candidate replica
	promotedReplica, err = GetCandidateReplicaOfBinlogServerTopology(&promotedBinlogServer.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Align it with binlog server coordinates
	promotedReplica, err = StopReplication(&promotedReplica.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = StartReplicationUntilMasterCoordinates(&promotedReplica.Key, &promotedBinlogServer.ExecBinlogCoordinates)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = StopReplication(&promotedReplica.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Detach, flush binary logs forward
	promotedReplica, err = ResetReplication(&promotedReplica.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = FlushBinaryLogsTo(&promotedReplica.Key, promotedBinlogServer.ExecBinlogCoordinates.LogFile)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = FlushBinaryLogs(&promotedReplica.Key, 1)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedReplica, err = PurgeBinaryLogsToLatest(&promotedReplica.Key, false)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	// Reconnect binlog servers to promoted replica (now master):
	promotedBinlogServer, err = SkipToNextBinaryLog(&promotedBinlogServer.Key)
	if err != nil {
		return promotedReplica, log.Errore(err)
	}
	promotedBinlogServer, err = Repoint(&promotedBinlogServer.Key, &promotedReplica.Key, mconstant.GTIDHintDeny)
	if err != nil {
		return nil, log.Errore(err)
	}

	func() {
		// Move binlog server replicas up to replicate from master.
		// This can only be done once a BLS has skipped to the next binlog
		// We postpone this operation. The master is already promoted and we're happy.
		binlogServerReplicas, err := ReadBinlogServerReplicaInstances(&promotedBinlogServer.Key)
		if err != nil {
			return
		}
		maxBinlogServersToPromote := 3
		for i, binlogServerReplica := range binlogServerReplicas {
			binlogServerReplica := binlogServerReplica
			if i >= maxBinlogServersToPromote {
				return
			}
			postponedFunction := func() error {
				binlogServerReplica, err := StopReplication(&binlogServerReplica.Key)
				if err != nil {
					return err
				}
				// Make sure the BLS has the "next binlog" -- the one the master flushed & purged to. Otherwise the BLS
				// will request a binlog the master does not have
				//binlogMIns := binlogServerReplica.InstanceAdaptor.(*mdtstruct.MysqlInstance)
				if binlogServerReplica.ExecBinlogCoordinates.SmallerThan(&binlogServerReplica.ExecBinlogCoordinates) {
					binlogServerReplica, err = StartReplicationUntilMasterCoordinates(&binlogServerReplica.Key, &binlogServerReplica.ExecBinlogCoordinates)
					if err != nil {
						return err
					}
				}
				_, err = Repoint(&binlogServerReplica.Key, &promotedReplica.Key, mconstant.GTIDHintDeny)
				return err
			}
			topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("recoverDeadMasterInBinlogServerTopology, moving binlog server %+v", binlogServerReplica.Key))
		}
	}()

	return promotedReplica, err
}

// RegroupReplicasBinlogServers works on a binlog-servers topology. It picks the most up-to-date BLS and repoints all other
// BLS below it
func RegroupReplicasBinlogServers(masterKey *dtstruct.InstanceKey, returnReplicaEvenOnFailureToRegroup bool) (repointedBinlogServers []*mdtstruct.MysqlInstance, promotedBinlogServer *mdtstruct.MysqlInstance, err error) {
	var binlogServerReplicas [](*mdtstruct.MysqlInstance)
	promotedBinlogServer, binlogServerReplicas, err = getMostUpToDateActiveBinlogServer(masterKey)

	resultOnError := func(err error) ([](*mdtstruct.MysqlInstance), *mdtstruct.MysqlInstance, error) {
		if !returnReplicaEvenOnFailureToRegroup {
			promotedBinlogServer = nil
		}
		return repointedBinlogServers, promotedBinlogServer, err
	}

	if err != nil {
		return resultOnError(err)
	}

	repointedBinlogServers, err, _ = RepointTo(binlogServerReplicas, &promotedBinlogServer.Key)

	if err != nil {
		return resultOnError(err)
	}
	base.AuditOperation("regroup-replicas-bls", masterKey, "", fmt.Sprintf("regrouped binlog server replicas of %+v; promoted %+v", *masterKey, promotedBinlogServer.Key))
	return repointedBinlogServers, promotedBinlogServer, nil
}

func getMostUpToDateActiveBinlogServer(masterKey *dtstruct.InstanceKey) (mostAdvancedBinlogServer *mdtstruct.MysqlInstance, binlogServerReplicas []*mdtstruct.MysqlInstance, err error) {
	if binlogServerReplicas, err = ReadBinlogServerReplicaInstances(masterKey); err == nil && len(binlogServerReplicas) > 0 {
		// Pick the most advanced binlog sever that is good to go
		for _, binlogServer := range binlogServerReplicas {
			if binlogServer.IsLastCheckValid {
				if mostAdvancedBinlogServer == nil {
					mostAdvancedBinlogServer = binlogServer
				}
				if mostAdvancedBinlogServer.ExecBinlogCoordinates.SmallerThan(&binlogServer.ExecBinlogCoordinates) {
					mostAdvancedBinlogServer = binlogServer
				}
			}
		}
	}
	return mostAdvancedBinlogServer, binlogServerReplicas, err
}

// GetCandidateReplicaOfBinlogServerTopology chooses the best replica to promote given a (possibly dead) master
func GetCandidateReplicaOfBinlogServerTopology(masterKey *dtstruct.InstanceKey) (candidateReplica *mdtstruct.MysqlInstance, err error) {
	replicas, err := getReplicasForSorting(masterKey, true)
	if err != nil {
		return candidateReplica, err
	}
	replicas = SortedReplicasDataCenterHint(replicas, cconstant.NoStopReplication, "")
	if len(replicas) == 0 {
		return candidateReplica, fmt.Errorf("No replicas found for %+v", *masterKey)
	}
	for _, replica := range replicas {
		replica := replica
		if candidateReplica != nil {
			break
		}
		if isValidAsCandidateMasterInBinlogServerTopology(replica) && !dtstruct.IsBannedFromBeingCandidateReplica(replica) {
			// this is the one
			candidateReplica = replica
		}
	}
	if candidateReplica != nil {
		log.Debugf("GetCandidateReplicaOfBinlogServerTopology: returning %+v as candidate replica for %+v", candidateReplica.Key, *masterKey)
	} else {
		log.Debugf("GetCandidateReplicaOfBinlogServerTopology: no candidate replica found for %+v", *masterKey)
	}
	return candidateReplica, err
}

// StartReplicationUntilMasterCoordinates issuesa START SLAVE UNTIL... statement on given instance
func StartReplicationUntilMasterCoordinates(instanceKey *dtstruct.InstanceKey, masterCoordinates *dtstruct.LogCoordinates) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", instanceKey)
	}
	if !instance.ReplicationThreadsStopped() {
		return instance, fmt.Errorf("replication threads are not stopped: %+v", instanceKey)
	}

	log.Infof("Will start replication on %+v until coordinates: %+v", instanceKey, masterCoordinates)

	if instance.SemiSyncEnforced {
		// Send ACK only from promotable instances.
		sendACK := instance.PromotionRule != dtstruct.MustNotPromoteRule
		// Always disable master setting, in case we're converting a former master.
		if err := EnableSemiSync(instanceKey, false, sendACK); err != nil {
			return instance, log.Errore(err)
		}
	}

	// MariaDB has a bug: a CHANGE MASTER TO statement does not work properly with prepared statement... :P
	// See https://mariadb.atlassian.net/browse/MDEV-7640
	// This is the reason for ExecInstance
	_, err = ExecSQLOnInstance(instanceKey, "start slave until master_log_file=?, master_log_pos=?",
		masterCoordinates.LogFile, masterCoordinates.LogPos)
	if err != nil {
		return instance, log.Errore(err)
	}

	instance, exactMatch, err := WaitForExecBinlogCoordinatesToReach(instanceKey, masterCoordinates, 0)
	if err != nil {
		return instance, log.Errore(err)
	}
	if !exactMatch {
		return instance, fmt.Errorf("Start SLAVE UNTIL is past coordinates: %+v", instanceKey)
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}

	return instance, err
}

// FlushBinaryLogsTo attempts to 'FLUSH BINARY LOGS' until given binary log is reached
func FlushBinaryLogsTo(instanceKey *dtstruct.InstanceKey, logFile string) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}

	distance := instance.SelfBinlogCoordinates.FileNumberDistance(&dtstruct.LogCoordinates{LogFile: logFile})
	if distance < 0 {
		return nil, log.Errorf("FlushBinaryLogsTo: target log file %+v is smaller than current log file %+v", logFile, instance.SelfBinlogCoordinates.LogFile)
	}
	return FlushBinaryLogs(instanceKey, distance)
}

// FlushBinaryLogs attempts a 'FLUSH BINARY LOGS' statement on the given instance.
func FlushBinaryLogs(instanceKey *dtstruct.InstanceKey, count int) (*mdtstruct.MysqlInstance, error) {
	if *dtstruct.RuntimeCLIFlags.Noop {
		return nil, fmt.Errorf("noop: aborting flush-binary-logs operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	for i := 0; i < count; i++ {
		_, err := ExecSQLOnInstance(instanceKey, `flush binary logs`)
		if err != nil {
			return nil, log.Errore(err)
		}
	}

	log.Infof("flush-binary-logs count=%+v on %+v", count, *instanceKey)
	base.AuditOperation("flush-binary-logs", instanceKey, "", "success")

	return GetInfoFromInstance(instanceKey, false, false, nil, "")
}

// PurgeBinaryLogsToLatest attempts to 'PURGE BINARY LOGS' until latest binary log
func PurgeBinaryLogsToLatest(instanceKey *dtstruct.InstanceKey, force bool) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}
	return PurgeBinaryLogsTo(instanceKey, instance.SelfBinlogCoordinates.LogFile, force)
}

// SkipToNextBinaryLog changes master position to beginning of next binlog
// USE WITH CARE!
// Use case is binlog servers where the master was gone & replaced by another.
func SkipToNextBinaryLog(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}

	nextFileCoordinates, err := instance.ExecBinlogCoordinates.NextFileCoordinates()
	if err != nil {
		return instance, log.Errore(err)
	}
	nextFileCoordinates.LogPos = 4
	log.Debugf("Will skip replication on %+v to next binary log: %+v", instance.Key, nextFileCoordinates.LogFile)

	instance, err = ChangeMasterTo(&instance.Key, &instance.UpstreamKey, &nextFileCoordinates, false, mconstant.GTIDHintNeutral)
	if err != nil {
		return instance, log.Errore(err)
	}
	base.AuditOperation("skip-binlog", instanceKey, instance.ClusterName, fmt.Sprintf("Skipped replication to next binary log: %+v", nextFileCoordinates.LogFile))
	return StartReplication(context.TODO(), instanceKey)
}

// ReadBinlogServerReplicaInstances reads direct replicas of a given master that are binlog servers
func ReadBinlogServerReplicaInstances(masterKey *dtstruct.InstanceKey) ([]*mdtstruct.MysqlInstance, error) {
	condition := `
			upstream_host = ?
			and upstream_port = ?
			and is_binlog_server = 1
		`
	return ReadInstancesByCondition(mconstant.MysqlDefaultQuery, condition, sqlutil.Args(masterKey.Hostname, masterKey.Port), "")
}

// RegroupReplicasGTID will choose a candidate replica of a given instance, and take its siblings using GTID
func RegroupReplicasGTID(
	masterKey *dtstruct.InstanceKey,
	returnReplicaEvenOnFailureToRegroup bool,
	onCandidateReplicaChosen func(handler dtstruct.InstanceAdaptor),
	postponedFunctionsContainer *dtstruct.PostponedFunctionsContainer,
	postponeAllMatchOperations func(dtstruct.InstanceAdaptor, bool) bool,
) (
	lostReplicas []*mdtstruct.MysqlInstance,
	movedReplicas []*mdtstruct.MysqlInstance,
	cannotReplicateReplicas []*mdtstruct.MysqlInstance,
	candidateReplica *mdtstruct.MysqlInstance,
	err error,
) {
	var emptyReplicas []*mdtstruct.MysqlInstance
	var unmovedReplicas []*mdtstruct.MysqlInstance
	candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := GetCandidateReplica(masterKey, true)
	if err != nil {
		if !returnReplicaEvenOnFailureToRegroup {
			candidateReplica = nil
		}
		return emptyReplicas, emptyReplicas, emptyReplicas, candidateReplica, err
	}

	if onCandidateReplicaChosen != nil {
		onCandidateReplicaChosen(candidateReplica)
	}
	replicasToMove := append(equalReplicas, laterReplicas...)
	hasBestPromotionRule := true
	if candidateReplica != nil {
		for _, replica := range replicasToMove {
			if replica.PromotionRule.BetterThan(candidateReplica.PromotionRule) {
				hasBestPromotionRule = false
			}
		}
	}
	moveGTIDFunc := func() error {
		log.Debugf("RegroupReplicasGTID: working on %d replicas", len(replicasToMove))

		movedReplicas, unmovedReplicas, err, _ = moveReplicasViaGTID(replicasToMove, candidateReplica, postponedFunctionsContainer)
		unmovedReplicas = append(unmovedReplicas, aheadReplicas...)
		return log.Errore(err)
	}
	if postponedFunctionsContainer != nil && postponeAllMatchOperations != nil && postponeAllMatchOperations(candidateReplica, hasBestPromotionRule) {
		postponedFunctionsContainer.AddPostponedFunction(moveGTIDFunc, fmt.Sprintf("regroup-replicas-gtid %+v", candidateReplica.Key))
	} else {
		err = moveGTIDFunc()
	}

	StartReplication(context.TODO(), &candidateReplica.Key)

	log.Debugf("RegroupReplicasGTID: done")
	base.AuditOperation("regroup-replicas-gtid", masterKey, "", fmt.Sprintf("regrouped replicas of %+v via GTID; promoted %+v", *masterKey, candidateReplica.Key))
	return unmovedReplicas, movedReplicas, cannotReplicateReplicas, candidateReplica, err
}

// RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers uses Pseugo-GTID to regroup replicas
// of given instance. The function also drill in to replicas of binlog servers that are replicating from given instance,
// and other recursive binlog servers, as long as they're in the same binlog-server-family.
func RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers(
	masterKey *dtstruct.InstanceKey,
	returnReplicaEvenOnFailureToRegroup bool,
	onCandidateReplicaChosen func(handler dtstruct.InstanceAdaptor),
	postponedFunctionsContainer *dtstruct.PostponedFunctionsContainer,
	postponeAllMatchOperations func(dtstruct.InstanceAdaptor, bool) bool,
) (
	aheadReplicas []*mdtstruct.MysqlInstance,
	equalReplicas []*mdtstruct.MysqlInstance,
	laterReplicas []*mdtstruct.MysqlInstance,
	cannotReplicateReplicas []*mdtstruct.MysqlInstance,
	candidateReplica *mdtstruct.MysqlInstance,
	err error,
) {
	// First, handle binlog server issues:
	func() error {
		log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: starting on replicas of %+v", *masterKey)
		// Find the most up to date binlog server:
		mostUpToDateBinlogServer, binlogServerReplicas, err := getMostUpToDateActiveBinlogServer(masterKey)
		if err != nil {
			return log.Errore(err)
		}
		if mostUpToDateBinlogServer == nil {
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: no binlog server replicates from %+v", *masterKey)
			// No binlog server; proceed as normal
			return nil
		}
		log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: most up to date binlog server of %+v: %+v", *masterKey, mostUpToDateBinlogServer.Key)

		// Find the most up to date candidate replica:
		candidateReplica, _, _, _, _, err := GetCandidateReplica(masterKey, true)
		if err != nil {
			return log.Errore(err)
		}
		if candidateReplica == nil {
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: no candidate replica for %+v", *masterKey)
			// Let the followup code handle that
			return nil
		}
		log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: candidate replica of %+v: %+v", *masterKey, candidateReplica.Key)

		if candidateReplica.ExecBinlogCoordinates.SmallerThan(&mostUpToDateBinlogServer.ExecBinlogCoordinates) {
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: candidate replica %+v coordinates smaller than binlog server %+v", candidateReplica.Key, mostUpToDateBinlogServer.Key)
			// Need to align under binlog server...
			candidateReplica, err = Repoint(&candidateReplica.Key, &mostUpToDateBinlogServer.Key, mconstant.GTIDHintDeny)
			if err != nil {
				return log.Errore(err)
			}
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: repointed candidate replica %+v under binlog server %+v", candidateReplica.Key, mostUpToDateBinlogServer.Key)
			candidateReplica, err = StartReplicationUntilMasterCoordinates(&candidateReplica.Key, &mostUpToDateBinlogServer.ExecBinlogCoordinates)
			if err != nil {
				return log.Errore(err)
			}
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: aligned candidate replica %+v under binlog server %+v", candidateReplica.Key, mostUpToDateBinlogServer.Key)
			// and move back
			candidateReplica, err = Repoint(&candidateReplica.Key, masterKey, mconstant.GTIDHintDeny)
			if err != nil {
				return log.Errore(err)
			}
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: repointed candidate replica %+v under master %+v", candidateReplica.Key, *masterKey)
			return nil
		}
		// Either because it _was_ like that, or we _made_ it so,
		// candidate replica is as/more up to date than all binlog servers
		for _, binlogServer := range binlogServerReplicas {
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: matching replicas of binlog server %+v below %+v", binlogServer.Key, candidateReplica.Key)
			// Right now sequentially.
			// At this point just do what you can, don't return an error
			MultiMatchReplicas(&binlogServer.Key, &candidateReplica.Key, "")
			log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: done matching replicas of binlog server %+v below %+v", binlogServer.Key, candidateReplica.Key)
		}
		log.Debugf("RegroupReplicasIncludingSubReplicasOfBinlogServers: done handling binlog regrouping for %+v; will proceed with normal RegroupReplicas", *masterKey)
		base.AuditOperation("regroup-replicas-including-bls", masterKey, "", fmt.Sprintf("matched replicas of binlog server replicas of %+v under %+v", *masterKey, candidateReplica.Key))
		return nil
	}()
	// Proceed to normal regroup:
	return RegroupReplicasPseudoGTID(masterKey, returnReplicaEvenOnFailureToRegroup, onCandidateReplicaChosen, postponedFunctionsContainer, postponeAllMatchOperations)
}

// MultiMatchReplicas will match (via pseudo-gtid) all replicas of given master below given instance.
func MultiMatchReplicas(masterKey *dtstruct.InstanceKey, belowKey *dtstruct.InstanceKey, pattern string) ([]*mdtstruct.MysqlInstance, *mdtstruct.MysqlInstance, error, []error) {
	res := []*mdtstruct.MysqlInstance{}
	errs := []error{}

	belowInstance, err := GetInfoFromInstance(belowKey, false, false, nil, "")
	if err != nil {
		// Can't access "below" ==> can't match replicas beneath it
		return res, nil, err, errs
	}

	masterInstance, found, err := ReadFromBackendDB(masterKey)
	if err != nil || !found {
		return res, nil, err, errs
	}

	// See if we have a binlog server case (special handling):
	binlogCase := false
	if masterInstance.IsReplicaServer() && masterInstance.GetInstance().UpstreamKey.Equals(belowKey) {
		// repoint-up
		log.Debugf("MultiMatchReplicas: pointing replicas up from binlog server")
		binlogCase = true
	} else if belowInstance.IsReplicaServer() && belowInstance.UpstreamKey.Equals(masterKey) {
		// repoint-down
		log.Debugf("MultiMatchReplicas: pointing replicas down to binlog server")
		binlogCase = true
	} else if masterInstance.IsReplicaServer() && belowInstance.IsReplicaServer() && masterInstance.GetInstance().UpstreamKey.Equals(&belowInstance.UpstreamKey) {
		// Both BLS, siblings
		log.Debugf("MultiMatchReplicas: pointing replicas to binlong sibling")
		binlogCase = true
	}
	if binlogCase {
		replicas, err, errors := RepointReplicasTo(masterKey, pattern, belowKey)
		// Bail out!
		return replicas, masterInstance, err, errors
	}

	// Not binlog server

	// replicas involved
	replicas, err := ReadReplicaInstancesIncludingBinlogServerSubReplicas(masterKey)
	if err != nil {
		return res, belowInstance, err, errs
	}
	replicas = mutil.FilterInstancesByPattern(replicas, pattern)
	matchedReplicas, belowInstance, err, errs := MultiMatchBelow(replicas, &belowInstance.Key, nil)

	if len(matchedReplicas) != len(replicas) {
		err = fmt.Errorf("MultiMatchReplicas: only matched %d out of %d replicas of %+v; error is: %+v", len(matchedReplicas), len(replicas), *masterKey, err)
	}
	base.AuditOperation("multi-match-replicas", masterKey, masterInstance.ClusterName, fmt.Sprintf("matched %d replicas under %+v", len(matchedReplicas), *belowKey))

	return matchedReplicas, belowInstance, err, errs
}

// Check if the instance is a MaxScale binlog server (a proxy not a real
// MySQL server) and also update the resolved hostname
func CheckMaxScale(instance *mdtstruct.MysqlInstance, sdb *sql.DB, latency *stopwatch.NamedStopwatch) (isMaxScale bool, resolvedHostname string, err error) {
	if config.Config.SkipMaxScaleCheck {
		return isMaxScale, resolvedHostname, err
	}

	latency.Start("instance")
	err = sqlutil.QueryRowsMap(sdb, "show variables like 'maxscale%'", func(m sqlutil.RowMap) error {
		if m.GetString("Variable_name") == "MAXSCALE_VERSION" {
			originalVersion := m.GetString("Value")
			if originalVersion == "" {
				originalVersion = m.GetString("value")
			}
			if originalVersion == "" {
				originalVersion = "0.0.0"
			}
			instance.Version = originalVersion + "-maxscale"
			instance.InstanceId = ""
			instance.Uptime = 0
			instance.Binlog_format = "INHERIT"
			instance.ReadOnly = true
			instance.LogBinEnabled = true
			instance.LogReplicationUpdatesEnabled = true
			resolvedHostname = instance.Key.Hostname
			latency.Start("backend")
			base.UpdateResolvedHostname(resolvedHostname, resolvedHostname)
			latency.Stop("backend")
			isMaxScale = true
		}
		return nil
	})
	latency.Stop("instance")

	// Detect failed connection attempts and don't report the command
	// we are executing as that might be confusing.
	if err != nil {
		if strings.Contains(err.Error(), mconstant.Error1045AccessDenied) {
			//accessDeniedCounter.Inc(1)
		}
		if mutil.UnrecoverableError(err) {
			cache2.LogReadTopologyInstanceError(&instance.Key, "", err)
		} else {
			cache2.LogReadTopologyInstanceError(&instance.Key, "show variables like 'maxscale%'", err)
		}
	}

	return isMaxScale, resolvedHostname, err
}

// PopulateGroupReplicationInformation obtains information about Group Replication  for this host as well as other hosts
// who are members of the same group (if any).
func PopulateGroupReplicationInformation(instance *mdtstruct.MysqlInstance, db *sql.DB) error {
	// We exclude below hosts with state OFFLINE because they have joined no group yet, so there is no point in getting
	// any group replication information from them
	q := `
	SELECT
		MEMBER_ID,
		MEMBER_HOST,
		MEMBER_PORT,
		MEMBER_STATE,
		MEMBER_ROLE,
		@@global.group_replication_group_name,
		@@global.group_replication_single_primary_mode
	FROM
		performance_schema.replication_group_members
	WHERE
		MEMBER_STATE != 'OFFLINE'
	`
	rows, err := db.Query(q)
	if err != nil {
		_, grNotSupported := GroupReplicationNotSupportedErrors[err.(*mysql.MySQLError).Number]
		if grNotSupported {
			return nil // If GR is not supported by the instance, just exit
		} else {
			// If we got here, the query failed but not because the server does not support group replication. Let's
			// log the error
			return log.Errorf("There was an error trying to check group replication information for instance "+
				"%+v: %+v", instance.Key, err)
		}
	}
	defer rows.Close()
	foundGroupPrimary := false
	// Loop over the query results and populate GR instance attributes from the row that matches the instance being
	// probed. In addition, figure out the group primary and also add it as attribute of the instance.
	for rows.Next() {
		var (
			uuid               string
			host               string
			port               uint16
			state              string
			role               string
			groupName          string
			singlePrimaryGroup bool
		)
		err := rows.Scan(&uuid, &host, &port, &state, &role, &groupName, &singlePrimaryGroup)
		if err == nil {
			// ToDo: add support for multi primary groups.
			if !singlePrimaryGroup {
				log.Debugf("This host seems to belong to a multi-primary replication group, which we don't " +
					"support")
				break
			}
			groupMemberKey, err := base.NewResolveInstanceKey(mconstant.DBTMysql, host, int(port))
			if err != nil {
				log.Errorf("Unable to resolve instance for group member %v:%v", host, port)
				continue
			}
			// Set the replication group primary from what we find in performance_schema.replication_group_members for
			// the instance being discovered.
			if role == mconstant.GroupReplicationMemberRolePrimary && groupMemberKey != nil {
				instance.ReplicationGroupPrimaryInstanceKey = *groupMemberKey
				foundGroupPrimary = true
			}
			if uuid == instance.InstanceId {
				instance.ReplicationGroupName = groupName
				instance.ReplicationGroupIsSinglePrimary = singlePrimaryGroup
				instance.ReplicationGroupMemberRole = role
				instance.ReplicationGroupMemberState = state
			} else {
				instance.AddGroupMemberKey(groupMemberKey) // This helps us keep info on all members of the same group as the instance
			}
		} else {
			log.Errorf("Unable to scan row  group replication information while processing %+v, skipping the "+
				"row and continuing: %+v", instance.Key, err)
		}
	}
	// If we did not manage to find the primary of the group in performance_schema.replication_group_members, we are
	// likely to have been expelled from the group. Still, try to find out the primary of the group and set it for the
	// instance being discovered, so that it is identified as part of the same cluster
	if !foundGroupPrimary {
		err = ReadReplicationGroupPrimary(instance)
		if err != nil {
			return log.Errorf("Unable to find the group primary of instance %+v even though it seems to be "+
				"part of a replication group", instance.Key)
		}
	}
	return nil
}

func ReadReplicationGroupPrimary(instance *mdtstruct.MysqlInstance) (err error) {
	query := `
	SELECT
		db_type,
		replication_group_primary_host,
		replication_group_primary_port
	FROM
		ham_database_instance
	WHERE
		replication_group_name = ?
		AND replication_group_member_role = 'PRIMARY'
`
	queryArgs := sqlutil.Args(instance.ReplicationGroupName)
	err = db.Query(query, queryArgs, func(row sqlutil.RowMap) error {
		dbt := row.GetString("db_type")
		groupPrimaryHost := row.GetString("replication_group_primary_host")
		groupPrimaryPort := row.GetInt("replication_group_primary_port")
		resolvedGroupPrimary, err := base.NewResolveInstanceKey(dtstruct.GetDatabaseType(dbt), groupPrimaryHost, groupPrimaryPort)
		if err != nil {
			return err
		}
		instance.ReplicationGroupPrimaryInstanceKey = *resolvedGroupPrimary
		return nil
	})
	return err
}

// RegisterInjectedPseudoGTID
func RegisterInjectedPseudoGTID(clusterName string) error {
	query := `
			insert into mysql_cluster_injected_pseudo_gtid (
					cluster_name,
					time_injected_timestamp
				) values (?, now())
				on duplicate key update
					cluster_name=values(cluster_name),
					time_injected_timestamp=now()
				`
	args := sqlutil.Args(clusterName)
	writeFunc := func() error {
		_, err := db.ExecSQL(query, args...)
		if err == nil {
			clusterInjectedPseudoGTIDCache.Set(clusterName, true, cache.DefaultExpiration)
		}
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}

// ExpireInjectedPseudoGTID
func ExpireInjectedPseudoGTID() error {
	writeFunc := func() error {
		_, err := db.ExecSQL(`
				delete from mysql_cluster_injected_pseudo_gtid
				where time_injected_timestamp < NOW() - INTERVAL ? MINUTE
				`, config.PseudoGTIDExpireMinutes,
		)
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}

// IsInjectedPseudoGTID reads from backend DB / cache
func IsInjectedPseudoGTID(clusterName string) (injected bool, err error) {
	if injectedValue, found := clusterInjectedPseudoGTIDCache.Get(clusterName); found {
		return injectedValue.(bool), err
	}
	query := `
			select
					count(*) as is_injected
				from
					mysql_cluster_injected_pseudo_gtid
				where
					cluster_name = ?
			`
	err = db.Query(query, sqlutil.Args(clusterName), func(m sqlutil.RowMap) error {
		injected = m.GetBool("is_injected")
		return nil
	})
	clusterInjectedPseudoGTIDCache.Set(clusterName, injected, cache.DefaultExpiration)
	return injected, log.Errore(err)
}

// See https://bugs.mysql.com/bug.php?id=83713
func workaroundBug83713(instanceKey *dtstruct.InstanceKey) {
	log.Debugf("workaroundBug83713: %+v", *instanceKey)
	queries := []string{
		`reset slave`,
		`start slave IO_THREAD`,
		`stop slave IO_THREAD`,
		`reset slave`,
	}
	for _, query := range queries {
		if _, err := ExecSQLOnInstance(instanceKey, query); err != nil {
			log.Debugf("workaroundBug83713: error on %s: %+v", query, err)
		}
	}
}

// EnableSemiSync sets the rpl_semi_sync_(master|replica)_enabled variables
// on a given instance.
func EnableSemiSync(instanceKey *dtstruct.InstanceKey, master, replica bool) error {
	log.Infof("instance %+v rpl_semi_sync_master_enabled: %t, rpl_semi_sync_slave_enabled: %t", instanceKey, master, replica)
	_, err := ExecSQLOnInstance(instanceKey,
		`set global rpl_semi_sync_master_enabled = ?, global rpl_semi_sync_slave_enabled = ?`,
		master, replica)
	return err
}

// ChangeMasterTo changes the given instance's master according to given input.
func ChangeMasterTo(instanceKey *dtstruct.InstanceKey, masterKey *dtstruct.InstanceKey, masterBinlogCoordinates *dtstruct.LogCoordinates, skipUnresolve bool, gtidHint mconstant.OperationGTIDHint) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.ReplicationThreadsExist() && !instance.ReplicationThreadsStopped() {
		return instance, fmt.Errorf("ChangeMasterTo: Cannot change master on: %+v because replication threads are not stopped", *instanceKey)
	}
	log.Debugf("ChangeMasterTo: will attempt changing master on %+v to %+v, %+v", *instanceKey, *masterKey, *masterBinlogCoordinates)
	changeToMasterKey := masterKey
	if !skipUnresolve {
		unresolvedMasterKey, nameUnresolved, err := base.UnresolveHostname(masterKey, func(instKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
			return GetInfoFromInstance(instanceKey, false, false, nil, "")
		})
		if err != nil {
			log.Debugf("ChangeMasterTo: aborting operation on %+v due to resolving error on %+v: %+v", *instanceKey, *masterKey, err)
			return instance, err
		}
		if nameUnresolved {
			log.Debugf("ChangeMasterTo: Unresolved %+v into %+v", *masterKey, unresolvedMasterKey)
		}
		changeToMasterKey = &unresolvedMasterKey
	}

	if *dtstruct.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting CHANGE MASTER TO operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	originalMasterKey := instance.UpstreamKey
	originalExecBinlogCoordinates := instance.ExecBinlogCoordinates

	var changeMasterFunc func() error
	changedViaGTID := false
	if instance.UsingMariaDBGTID && gtidHint != mconstant.GTIDHintDeny {
		// Keep on using GTID
		changeMasterFunc = func() error {
			_, err := ExecSQLOnInstance(instanceKey, "change master to master_host=?, master_port=?",
				changeToMasterKey.Hostname, changeToMasterKey.Port)
			return err
		}
		changedViaGTID = true
	} else if instance.UsingMariaDBGTID && gtidHint == mconstant.GTIDHintDeny {
		// Make sure to not use GTID
		changeMasterFunc = func() error {
			_, err = ExecSQLOnInstance(instanceKey, "change master to master_host=?, master_port=?, master_log_file=?, master_log_pos=?, master_use_gtid=no",
				changeToMasterKey.Hostname, changeToMasterKey.Port, masterBinlogCoordinates.LogFile, masterBinlogCoordinates.LogPos)
			return err
		}
	} else if instance.IsMariaDB() && gtidHint == mconstant.GTIDHintForce {
		// Is MariaDB; not using GTID, turn into GTID
		mariadbGTIDHint := "slave_pos"
		if !instance.ReplicationThreadsExist() {
			// This instance is currently a master. As per https://mariadb.com/kb/en/change-master-to/#master_use_gtid
			// we should be using current_pos.
			// See also:
			// - https://gitee.com/opengauss/orchestrator/issues/1146
			// - https://dba.stackexchange.com/a/234323
			mariadbGTIDHint = "current_pos"
		}
		changeMasterFunc = func() error {
			_, err = ExecSQLOnInstance(instanceKey, fmt.Sprintf("change master to master_host=?, master_port=?, master_use_gtid=%s", mariadbGTIDHint),
				changeToMasterKey.Hostname, changeToMasterKey.Port)
			return err
		}
		changedViaGTID = true
	} else if instance.UsingOracleGTID && gtidHint != mconstant.GTIDHintDeny {
		// Is Oracle; already uses GTID; keep using it.
		changeMasterFunc = func() error {
			_, err = ExecSQLOnInstance(instanceKey, "change master to master_host=?, master_port=?",
				changeToMasterKey.Hostname, changeToMasterKey.Port)
			return err
		}
		changedViaGTID = true
	} else if instance.UsingOracleGTID && gtidHint == mconstant.GTIDHintDeny {
		// Is Oracle; already uses GTID
		changeMasterFunc = func() error {
			_, err = ExecSQLOnInstance(instanceKey, "change master to master_host=?, master_port=?, master_log_file=?, master_log_pos=?, master_auto_position=0",
				changeToMasterKey.Hostname, changeToMasterKey.Port, masterBinlogCoordinates.LogFile, masterBinlogCoordinates.LogPos)
			return err
		}
	} else if instance.SupportsOracleGTID && gtidHint == mconstant.GTIDHintForce {
		// Is Oracle; not using GTID right now; turn into GTID
		changeMasterFunc = func() error {
			_, err = ExecSQLOnInstance(instanceKey, "change master to master_host=?, master_port=?, master_auto_position=1",
				changeToMasterKey.Hostname, changeToMasterKey.Port)
			return err
		}
		changedViaGTID = true
	} else {
		// Normal binlog file:pos
		changeMasterFunc = func() error {
			_, err = ExecSQLOnInstance(instanceKey, "change master to master_host=?, master_port=?, master_log_file=?, master_log_pos=?",
				changeToMasterKey.Hostname, changeToMasterKey.Port, masterBinlogCoordinates.LogFile, masterBinlogCoordinates.LogPos)
			return err
		}
	}
	err = changeMasterFunc()
	if err != nil && instance.UsingOracleGTID && strings.Contains(err.Error(), mconstant.Error1201CouldnotInitializeMasterInfoStructure) {
		log.Debugf("ChangeMasterTo: got %+v", err)
		workaroundBug83713(instanceKey)
		err = changeMasterFunc()
	}
	if err != nil {
		return instance, log.Errore(err)
	}
	base.WriteMasterPositionEquivalence(&originalMasterKey, &originalExecBinlogCoordinates, changeToMasterKey, masterBinlogCoordinates)
	ResetInstanceRelaylogCoordinatesHistory(instanceKey)

	log.Infof("ChangeMasterTo: Changed master on %+v to: %+v, %+v. GTID: %+v", *instanceKey, masterKey, masterBinlogCoordinates, changedViaGTID)

	instance, err = GetInfoFromInstance(instanceKey, false, false, nil, "")
	return instance, err
}

// GetCandidateReplica chooses the best replica to promote given a (possibly dead) master
func GetCandidateReplica(masterKey *dtstruct.InstanceKey, forRematchPurposes bool) (*mdtstruct.MysqlInstance, []*mdtstruct.MysqlInstance, []*mdtstruct.MysqlInstance, []*mdtstruct.MysqlInstance, []*mdtstruct.MysqlInstance, error) {
	var candidateReplica *mdtstruct.MysqlInstance
	aheadReplicas := []*mdtstruct.MysqlInstance{}
	equalReplicas := []*mdtstruct.MysqlInstance{}
	laterReplicas := []*mdtstruct.MysqlInstance{}
	cannotReplicateReplicas := []*mdtstruct.MysqlInstance{}

	dataCenterHint := ""
	if master, _, _ := ReadFromBackendDB(masterKey); master != nil {
		dataCenterHint = master.GetInstance().DataCenter
	}
	replicas, err := getReplicasForSorting(masterKey, false)
	if err != nil {
		return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err
	}
	stopReplicationMethod := cconstant.NoStopReplication
	if forRematchPurposes {
		stopReplicationMethod = cconstant.StopReplicationNice
	}
	replicas = SortedReplicasDataCenterHint(replicas, stopReplicationMethod, dataCenterHint)
	if err != nil {
		return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err
	}
	if len(replicas) == 0 {
		return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, fmt.Errorf("No replicas found for %+v", *masterKey)
	}
	candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err = chooseCandidateReplica(replicas)
	if err != nil {
		return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err
	}
	if candidateReplica != nil {
		mostUpToDateReplica := replicas[0]
		if candidateReplica.ExecBinlogCoordinates.SmallerThan(&mostUpToDateReplica.ExecBinlogCoordinates) {
			log.Warningf("GetCandidateReplica: chosen replica: %+v is behind most-up-to-date replica: %+v", candidateReplica.Key, mostUpToDateReplica.Key)
		}
	}
	log.Debugf("GetCandidateReplica: candidate: %+v, ahead: %d, equal: %d, late: %d, break: %d", candidateReplica.Key, len(aheadReplicas), len(equalReplicas), len(laterReplicas), len(cannotReplicateReplicas))
	return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, nil
}

// getReplicasForSorting returns a list of replicas of a given master potentially for candidate choosing
func getReplicasForSorting(masterKey *dtstruct.InstanceKey, includeBinlogServerSubReplicas bool) (replicas []*mdtstruct.MysqlInstance, err error) {
	if includeBinlogServerSubReplicas {
		replicas, err = ReadReplicaInstancesIncludingBinlogServerSubReplicas(masterKey)
	} else {
		replicas, err = ReadReplicaInstances(masterKey)
	}
	return replicas, err
}

// chooseCandidateReplica
func chooseCandidateReplica(replicas []*mdtstruct.MysqlInstance) (candidateReplica *mdtstruct.MysqlInstance, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas []*mdtstruct.MysqlInstance, err error) {
	if len(replicas) == 0 {
		return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, fmt.Errorf("No replicas found given in chooseCandidateReplica")
	}
	priorityMajorVersion, _ := GetPriorityMajorVersionForCandidate(replicas)
	priorityBinlogFormat, _ := GetPriorityBinlogFormatForCandidate(replicas)

	for _, replica := range replicas {
		replica := replica
		if isGenerallyValidAsCandidateReplica(replica) &&
			!dtstruct.IsBannedFromBeingCandidateReplica(replica) &&
			!dtstruct.IsSmallerMajorVersion(priorityMajorVersion, replica.MajorVersionString()) &&
			!IsSmallerBinlogFormat(priorityBinlogFormat, replica.Binlog_format) {
			// this is the one
			candidateReplica = replica
			break
		}
	}
	if candidateReplica == nil {
		// Unable to find a candidate that will master others.
		// Instead, pick a (single) replica which is not banned.
		for _, replica := range replicas {
			replica := replica
			if !dtstruct.IsBannedFromBeingCandidateReplica(replica) {
				// this is the one
				candidateReplica = replica
				break
			}
		}
		if candidateReplica != nil {
			replicas = mutil.RemoveInstance(replicas, &candidateReplica.Key)
		}
		return candidateReplica, replicas, equalReplicas, laterReplicas, cannotReplicateReplicas, fmt.Errorf("chooseCandidateReplica: no candidate replica found")
	}
	replicas = mutil.RemoveInstance(replicas, &candidateReplica.Key)
	for _, replica := range replicas {
		replica := replica
		if canReplicate, err := CanReplicateFrom(replica, candidateReplica); !canReplicate {
			// lost due to inability to replicate
			cannotReplicateReplicas = append(cannotReplicateReplicas, replica)
			if err != nil {
				log.Errorf("chooseCandidateReplica(): error checking CanReplicateFrom(). replica: %v; error: %v", replica.Key, err)
			}
		} else if replica.ExecBinlogCoordinates.SmallerThan(&candidateReplica.ExecBinlogCoordinates) {
			laterReplicas = append(laterReplicas, replica)
		} else if replica.ExecBinlogCoordinates.Equals(&candidateReplica.ExecBinlogCoordinates) {
			equalReplicas = append(equalReplicas, replica)
		} else {
			// lost due to being more advanced/ahead of chosen replica.
			aheadReplicas = append(aheadReplicas, replica)
		}
	}
	return candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err
}
func isGenerallyValidAsCandidateReplica(replica *mdtstruct.MysqlInstance) bool {
	if !isGenerallyValidAsBinlogSource(replica) {
		// does not have binary logs
		return false
	}
	if replica.IsReplicaServer() {
		// Can't regroup under a binlog server because it does not support pseudo-gtid related queries such as SHOW BINLOG EVENTS
		return false
	}

	return true
}
func isGenerallyValidAsBinlogSource(replica *mdtstruct.MysqlInstance) bool {
	if !replica.IsLastCheckValid {
		// something wrong with this replica right now. We shouldn't hope to be able to promote it
		return false
	}
	if !replica.LogBinEnabled {
		return false
	}
	if !replica.LogReplicationUpdatesEnabled {
		return false
	}

	return true
}

// IsSmallerBinlogFormat tests two binlog formats and sees if one is "smaller" than the other.
// "smaller" binlog format means you can replicate from the smaller to the larger.
func IsSmallerBinlogFormat(binlogFormat string, otherBinlogFormat string) bool {
	return mdtstruct.IsSmallerBinlogFormat(binlogFormat, otherBinlogFormat)
}

// SetReadOnly sets or clears the instance's global read_only variable
func SetReadOnly(instanceKey *dtstruct.InstanceKey, readOnly bool) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}

	if *dtstruct.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting set-read-only operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	// If async fallback is disallowed, we're responsible for flipping the master
	// semi-sync switch ON before accepting writes. The setting is off by default.
	if instance.SemiSyncEnforced && !readOnly {
		// Send ACK only from promotable instances.
		sendACK := instance.PromotionRule != dtstruct.MustNotPromoteRule
		if err := EnableSemiSync(instanceKey, true, sendACK); err != nil {
			return instance, log.Errore(err)
		}
	}

	if _, err := ExecSQLOnInstance(instanceKey, "set global read_only = ?", readOnly); err != nil {
		return instance, log.Errore(err)
	}
	if config.Config.UseSuperReadOnly {
		if _, err := ExecSQLOnInstance(instanceKey, "set global super_read_only = ?", readOnly); err != nil {
			// We don't bail out here. super_read_only is only available on
			// MySQL 5.7.8 and Percona Server 5.6.21-70
			// At this time ham4db does not verify whether a server supports super_read_only or not.
			// It makes a best effort to set it.
			log.Errore(err)
		}
	}
	instance, err = GetInfoFromInstance(instanceKey, false, false, nil, "")

	// If we just went read-only, it's safe to flip the master semi-sync switch
	// OFF, which is the default value so that replicas can make progress.
	if instance.SemiSyncEnforced && readOnly {
		// Send ACK only from promotable instances.
		sendACK := instance.PromotionRule != dtstruct.MustNotPromoteRule
		if err := EnableSemiSync(instanceKey, false, sendACK); err != nil {
			return instance, log.Errore(err)
		}
	}

	log.Infof("instance %+v read_only: %t", instanceKey, readOnly)
	base.AuditOperation("read-only", instanceKey, instance.ClusterName, fmt.Sprintf("set as %t", readOnly))

	return instance, err
}

// ErrantGTIDResetMaster will issue a safe RESET MASTER on a replica that replicates via GTID:
// It will make sure the gtid_purged set matches the executed set value as read just before the RESET.
// this will enable new replicas to be attached to given instance without complaints about missing/purged entries.
// This function requires that the instance does not have replicas.
func ErrantGTIDResetMaster(instanceKey *dtstruct.InstanceKey) (instance *mdtstruct.MysqlInstance, err error) {
	instance, err = GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	if instance.GtidErrant == "" {
		return instance, log.Errorf("gtid-errant-reset-master will not operate on %+v because no errant GTID is found", *instanceKey)
	}
	if !instance.SupportsOracleGTID {
		return instance, log.Errorf("gtid-errant-reset-master requested for %+v but it is not using oracle-gtid", *instanceKey)
	}
	if len(instance.DownstreamKeyMap) > 0 {
		return instance, log.Errorf("gtid-errant-reset-master will not operate on %+v because it has %+v replicas. Expecting no replicas", *instanceKey, len(instance.DownstreamKeyMap))
	}

	gtidSubtract := ""
	executedGtidSet := ""
	masterStatusFound := false
	replicationStopped := false
	waitInterval := time.Second * 5

	if maintenanceToken, merr := base.BeginMaintenance(instanceKey, dtstruct.GetMaintenanceOwner(), "reset-master-gtid"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer base.EndMaintenance(maintenanceToken)
	}

	if instance.IsReplica() {
		instance, err = StopReplication(instanceKey)
		if err != nil {
			goto Cleanup
		}
		replicationStopped, err = WaitForReplicationState(instanceKey, mdtstruct.ReplicationThreadStateStopped)
		if err != nil {
			goto Cleanup
		}
		if !replicationStopped {
			err = fmt.Errorf("gtid-errant-reset-master: timeout while waiting for replication to stop on %+v", instance.Key)
			goto Cleanup
		}
	}

	gtidSubtract, err = GTIDSubtract(instanceKey, instance.ExecutedGtidSet, instance.GtidErrant)
	if err != nil {
		goto Cleanup
	}

	// We're about to perform a destructive operation. It is non transactional and cannot be rolled back.
	// The replica will be left in a broken state.
	// This is why we allow multiple attempts at the following:
	for i := 0; i < countRetries; i++ {
		instance, err = ResetMaster(instanceKey)
		if err == nil {
			break
		}
		time.Sleep(waitInterval)
	}
	if err != nil {
		err = fmt.Errorf("gtid-errant-reset-master: error while resetting master on %+v, after which intended to set gtid_purged to: %s. Error was: %+v", instance.Key, gtidSubtract, err)
		goto Cleanup
	}

	masterStatusFound, executedGtidSet, err = ShowMasterStatus(instanceKey)
	if err != nil {
		err = fmt.Errorf("gtid-errant-reset-master: error getting master status on %+v, after which intended to set gtid_purged to: %s. Error was: %+v", instance.Key, gtidSubtract, err)
		goto Cleanup
	}
	if !masterStatusFound {
		err = fmt.Errorf("gtid-errant-reset-master: cannot get master status on %+v, after which intended to set gtid_purged to: %s.", instance.Key, gtidSubtract)
		goto Cleanup
	}
	if executedGtidSet != "" {
		err = fmt.Errorf("gtid-errant-reset-master: Unexpected non-empty Executed_Gtid_Set found on %+v following RESET MASTER, after which intended to set gtid_purged to: %s. Executed_Gtid_Set found to be: %+v", instance.Key, gtidSubtract, executedGtidSet)
		goto Cleanup
	}

	// We've just made the destructive operation. Again, allow for retries:
	for i := 0; i < countRetries; i++ {
		err = setGTIDPurged(instance, gtidSubtract)
		if err == nil {
			break
		}
		time.Sleep(waitInterval)
	}
	if err != nil {
		err = fmt.Errorf("gtid-errant-reset-master: error setting gtid_purged on %+v to: %s. Error was: %+v", instance.Key, gtidSubtract, err)
		goto Cleanup
	}

Cleanup:
	var startReplicationErr error
	instance, startReplicationErr = StartReplication(context.TODO(), instanceKey)
	log.Errore(startReplicationErr)

	if err != nil {
		return instance, log.Errore(err)
	}

	// and we're done (pending deferred functions)
	base.AuditOperation("gtid-errant-reset-master", instanceKey, instance.ClusterName, fmt.Sprintf("%+v master reset", *instanceKey))

	return instance, err
}

// skipQueryClassic skips a query in normal binlog file:pos replication
func setGTIDPurged(instance *mdtstruct.MysqlInstance, gtidPurged string) error {
	if *dtstruct.RuntimeCLIFlags.Noop {
		return fmt.Errorf("noop: aborting set-gtid-purged operation on %+v; signalling error but nothing went wrong.", instance.Key)
	}

	_, err := ExecSQLOnInstance(&instance.Key, `set global gtid_purged := ?`, gtidPurged)
	return err
}

// GracefulMasterTakeover will demote master of existing topology and promote its
// direct replica instead.
// It expects that replica to have no siblings.
// This function is graceful in that it will first lock down the master, then wait
// for the designated replica to catch up with last position.
// It will point old master at the newly promoted master at the correct coordinates, but will not start replication.
func GracefulMasterTakeover(clusterName string, designatedKey *dtstruct.InstanceKey, auto bool) (topologyRecovery *dtstruct.TopologyRecovery, promotedMasterCoordinates *dtstruct.LogCoordinates, err error) {
	clusterMasters, err := ReadClusterMaster(clusterName)
	if err != nil {
		return nil, nil, fmt.Errorf("Cannot deduce cluster master for %+v; error: %+v", clusterName, err)
	}
	if len(clusterMasters) != 1 {
		return nil, nil, fmt.Errorf("Cannot deduce cluster master for %+v. Found %+v potential masters", clusterName, len(clusterMasters))
	}
	clusterMaster := clusterMasters[0]

	clusterMasterDirectReplicas, err := ReadReplicaInstances(&clusterMaster.GetInstance().Key)
	if err != nil {
		return nil, nil, log.Errore(err)
	}

	if len(clusterMasterDirectReplicas) == 0 {
		return nil, nil, fmt.Errorf("Master %+v doesn't seem to have replicas", clusterMaster.GetInstance().Key)
	}

	if designatedKey != nil && !designatedKey.IsValid() {
		// An empty or invalid key is as good as no key
		designatedKey = nil
	}
	designatedInstance, err := getGracefulMasterTakeoverDesignatedInstance(&clusterMaster.GetInstance().Key, designatedKey, clusterMasterDirectReplicas, auto)
	if err != nil {
		return nil, nil, log.Errore(err)
	}

	if dtstruct.IsBannedFromBeingCandidateReplica(designatedInstance) {
		return nil, nil, fmt.Errorf("GracefulMasterTakeover: designated instance %+v cannot be promoted due to promotion rule or it is explicitly ignored in PromotionIgnoreHostnameFilters configuration", designatedInstance.Key)
	}

	masterOfDesignatedInstance, err := GetInfoFromInstance(&designatedInstance.UpstreamKey, false, false, nil, "")
	if err != nil {
		return nil, nil, err
	}
	if !masterOfDesignatedInstance.Key.Equals(&clusterMaster.GetInstance().Key) {
		return nil, nil, fmt.Errorf("Sanity check failure. It seems like the designated instance %+v does not replicate from the master %+v (designated instance's master key is %+v). This error is strange. Panicking", designatedInstance.Key, clusterMaster.Key, designatedInstance.UpstreamKey)
	}
	if !designatedInstance.HasReasonableMaintenanceReplicationLag() {
		return nil, nil, fmt.Errorf("Desginated instance %+v seems to be lagging to much for thie operation. Aborting.", designatedInstance.Key)
	}

	if len(clusterMasterDirectReplicas) > 1 {
		log.Infof("GracefulMasterTakeover: Will let %+v take over its siblings", designatedInstance.Key)
		relocatedReplicas, _, err, _ := RelocateReplicas(&clusterMaster.GetInstance().Key, &designatedInstance.Key, "")
		if len(relocatedReplicas) != len(clusterMasterDirectReplicas)-1 {
			// We are unable to make designated instance master of all its siblings
			relocatedReplicasKeyMap := dtstruct.NewInstanceKeyMap()
			mutil.AddInstances(relocatedReplicasKeyMap, relocatedReplicas)
			// Let's see which replicas have not been relocated
			for _, directReplica := range clusterMasterDirectReplicas {
				if relocatedReplicasKeyMap.HasKey(directReplica.Key) {
					// relocated, good
					continue
				}
				if directReplica.Key.Equals(&designatedInstance.Key) {
					// obviously we skip this one
					continue
				}
				if directReplica.IsDowntimed {
					// obviously we skip this one
					log.Warningf("GracefulMasterTakeover: unable to relocate %+v below designated %+v, but since it is downtimed (downtime reason: %s) I will proceed", directReplica.Key, designatedInstance.Key, directReplica.DowntimeReason)
					continue
				}
				return nil, nil, fmt.Errorf("Desginated instance %+v cannot take over all of its siblings. Error: %+v", designatedInstance.Key, err)
			}
		}
	}
	log.Infof("GracefulMasterTakeover: Will demote %+v and promote %+v instead", clusterMaster.GetInstance().Key, designatedInstance.Key)

	replicationCreds, replicationCredentialsError := ReadReplicationCredentials(&designatedInstance.Key)

	analysisEntry, err := ForceAnalysisEntry(clusterName, dtstruct.DeadMaster, dtstruct.GracefulMasterTakeoverCommandHint, &clusterMaster.Key)
	if err != nil {
		return nil, nil, err
	}
	preGracefulTakeoverTopologyRecovery := &dtstruct.TopologyRecovery{
		SuccessorKey:  &designatedInstance.Key,
		AnalysisEntry: analysisEntry,
	}
	if err := base.ExecuteProcesses(config.Config.PreGracefulTakeoverProcesses, "PreGracefulTakeoverProcesses", preGracefulTakeoverTopologyRecovery, true); err != nil {
		return nil, nil, fmt.Errorf("Failed running PreGracefulTakeoverProcesses: %+v", err)
	}

	log.Infof("GracefulMasterTakeover: Will set %+v as read_only", clusterMaster.GetInstance().Key)
	if clusterMaster, err = SetReadOnly(&clusterMaster.GetInstance().Key, true); err != nil {
		return nil, nil, err
	}
	demotedMasterSelfBinlogCoordinates := &clusterMaster.SelfBinlogCoordinates
	log.Infof("GracefulMasterTakeover: Will wait for %+v to reach master coordinates %+v", designatedInstance.Key, *demotedMasterSelfBinlogCoordinates)
	if designatedInstance, _, err = WaitForExecBinlogCoordinatesToReach(&designatedInstance.Key, demotedMasterSelfBinlogCoordinates, time.Duration(config.Config.ReasonableMaintenanceReplicationLagSeconds)*time.Second); err != nil {
		return nil, nil, err
	}
	promotedMasterCoordinates = &designatedInstance.SelfBinlogCoordinates

	log.Infof("GracefulMasterTakeover: attempting recovery")

	recoveryAttempted, topologyRecovery, err := base.ExecuteCheckAndRecoverFunction(analysisEntry, &designatedInstance.Key, true, false, GetCheckAndRecoverFunction, RunEmergentOperations, ReplicationConfirm)
	if err != nil {
		log.Errorf("GracefulMasterTakeover: noting an error, and for now proceeding: %+v", err)
	}
	if !recoveryAttempted {
		return nil, nil, fmt.Errorf("GracefulMasterTakeover: unexpected error: recovery not attempted. This should not happen")
	}
	if topologyRecovery == nil {
		return nil, nil, fmt.Errorf("GracefulMasterTakeover: recovery attempted but with no results. This should not happen")
	}
	if topologyRecovery.SuccessorKey == nil {
		// Promotion fails.
		// Undo setting read-only on original master.
		SetReadOnly(&clusterMaster.Key, false)
		return nil, nil, fmt.Errorf("GracefulMasterTakeover: Recovery attempted yet no replica promoted; err=%+v", err)
	}
	var gtidHint = mconstant.GTIDHintNeutral
	if topologyRecovery.RecoveryType == mconstant.MasterRecoveryGTID {
		gtidHint = mconstant.GTIDHintForce
	}
	clusterMaster, err = ChangeMasterTo(&clusterMaster.Key, &designatedInstance.Key, promotedMasterCoordinates, false, mconstant.OperationGTIDHint(gtidHint))
	if !clusterMaster.SelfBinlogCoordinates.Equals(demotedMasterSelfBinlogCoordinates) {
		log.Errorf("GracefulMasterTakeover: sanity problem. Demoted master's coordinates changed from %+v to %+v while supposed to have been frozen", *demotedMasterSelfBinlogCoordinates, clusterMaster.SelfBinlogCoordinates)
	}
	if !clusterMaster.HasReplicationCredentials && replicationCredentialsError == nil {
		_, credentialsErr := ChangeMasterCredentials(&clusterMaster.Key, replicationCreds)
		if err == nil {
			err = credentialsErr
		}
	}
	if designatedInstance.AllowTLS {
		_, enableSSLErr := EnableMasterSSL(&clusterMaster.Key)
		if err == nil {
			err = enableSSLErr
		}
	}
	if auto {
		_, startReplicationErr := StartReplication(context.TODO(), &clusterMaster.Key)
		if err == nil {
			err = startReplicationErr
		}
	}
	base.ExecuteProcesses(config.Config.PostGracefulTakeoverProcesses, "PostGracefulTakeoverProcesses", topologyRecovery, false)

	return topologyRecovery, promotedMasterCoordinates, err
}

func ForceAnalysisEntry(clusterName string, analysisCode dtstruct.AnalysisCode, commandHint string, failedInstanceKey *dtstruct.InstanceKey) (analysisEntry dtstruct.ReplicationAnalysis, err error) {
	clusterInfo, err := base.ReadClusterInfo(mconstant.DBTMysql, clusterName)
	if err != nil {
		return analysisEntry, err
	}

	clusterAnalysisEntries, err := GetReplicationAnalysis(clusterInfo.ClusterName, "", &dtstruct.ReplicationAnalysisHints{IncludeDowntimed: true, IncludeNoProblem: true})
	if err != nil {
		return analysisEntry, err
	}

	for _, entry := range clusterAnalysisEntries {
		if entry.AnalyzedInstanceKey.Equals(failedInstanceKey) {
			analysisEntry = entry
		}
	}
	analysisEntry.Analysis = analysisCode // we force this analysis
	analysisEntry.CommandHint = commandHint
	analysisEntry.ClusterDetails = *clusterInfo
	analysisEntry.AnalyzedInstanceKey = *failedInstanceKey

	return analysisEntry, nil
}

// RecoverDeadIntermediateMaster performs intermediate master recovery; complete logic inside
func RecoverDeadIntermediateMaster(topologyRecovery *dtstruct.TopologyRecovery, skipProcesses bool) (successorInstance *mdtstruct.MysqlInstance, err error) {
	topologyRecovery.Type = dtstruct.IntermediateMasterRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	recoveryResolved := false

	base.AuditOperation("recover-dead-intermediate-master", failedInstanceKey, "", "problem found; will recover")
	if !skipProcesses {
		if err := base.ExecuteProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, topologyRecovery.AddError(err)
		}
	}

	intermediateMasterInstance, _, err := ReadFromBackendDB(failedInstanceKey)
	if err != nil {
		return nil, topologyRecovery.AddError(err)
	}
	// Find possible candidate
	candidateSiblingOfIntermediateMaster, _ := GetCandidateSiblingOfIntermediateMaster(topologyRecovery, intermediateMasterInstance)
	relocateReplicasToCandidateSibling := func() {
		if candidateSiblingOfIntermediateMaster == nil {
			return
		}
		// We have a candidate
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: will attempt a candidate intermediate master: %+v", candidateSiblingOfIntermediateMaster.Key))
		relocatedReplicas, candidateSibling, err, errs := RelocateReplicas(failedInstanceKey, &candidateSiblingOfIntermediateMaster.Key, "")
		topologyRecovery.AddErrors(errs)
		topologyRecovery.ParticipatingInstanceKeys.AddKey(candidateSiblingOfIntermediateMaster.Key)

		if len(relocatedReplicas) == 0 {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: failed to move any replica to candidate intermediate master (%+v)", candidateSibling.Key))
			return
		}
		if err != nil || len(errs) > 0 {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: move to candidate intermediate master (%+v) did not complete: err: %+v, errs: %+v", candidateSibling.Key, err, errs))
			return
		}
		if err == nil {
			recoveryResolved = true
			successorInstance = candidateSibling

			base.AuditOperation("recover-dead-intermediate-master", failedInstanceKey, "", fmt.Sprintf("Relocated %d replicas under candidate sibling: %+v; %d errors: %+v", len(relocatedReplicas), candidateSibling.Key, len(errs), errs))
		}
	}
	// Plan A: find a replacement intermediate master in same Data Center
	if candidateSiblingOfIntermediateMaster != nil && candidateSiblingOfIntermediateMaster.DataCenter == intermediateMasterInstance.GetInstance().DataCenter {
		relocateReplicasToCandidateSibling()
	}
	if !recoveryResolved {
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: will next attempt regrouping of replicas"))
		// Plan B: regroup (we wish to reduce cross-DC replication streams)
		lostReplicas, _, _, _, regroupPromotedReplica, regroupError := RegroupReplicas(failedInstanceKey, true, nil, nil)
		if regroupError != nil {
			topologyRecovery.AddError(regroupError)
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: regroup failed on: %+v", regroupError))
		}
		if regroupPromotedReplica != nil {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: regrouped under %+v, with %d lost replicas", regroupPromotedReplica.Key, len(lostReplicas)))
			topologyRecovery.ParticipatingInstanceKeys.AddKey(regroupPromotedReplica.Key)
			if len(lostReplicas) == 0 && regroupError == nil {
				// Seems like the regroup worked flawlessly. The local replica took over all of its siblings.
				// We can consider this host to be the successor.
				successorInstance = regroupPromotedReplica
			}
		}
		// Plan C: try replacement intermediate master in other DC...
		if candidateSiblingOfIntermediateMaster != nil && candidateSiblingOfIntermediateMaster.DataCenter != intermediateMasterInstance.GetInstance().DataCenter {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: will next attempt relocating to another DC server"))
			relocateReplicasToCandidateSibling()
		}
	}
	if !recoveryResolved {
		// Do we still have leftovers? some replicas couldn't move? Couldn't regroup? Only left with regroup's resulting leader?
		// nothing moved?
		// We don't care much if regroup made it or not. We prefer that it made it, in which case we only need to relocate up
		// one replica, but the operation is still valid if regroup partially/completely failed. We just promote anything
		// not regrouped.
		// So, match up all that's left, plan D
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: will next attempt to relocate up from %+v", *failedInstanceKey))

		relocatedReplicas, masterInstance, err, errs := RelocateReplicas(failedInstanceKey, &analysisEntry.AnalyzedInstanceUpstreamKey, "")
		topologyRecovery.AddErrors(errs)
		topologyRecovery.ParticipatingInstanceKeys.AddKey(analysisEntry.AnalyzedInstanceUpstreamKey)

		if len(relocatedReplicas) > 0 {
			recoveryResolved = true
			if successorInstance == nil {
				// There could have been a local replica taking over its siblings. We'd like to consider that one as successor.
				successorInstance = masterInstance
			}
			base.AuditOperation("recover-dead-intermediate-master", failedInstanceKey, "", fmt.Sprintf("Relocated replicas under: %+v %d errors: %+v", successorInstance.Key, len(errs), errs))
		} else {
			err = log.Errorf("topology_recovery: RecoverDeadIntermediateMaster failed to match up any replica from %+v", *failedInstanceKey)
			topologyRecovery.AddError(err)
		}
	}
	if !recoveryResolved {
		successorInstance = nil
	}
	base.ResolveRecovery(topologyRecovery, successorInstance)
	return successorInstance, err
}

// checkAndRecoverDeadMaster checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func CheckAndRecoverDeadMaster(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *dtstruct.TopologyRecovery, err error) {
	if !(forceInstanceRecovery || analysisEntry.ClusterDetails.HasAutomatedMasterRecovery) {
		return false, nil, nil
	}
	topologyRecovery, err = base.AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("found an active or recent recovery on %+v. Will not issue another RecoverDeadMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	// That's it! We must do recovery!
	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("will handle DeadMaster event on %+v", analysisEntry.ClusterDetails.ClusterName))
	//recoverDeadMasterCounter.Inc(1)
	recoveryAttempted, promotedReplica, lostReplicas, err := RecoverDeadMaster(topologyRecovery, candidateInstanceKey, skipProcesses)
	if err != nil {
		base.AuditTopologyRecovery(topologyRecovery, err.Error())
	}
	mutil.AddInstances(&topologyRecovery.LostReplicas, lostReplicas)
	if !recoveryAttempted {
		return false, topologyRecovery, err
	}

	overrideMasterPromotion := func() (*mdtstruct.MysqlInstance, error) {
		if promotedReplica == nil {
			// No promotion; nothing to override.
			return promotedReplica, err
		}
		// Scenarios where we might cancel the promotion.
		if satisfied, reason := base.MasterFailoverGeographicConstraintSatisfied(&analysisEntry, promotedReplica); !satisfied {
			return nil, fmt.Errorf("RecoverDeadMaster: failed %+v promotion; %s", promotedReplica.Key, reason)
		}
		if config.Config.FailMasterPromotionOnLagMinutes > 0 &&
			time.Duration(promotedReplica.ReplicationLagSeconds.Int64)*time.Second >= time.Duration(config.Config.FailMasterPromotionOnLagMinutes)*time.Minute {
			// candidate replica lags too much
			return nil, fmt.Errorf("RecoverDeadMaster: failed promotion. FailMasterPromotionOnLagMinutes is set to %d (minutes) and promoted replica %+v 's lag is %d (seconds)", config.Config.FailMasterPromotionOnLagMinutes, promotedReplica.Key, promotedReplica.ReplicationLagSeconds.Int64)
		}
		if config.Config.FailMasterPromotionIfSQLThreadNotUpToDate && !promotedReplica.SQLThreadUpToDate() {
			return nil, fmt.Errorf("RecoverDeadMaster: failed promotion. FailMasterPromotionIfSQLThreadNotUpToDate is set and promoted replica %+v 's sql thread is not up to date (relay logs still unapplied). Aborting promotion", promotedReplica.Key)
		}
		if config.Config.DelayMasterPromotionIfSQLThreadNotUpToDate && !promotedReplica.SQLThreadUpToDate() {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("DelayMasterPromotionIfSQLThreadNotUpToDate: waiting for SQL thread on %+v", promotedReplica.Key))
			if _, err := WaitForSQLThreadUpToDate(&promotedReplica.Key, 0, 0); err != nil {
				return nil, fmt.Errorf("DelayMasterPromotionIfSQLThreadNotUpToDate error: %+v", err)
			}
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("DelayMasterPromotionIfSQLThreadNotUpToDate: SQL thread caught up on %+v", promotedReplica.Key))
		}
		// All seems well. No override done.
		return promotedReplica, err
	}
	if promotedReplica, err = overrideMasterPromotion(); err != nil {
		base.AuditTopologyRecovery(topologyRecovery, err.Error())
	}
	// And this is the end; whether successful or not, we're done.
	base.ResolveRecovery(topologyRecovery, promotedReplica)
	// Now, see whether we are successful or not. From this point there's no going back.
	if promotedReplica != nil {
		// Success!
		recoverDeadMasterSuccessCounter.Inc(1)
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("RecoverDeadMaster: successfully promoted %+v", promotedReplica.Key))
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: promoted server coordinates: %+v", promotedReplica.SelfBinlogCoordinates))

		if config.Config.ApplyMySQLPromotionAfterMasterFailover || analysisEntry.CommandHint == dtstruct.GracefulMasterTakeoverCommandHint {
			// on GracefulMasterTakeoverCommandHint it makes utter sense to RESET SLAVE ALL and read_only=0, and there is no sense in not doing so.
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: will apply MySQL changes to promoted master"))
			{
				_, err := ResetReplicationOperation(&promotedReplica.Key)
				if err != nil {
					// Ugly, but this is important. Let's give it another try
					_, err = ResetReplicationOperation(&promotedReplica.Key)
				}
				base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: applying RESET SLAVE ALL on promoted master: success=%t", (err == nil)))
				if err != nil {
					base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: NOTE that %+v is promoted even though SHOW SLAVE STATUS may still show it has a master", promotedReplica.Key))
				}
			}
			{
				_, err := SetReadOnly(&promotedReplica.Key, false)
				base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: applying read-only=0 on promoted master: success=%t", (err == nil)))
			}
			// Let's attempt, though we won't necessarily succeed, to set old master as read-only
			go func() {
				_, err := SetReadOnly(&analysisEntry.AnalyzedInstanceKey, true)
				base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: applying read-only=1 on demoted master: success=%t", (err == nil)))
			}()
		}

		kvPairs := base.GetClusterMasterKVPairs(analysisEntry.ClusterDetails.ClusterAlias, &promotedReplica.Key)
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Writing KV %+v", kvPairs))
		if orcraft.IsRaftEnabled() {
			for _, kvPair := range kvPairs {
				_, err := orcraft.PublishCommand("put-key-value", kvPair)
				log.Errore(err)
			}
			// since we'll be affecting 3rd party tools here, we _prefer_ to mitigate re-applying
			// of the put-key-value event upon startup. We _recommend_ a snapshot in the near future.
			go orcraft.PublishCommand("async-snapshot", "")
		} else {
			err := kv.PutKVPairs(kvPairs)
			log.Errore(err)
		}
		{
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Distributing KV %+v", kvPairs))
			err := kv.DistributePairs(kvPairs)
			log.Errore(err)
		}
		if config.Config.MasterFailoverDetachReplicaMasterHost {
			postponedFunction := func() error {
				base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: detaching master host on promoted master"))
				DetachMaster(&promotedReplica.Key)
				return nil
			}
			topologyRecovery.AddPostponedFunction(postponedFunction, fmt.Sprintf("RecoverDeadMaster, detaching promoted master host %+v", promotedReplica.Key))
		}
		func() error {
			before := analysisEntry.AnalyzedInstanceKey.StringCode()
			after := promotedReplica.Key.StringCode()
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: updating cluster_alias: %v -> %v", before, after))
			//~~~inst.ReplaceClusterName(before, after)
			if alias := analysisEntry.ClusterDetails.ClusterAlias; alias != "" {
				base.WriteClusterAlias(promotedReplica.Key.StringCode(), alias)
			} else {
				base.ReplaceAliasClusterName(before, after)
			}
			return nil
		}()

		base.SetGeneralAttribute(analysisEntry.ClusterDetails.ClusterDomain, promotedReplica.Key.StringCode())

		if !skipProcesses {
			// Execute post master-failover processes
			base.ExecuteProcesses(config.Config.PostMasterFailoverProcesses, "PostMasterFailoverProcesses", topologyRecovery, false)
		}
	} else {
		recoverDeadMasterFailureCounter.Inc(1)
	}

	return true, topologyRecovery, err
}

func GetMasterRecoveryType(analysisEntry *dtstruct.ReplicationAnalysis) (masterRecoveryType dtstruct.RecoveryType) {
	masterRecoveryType = mconstant.MasterRecoveryPseudoGTID
	if analysisEntry.OracleGTIDImmediateTopology || analysisEntry.MariaDBGTIDImmediateTopology {
		masterRecoveryType = mconstant.MasterRecoveryGTID
	} else if analysisEntry.BinlogServerImmediateTopology {
		masterRecoveryType = mconstant.MasterRecoveryBinlogServer
	}
	return masterRecoveryType
}

// emergentlyRestartReplicationOnTopologyInstance forces a RestartReplication on a given instance.
func emergentlyRestartReplicationOnTopologyInstance(instanceKey *dtstruct.InstanceKey, analysisCode dtstruct.AnalysisCode) {
	if existsInCacheError := emergencyRestartReplicaTopologyInstanceMap.Add(instanceKey.StringCode(), true, cache.DefaultExpiration); existsInCacheError != nil {
		// Just recently attempted on this specific replica
		return
	}

	go limiter.ExecuteOnTopology(func() {
		instance, _, err := ReadFromBackendDB(instanceKey)
		if err != nil {
			return
		}
		if instance.UsingMariaDBGTID {
			// In MariaDB GTID, stopping and starting IO thread actually deletes relay logs.
			// This is counter productive to our objective.
			// Specifically, in a situation where the primary is unreachable and where replicas are lagging,
			// we want to restart IO thread to see if lag is actually caused by locked primary. If this results
			// with losing relay logs, then we've lost data.
			// So, unfortunately we avoid this step in MariaDB GTID.
			// See https://gitee.com/opengauss/orchestrator/issues/1260
			return
		}

		RestartReplicationQuick(instanceKey)
		base.AuditOperation("emergently-restart-replication-topology-instance", instanceKey, "", string(analysisCode))
	})
}

// WaitForReplicationState waits for both replication threads to be either running or not running, together.
// This is useful post- `start slave` operation, ensuring both threads are actually running,
// or post `stop slave` operation, ensuring both threads are not running.
func WaitForReplicationState(instanceKey *dtstruct.InstanceKey, expectedState mdtstruct.ReplicationThreadState) (expectationMet bool, err error) {
	waitDuration := time.Second
	waitInterval := 10 * time.Millisecond
	startTime := time.Now()

	for {
		// Since this is an incremental aggressive polling, it's OK if an occasional
		// error is observed. We don't bail out on a single error.
		if expectationMet, _ := expectReplicationThreadsState(instanceKey, expectedState); expectationMet {
			return true, nil
		}
		if time.Since(startTime)+waitInterval > waitDuration {
			break
		}
		time.Sleep(waitInterval)
		waitInterval = 2 * waitInterval
	}
	return false, nil
}

// expectReplicationThreadsState expects both replication threads to be running, or both to be not running.
// Specifically, it looks for both to be "Yes" or for both to be "No".
func expectReplicationThreadsState(instanceKey *dtstruct.InstanceKey, expectedState mdtstruct.ReplicationThreadState) (expectationMet bool, err error) {
	db, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return false, err
	}
	err = sqlutil.QueryRowsMap(db, "show slave status", func(m sqlutil.RowMap) error {
		ioThreadState := mdtstruct.ReplicationThreadStateFromStatus(m.GetString("Slave_IO_Running"))
		sqlThreadState := mdtstruct.ReplicationThreadStateFromStatus(m.GetString("Slave_SQL_Running"))

		if ioThreadState == expectedState && sqlThreadState == expectedState {
			expectationMet = true
		}
		return nil
	})
	return expectationMet, err
}

// GetPriorityMajorVersionForCandidate returns the primary (most common) major version found
// among given instances. This will be used for choosing best candidate for promotion.
func GetPriorityMajorVersionForCandidate(replicas []*mdtstruct.MysqlInstance) (priorityMajorVersion string, err error) {
	if len(replicas) == 0 {
		return "", log.Errorf("empty replicas list in getPriorityMajorVersionForCandidate")
	}
	majorVersionsCount := make(map[string]int)
	for _, replica := range replicas {
		majorVersionsCount[replica.MajorVersionString()] = majorVersionsCount[replica.MajorVersionString()] + 1
	}
	if len(majorVersionsCount) == 1 {
		// all same version, simple case
		return replicas[0].MajorVersionString(), nil
	}
	sorted := dtstruct.NewMajorVersionsSortedByCount(majorVersionsCount)
	sort.Sort(sort.Reverse(sorted))
	return sorted.First(), nil
}

// sortedReplicas returns the list of replicas of some master, sorted by exec coordinates
// (most up-to-date replica first).
// This function assumes given `replicas` argument is indeed a list of instances all replicating
// from the same master (the result of `getReplicasForSorting()` is appropriate)
func SortedReplicasDataCenterHint(replicas []*mdtstruct.MysqlInstance, stopReplicationMethod cconstant.StopReplicationMethod, dataCenterHint string) []*mdtstruct.MysqlInstance {
	if len(replicas) <= 1 {
		return replicas
	}
	replicas = StopReplicas(replicas, stopReplicationMethod, time.Duration(config.Config.InstanceBulkOperationsWaitTimeoutSeconds)*time.Second)
	replicas = mutil.RemoveNilInstances(replicas)

	SortInstancesDataCenterHint(replicas, dataCenterHint)
	for _, replica := range replicas {
		log.Debugf("- sorted replica: %+v", replica.GetInstance().Key)
	}

	return replicas
}

// StopReplicas will stop replication concurrently on given set of replicas.
// It will potentially do nothing, or attempt to stop _nicely_ or just stop normally, all according to stopReplicationMethod
func StopReplicas(replicas []*mdtstruct.MysqlInstance, stopReplicationMethod cconstant.StopReplicationMethod, timeout time.Duration) []*mdtstruct.MysqlInstance {
	if stopReplicationMethod == cconstant.NoStopReplication {
		return replicas
	}
	refreshedReplicas := []*mdtstruct.MysqlInstance{}

	log.Debugf("Stopping %d replicas via %s", len(replicas), string(stopReplicationMethod))
	// use concurrency but wait for all to complete
	barrier := make(chan *mdtstruct.MysqlInstance)
	for _, replica := range replicas {
		replica := replica
		go func() {
			updatedReplica := &replica
			// Signal completed replica
			defer func() { barrier <- *updatedReplica }()
			// Wait your turn to read a replica
			limiter.ExecuteOnTopology(func() {
				if stopReplicationMethod == cconstant.StopReplicationNice {
					StopReplicationNicely(&replica.GetInstance().Key, timeout)
				}
				replica, _ = StopReplication(&replica.GetInstance().Key)
				updatedReplica = &replica
			})
		}()
	}
	for range replicas {
		refreshedReplicas = append(refreshedReplicas, <-barrier)
	}
	return refreshedReplicas
}

// sortInstances shuffles given list of instances according to some logic
func SortInstancesDataCenterHint(instances []*mdtstruct.MysqlInstance, dataCenterHint string) {
	sort.Sort(sort.Reverse(mdtstruct.NewInstancesSorterByExec(instances, dataCenterHint)))
}
