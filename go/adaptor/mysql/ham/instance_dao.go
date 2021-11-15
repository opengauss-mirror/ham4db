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
	"fmt"
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/common"
	mconstant "gitee.com/opengauss/ham4db/go/adaptor/mysql/common/constant"
	mdtstruct "gitee.com/opengauss/ham4db/go/adaptor/mysql/dtstruct"
	mutil "gitee.com/opengauss/ham4db/go/adaptor/mysql/util"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/cache"
	"gitee.com/opengauss/ham4db/go/core/limiter"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/util/math"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"github.com/rcrowley/go-metrics"
	"sort"

	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util"
	"github.com/sjmudd/stopwatch"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var instanceReadChan = make(chan bool, constant.ConcurrencyBackendDBRead)

//var accessDeniedCounter = metrics.NewCounter()
var ReadTopologyInstanceCounter = metrics.NewCounter()

func init() {
	//metric.Register("instance.access_denied", accessDeniedCounter)
	metrics.Register("instance.mysql.read_topology", ReadTopologyInstanceCounter)
}

// ForgetInstance delete instance from table
func ForgetInstance(instanceKey *dtstruct.InstanceKey) error {

	// check if instance key is nil
	if instanceKey == nil {
		return log.Errorf("cannot forget nil instance")
	}

	// delete from table mysql_database_instance
	sqlResult, err := db.ExecSQL(`delete from mysql_database_instance where di_hostname = ? and di_port = ?`, instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return err
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return log.Errorf("instance %+v not found", *instanceKey)
	}

	// get cluster name of instance and write to audit
	base.AuditOperation("forget", instanceKey, "", "")
	return nil
}

//TODO-----

// ReadClusterNeutralPromotionRuleInstances reads cluster instances whose promotion-rule is marked as 'neutral'
func ReadClusterNeutralPromotionRuleInstances(clusterName string) (neutralInstances []*mdtstruct.MysqlInstance, err error) {
	instances, err := ReadClusterInstances(clusterName)
	if err != nil {
		return neutralInstances, err
	}
	for _, instance := range instances {
		if instance.GetInstance().PromotionRule == dtstruct.NeutralPromoteRule {
			neutralInstances = append(neutralInstances, instance)
		}
	}
	return neutralInstances, nil
}

func ValidateInstanceIsFound(instanceKey *dtstruct.InstanceKey) *mdtstruct.MysqlInstance {
	instance, _, err := ReadFromBackendDB(instanceKey)
	if err != nil {
		log.Fatale(err)
	}
	if instance == nil {
		log.Fatalf("Instance not found: %+v", *instanceKey)
	}
	return instance
}

// GetClusterHeuristicLag returns a heuristic lag for a cluster, based on its OSC replicas
func GetClusterHeuristicLag(clusterName string) (int64, error) {
	instances, err := GetClusterOSCReplicas(clusterName)
	if err != nil {
		return 0, err
	}
	return GetInstancesMaxLag(instances)
}

//// GetHeuristicClusterPoolInstancesLag returns a heuristic lag for the instances participating
//// in a cluster pool (or all the cluster's pools)
//func GetHeuristicClusterPoolInstancesLag(clusterName string, pool string) (int64, error) {
//	instances, err := GetHeuristicClusterPoolInstances(clusterName, pool)
//	if err != nil {
//		return 0, err
//	}
//	return GetInstancesMaxLag(instances)
//}

// ReadTopologyInstanceBufferable connects to a topology MySQL instance
// and collects information on the server and its replication state.
// It writes the information retrieved into ham4db's backend.
// - writes are optionally buffered.
// - timing information can be collected for the stages performed.

// GetInstancesMaxLag returns the maximum lag in a set of instances
func GetInstancesMaxLag(instances []*mdtstruct.MysqlInstance) (maxLag int64, err error) {
	if len(instances) == 0 {
		return 0, log.Errorf("No instances found in GetInstancesMaxLag")
	}
	for _, clusterInstance := range instances {
		if clusterInstance.GetInstance().ReplicationLagSeconds.Valid && clusterInstance.GetInstance().ReplicationLagSeconds.Int64 > maxLag {
			maxLag = clusterInstance.GetInstance().ReplicationLagSeconds.Int64
		}
	}
	return maxLag, nil
}

// GetClusterOSCReplicas returns a heuristic list of replicas which are fit as controll replicas for an OSC operation.
// These would be intermediate masters
func GetClusterOSCReplicas(clusterName string) ([]*mdtstruct.MysqlInstance, error) {
	var intermediateMasters []*mdtstruct.MysqlInstance
	var result []*mdtstruct.MysqlInstance
	var err error
	if strings.Index(clusterName, "'") >= 0 {
		return []*mdtstruct.MysqlInstance{}, log.Errorf("Invalid cluster name: %s", clusterName)
	}
	{
		// Pick up to two busiest IMs
		condition := `
			replication_depth = 1
			and downstream_count > 0
			and cluster_name = ?
		`
		intermediateMasters, err = ReadInstancesByCondition(mconstant.MysqlDefaultQuery, condition, sqlutil.Args(clusterName), "")
		if err != nil {
			return result, err
		}
		sort.Sort(sort.Reverse(mdtstruct.InstancesByCountReplicas(intermediateMasters)))
		intermediateMasters = filterOSCInstances(intermediateMasters)
		intermediateMasters = intermediateMasters[0:math.MinInt(2, len(intermediateMasters))]
		result = append(result, intermediateMasters...)
	}
	{
		// Get 2 replicas of found IMs, if possible
		if len(intermediateMasters) == 1 {
			// Pick 2 replicas for this IM
			replicas, err := ReadReplicaInstances(&(intermediateMasters[0].GetInstance().Key))
			if err != nil {
				return result, err
			}
			sort.Sort(sort.Reverse(mdtstruct.InstancesByCountReplicas(replicas)))
			replicas = filterOSCInstances(replicas)
			replicas = replicas[0:math.MinInt(2, len(replicas))]
			result = append(result, replicas...)

		}
		if len(intermediateMasters) == 2 {
			// Pick one replica from each IM (should be possible)
			for _, im := range intermediateMasters {
				replicas, err := ReadReplicaInstances(&im.GetInstance().Key)
				if err != nil {
					return result, err
				}
				sort.Sort(sort.Reverse(mdtstruct.InstancesByCountReplicas(replicas)))
				replicas = filterOSCInstances(replicas)
				if len(replicas) > 0 {
					result = append(result, replicas[0])
				}
			}
		}
	}
	{
		// Get 2 3rd tier replicas, if possible
		condition := `
			replication_depth = 3
			and cluster_name = ?
		`
		replicas, err := ReadInstancesByCondition(mconstant.MysqlDefaultQuery, condition, sqlutil.Args(clusterName), "")
		if err != nil {
			return result, err
		}
		sort.Sort(sort.Reverse(mdtstruct.InstancesByCountReplicas(replicas)))
		replicas = filterOSCInstances(replicas)
		replicas = replicas[0:math.MinInt(2, len(replicas))]
		result = append(result, replicas...)
	}
	{
		// Get 2 1st tier leaf replicas, if possible
		condition := `
			replication_depth = 1
			and downstream_count = 0
			and cluster_name = ?
		`
		replicas, err := ReadInstancesByCondition(mconstant.MysqlDefaultQuery, condition, sqlutil.Args(clusterName), "")
		if err != nil {
			return result, err
		}
		replicas = filterOSCInstances(replicas)
		replicas = replicas[0:math.MinInt(2, len(replicas))]
		result = append(result, replicas...)
	}

	return result, nil
}

// filterOSCInstances will filter the given list such that only replicas fit for OSC control remain.
func filterOSCInstances(instances []*mdtstruct.MysqlInstance) []*mdtstruct.MysqlInstance {
	result := []*mdtstruct.MysqlInstance{}
	for _, instance := range instances {
		if util.RegexpMatchPattern(instance.GetInstance().Key.StringCode(), config.Config.OSCIgnoreHostnameFilters) {
			continue
		}
		if instance.IsReplicaServer() {
			continue
		}
		if !instance.GetInstance().IsLastCheckValid {
			continue
		}
		result = append(result, instance)
	}
	return result
}

// ReadFromBackendDB reads an instance from the ham4db backend database
func ReadFromBackendDB(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, bool, error) {
	instances, err := readInstancesByExactKey(instanceKey)
	// We know there will be at most one (hostname & port are PK)
	// And we expect to find one
	//readInstanceCounter.Inc(1)
	if len(instances) == 0 {
		return nil, false, err
	}
	if err != nil {
		return instances[0], false, err
	}
	return instances[0], true, nil
}

func readInstancesByExactKey(instanceKey *dtstruct.InstanceKey) ([]*mdtstruct.MysqlInstance, error) {
	condition := `
			hostname = ?
			and port = ?
		`
	return ReadInstancesByCondition(mconstant.MysqlDefaultQuery, condition, sqlutil.Args(instanceKey.Hostname, instanceKey.Port), "")
}

// ReadReplicaInstances reads replicas of a given master
func ReadReplicaInstances(masterKey *dtstruct.InstanceKey) ([]*mdtstruct.MysqlInstance, error) {
	condition := `
			upstream_host = ?
			and upstream_port = ?
		`
	return ReadInstancesByCondition(mconstant.MysqlDefaultQuery, condition, sqlutil.Args(masterKey.Hostname, masterKey.Port), "")
}

// ReadInstancesByCondition is a generic function to read instances from the backend database
func ReadInstancesByCondition(query string, condition string, args []interface{}, sort string) ([]*mdtstruct.MysqlInstance, error) {
	readFunc := func() ([]*mdtstruct.MysqlInstance, error) {
		var instances []*mdtstruct.MysqlInstance

		if sort == "" {
			sort = `hostname, port`
		}
		sql := fmt.Sprintf(`
			%s
		where
			%s
		order by
			%s
			`, query, condition, sort)

		err := db.Query(sql, args, func(m sqlutil.RowMap) error {
			instance := RowToInstance(m)
			instances = append(instances, instance)
			return nil
		})
		if err != nil {
			return instances, log.Errore(err)
		}
		//err = PopulateInstancesAgents(instances)
		//if err != nil {
		//	return instances, log.Errore(err)
		//}
		return instances, err
	}
	instanceReadChan <- true
	instances, err := readFunc()
	<-instanceReadChan
	return instances, err
}

// ReadClusterInstancesByClusterIdOrHint reads all instances of a given cluster
func ReadClusterInstancesByClusterIdOrHint(request *dtstruct.Request) ([]*mdtstruct.MysqlInstance, error) {
	if request.ClusterId != "" {
		return ReadInstancesByCondition(mconstant.MysqlDefaultQuery, " cluster_id = ?", sqlutil.Args(request.ClusterId), "")
	}
	return ReadInstancesByCondition(mconstant.MysqlDefaultQuery, ` (cluster_id = ? or hostname = ? or cluster_name = ?) `, sqlutil.Args(request.Hint, request.Hint, request.Hint), "")
}

// ReadClusterInstances reads all instances of a given cluster
func ReadClusterInstances(clusterName string) ([]*mdtstruct.MysqlInstance, error) {
	if strings.Index(clusterName, "'") >= 0 {
		return []*mdtstruct.MysqlInstance{}, log.Errorf("Invalid cluster name: %s", clusterName)
	}
	condition := `cluster_name = ?`
	return ReadInstancesByCondition(mconstant.MysqlDefaultQuery, condition, sqlutil.Args(clusterName), "")
}

// ReadHistoryClusterInstances reads (thin) instances from history
func ReadHistoryClusterInstancesByClusterIdOrHint(request *dtstruct.Request, historyTimestampPattern string) ([]*mdtstruct.MysqlInstance, error) {
	instances := []*mdtstruct.MysqlInstance{}

	query := `
		select
			*
		from
			ham_database_instance_topology_history
		where
			snapshot_unix_timestamp rlike ?
			and (cluster_id = ? or (cluster_id like ? or hostname like ? or cluster_name like ?))
		order by
			hostname, port`

	err := db.Query(query, sqlutil.Args(historyTimestampPattern, request.ClusterId, request.Hint, request.Hint, request.Hint), func(m sqlutil.RowMap) error {
		instance := mdtstruct.NewInstance()

		instance.Key.Hostname = m.GetString("hostname")
		instance.Key.Port = m.GetInt("port")
		instance.UpstreamKey.Hostname = m.GetString("upstream_host")
		instance.UpstreamKey.Port = m.GetInt("upstream_port")
		instance.ClusterName = m.GetString("cluster_name")

		instances = append(instances, instance)
		return nil
	})
	if err != nil {
		return instances, log.Errore(err)
	}
	return instances, err
}

// ReadHistoryClusterInstances reads (thin) instances from history
func ReadHistoryClusterInstances(clusterName string, historyTimestampPattern string) ([]*mdtstruct.MysqlInstance, error) {
	instances := []*mdtstruct.MysqlInstance{}

	query := `
		select
			*
		from
			ham_database_instance_topology_history
		where
			snapshot_unix_timestamp rlike ?
			and cluster_name = ?
		order by
			hostname, port`

	err := db.Query(query, sqlutil.Args(historyTimestampPattern, clusterName), func(m sqlutil.RowMap) error {
		instance := mdtstruct.NewInstance()

		instance.Key.Hostname = m.GetString("hostname")
		instance.Key.Port = m.GetInt("port")
		instance.UpstreamKey.Hostname = m.GetString("upstream_host")
		instance.UpstreamKey.Port = m.GetInt("upstream_port")
		instance.ClusterName = m.GetString("cluster_name")

		instances = append(instances, instance)
		return nil
	})
	if err != nil {
		return instances, log.Errore(err)
	}
	return instances, err
}

// ReadClusterWriteableMaster returns the/a writeable master of this cluster
// Typically, the cluster name indicates the master of the cluster. However, in circular
// master-master replication one master can assume the name of the cluster, and it is
// not guaranteed that it is the writeable one.
func ReadClusterWriteableMaster(clusterName string) ([]*mdtstruct.MysqlInstance, error) {
	condition := `
		cluster_name = ?
		and is_read_only = 0
		and (replication_depth = 0 or is_co_master)
	`
	return ReadInstancesByCondition(mconstant.MysqlDefaultQuery, condition, sqlutil.Args(clusterName), "replication_depth asc")
}

// ReadClusterMaster returns the master of this cluster.
// - if the cluster has co-masters, the/a writable one is returned
// - if the cluster has a single master, that master is retuened whether it is read-only or writable.
func ReadClusterMaster(clusterName string) ([]*mdtstruct.MysqlInstance, error) {
	condition := `
		cluster_name = ?
		and (replication_depth = 0 or is_co_master)
	`
	return ReadInstancesByCondition(mconstant.MysqlDefaultQuery, condition, sqlutil.Args(clusterName), "is_read_only asc, replication_depth asc")
}

// ReadWriteableClustersMasters returns writeable masters of all clusters, but only one
// per cluster, in similar logic to ReadClusterWriteableMaster
func ReadWriteableClustersMasters() (instances []*mdtstruct.MysqlInstance, err error) {
	condition := `
		is_read_only = 0
		and (replication_depth = 0 or is_co_master)
	`
	allMasters, err := ReadInstancesByCondition(mconstant.MysqlDefaultQuery, condition, sqlutil.Args(), "cluster_name asc, replication_depth asc")
	if err != nil {
		return instances, err
	}
	visitedClusters := make(map[string]bool)
	for _, instance := range allMasters {
		if !visitedClusters[instance.GetInstance().ClusterName] {
			visitedClusters[instance.GetInstance().ClusterName] = true
			instances = append(instances, instance)
		}
	}
	return instances, err
}

// ReadUnseenInstances reads all instances which were not recently seen
func ReadUnseenInstances() ([]*mdtstruct.MysqlInstance, error) {
	condition := `last_seen_timestamp < last_checked_timestamp`
	return ReadInstancesByCondition(mconstant.MysqlDefaultQuery, condition, sqlutil.Args(), "")
}

// readInstanceRow reads a single instance row from the ham4db backend database.
func RowToInstance(m sqlutil.RowMap) *mdtstruct.MysqlInstance {

	instance := mdtstruct.NewInstance()

	instance.Key.DBType = m.GetString("db_type")
	instance.Key.Hostname = m.GetString("hostname")
	instance.Key.Port = m.GetInt("port")
	instance.Key.ClusterId = m.GetString("cluster_id")
	instance.Uptime = m.GetUint("uptime")
	instance.InstanceId = m.GetString("db_id")
	instance.ClusterId = m.GetString("cluster_id")
	instance.Version = m.GetString("db_version")
	instance.VersionComment = m.GetString("version_comment")
	instance.ReadOnly = m.GetBool("is_read_only")
	instance.Binlog_format = m.GetString("binlog_format")
	instance.BinlogRowImage = m.GetString("binlog_row_image")
	instance.LogBinEnabled = m.GetBool("log_bin")
	instance.LogReplicationUpdatesEnabled = m.GetBool("log_slave_updates")
	instance.UpstreamKey.Hostname = m.GetString("upstream_host")
	instance.UpstreamKey.Port = m.GetInt("upstream_port")
	instance.UpstreamKey.ClusterId = m.GetString("cluster_id")
	instance.UpstreamKey.DBType = m.GetString("db_type")
	instance.IsDetachedMaster = instance.UpstreamKey.IsDetached()
	instance.ReplicationSQLThreadRuning = m.GetBool("slave_sql_running")
	instance.ReplicationIOThreadRuning = m.GetBool("slave_io_running")
	instance.ReplicationSQLThreadState = mdtstruct.ReplicationThreadState(m.GetInt("replication_sql_thread_state"))
	instance.ReplicationIOThreadState = mdtstruct.ReplicationThreadState(m.GetInt("replication_io_thread_state"))
	instance.HasReplicationFilters = m.GetBool("has_replication_filters")
	instance.SupportsOracleGTID = m.GetBool("supports_oracle_gtid")
	instance.UsingOracleGTID = m.GetBool("oracle_gtid")
	instance.MasterUUID = m.GetString("master_uuid")
	instance.AncestryUUID = m.GetString("ancestry_uuid")
	instance.ExecutedGtidSet = m.GetString("executed_gtid_set")
	instance.GTIDMode = m.GetString("gtid_mode")
	instance.GtidPurged = m.GetString("gtid_purged")
	instance.GtidErrant = m.GetString("gtid_errant")
	instance.UsingMariaDBGTID = m.GetBool("mariadb_gtid")
	instance.UsingPseudoGTID = m.GetBool("pseudo_gtid")
	instance.SelfBinlogCoordinates.LogFile = m.GetString("binary_log_file")
	instance.SelfBinlogCoordinates.LogPos = m.GetInt64("binary_log_pos")
	instance.ReadBinlogCoordinates.LogFile = m.GetString("master_log_file")
	instance.ReadBinlogCoordinates.LogPos = m.GetInt64("read_master_log_pos")
	instance.ExecBinlogCoordinates.LogFile = m.GetString("relay_master_log_file")
	instance.ExecBinlogCoordinates.LogPos = m.GetInt64("exec_master_log_pos")
	instance.IsDetached, _ = instance.ExecBinlogCoordinates.ExtractDetachedCoordinates()
	instance.RelaylogCoordinates.LogFile = m.GetString("relay_log_file")
	instance.RelaylogCoordinates.LogPos = m.GetInt64("relay_log_pos")
	instance.RelaylogCoordinates.Type = constant.RelayLog
	instance.LastSQLError = m.GetString("last_sql_error")
	instance.LastIOError = m.GetString("last_io_error")
	instance.SecondsBehindMaster = m.GetNullInt64("seconds_behind_master")
	instance.ReplicationLagSeconds = m.GetNullInt64("replication_downstream_lag")
	instance.SQLDelay = m.GetUint("sql_delay")
	replicasJSON := m.GetString("downstream_hosts")
	instance.ClusterName = m.GetString("cluster_name")
	instance.SuggestedClusterAlias = m.GetString("cluster_alias")
	instance.DataCenter = m.GetString("pl_data_center")
	instance.Region = m.GetString("pl_region")
	instance.Environment = m.GetString("environment")
	instance.SemiSyncEnforced = m.GetBool("semi_sync_enforced")
	instance.SemiSyncAvailable = m.GetBool("semi_sync_available")
	instance.SemiSyncMasterEnabled = m.GetBool("semi_sync_master_enabled")
	instance.SemiSyncMasterTimeout = m.GetUint64("semi_sync_master_timeout")
	instance.SemiSyncMasterWaitForReplicaCount = m.GetUint("semi_sync_master_wait_for_slave_count")
	instance.SemiSyncReplicaEnabled = m.GetBool("semi_sync_replica_enabled")
	instance.SemiSyncMasterStatus = m.GetBool("semi_sync_master_status")
	instance.SemiSyncMasterClients = m.GetUint("semi_sync_master_clients")
	instance.SemiSyncReplicaStatus = m.GetBool("semi_sync_replica_status")
	instance.ReplicationDepth = m.GetUint("replication_depth")
	instance.IsCoUpstream = m.GetBool("is_co_master")
	instance.ReplicationCredentialsAvailable = m.GetBool("is_replication_credentials_available")
	instance.HasReplicationCredentials = m.GetBool("has_replication_credentials")
	instance.IsUpToDate = (m.GetUint("seconds_since_last_checked") <= config.Config.InstancePollSeconds)
	instance.IsRecentlyChecked = (m.GetUint("seconds_since_last_checked") <= config.Config.InstancePollSeconds*5)
	instance.LastSeenTimestamp = m.GetString("last_seen_timestamp")
	instance.IsLastCheckValid = m.GetBool("is_last_check_valid")
	instance.SecondsSinceLastSeen = m.GetNullInt64("seconds_since_last_seen")
	instance.IsCandidate = m.GetBool("is_candidate")
	instance.PromotionRule = dtstruct.CandidatePromotionRule(m.GetString("promotion_rule"))
	instance.IsDowntimed = m.GetBool("is_downtimed")
	instance.DowntimeReason = m.GetString("downtime_reason")
	instance.DowntimeOwner = m.GetString("downtime_owner")
	instance.DowntimeEndTimestamp = m.GetString("downtime_end_timestamp")
	instance.ElapsedDowntime = time.Second * time.Duration(m.GetInt("elapsed_downtime_seconds"))
	instance.UnresolvedHostname = m.GetString("unresolved_hostname")
	instance.AllowTLS = m.GetBool("is_allow_tls")
	instance.InstanceAlias = m.GetString("db_alias")
	instance.LastDiscoveryLatency = time.Duration(m.GetInt64("last_discovery_latency")) * time.Nanosecond

	instance.DownstreamKeyMap.ReadJson(replicasJSON)
	instance.SetFlavorName()

	/* Read Group Replication variables below */
	instance.ReplicationGroupName = m.GetString("replication_group_name")
	instance.ReplicationGroupIsSinglePrimary = m.GetBool("replication_group_is_single_primary_mode")
	instance.ReplicationGroupMemberState = m.GetString("replication_group_member_state")
	instance.ReplicationGroupMemberRole = m.GetString("replication_group_member_role")
	instance.ReplicationGroupPrimaryInstanceKey = dtstruct.InstanceKey{
		Hostname:  m.GetString("replication_group_primary_host"),
		Port:      m.GetInt("replication_group_primary_port"),
		DBType:    m.GetString("db_type"),
		ClusterId: m.GetString("cluster_id"),
	}
	instance.ReplicationGroupMembers.ReadJson(m.GetString("replication_group_members"))
	//instance.ReplicationGroup = m.GetString("replication_group_")

	// problems
	if !instance.IsLastCheckValid {
		instance.Problems = append(instance.Problems, "last_check_invalid")
	} else if !instance.IsRecentlyChecked {
		instance.Problems = append(instance.Problems, "not_recently_checked")
	} else if instance.ReplicationThreadsExist() && !instance.ReplicaRunning() {
		instance.Problems = append(instance.Problems, "not_replicating")
	} else if instance.ReplicationLagSeconds.Valid && math.AbsInt64(instance.ReplicationLagSeconds.Int64-int64(instance.SQLDelay)) > int64(config.Config.ReasonableReplicationLagSeconds) {
		instance.Problems = append(instance.Problems, "replication_lag")
	}
	if instance.GtidErrant != "" {
		instance.Problems = append(instance.Problems, "errant_gtid")
	}
	// Group replication problems
	if instance.ReplicationGroupName != "" && instance.ReplicationGroupMemberState != mconstant.GroupReplicationMemberStateOnline {
		instance.Problems = append(instance.Problems, "group_replication_member_not_online")
	}
	return instance
}

func GetInfoFromInstance(instanceKey *dtstruct.InstanceKey, checkOnly, bufferWrites bool, latency *stopwatch.NamedStopwatch, agent string) (*mdtstruct.MysqlInstance, error) {
	defer func() {
		if r := recover(); r != nil {
			cache.LogReadTopologyInstanceError(instanceKey, "Unexpected, aborting", fmt.Errorf("%+v", r))
		}
	}()

	if !instanceKey.IsValid() {
		latency.Start("backend")
		if err := base.UpdateInstanceLastAttemptedCheck(instanceKey); err != nil {
			log.Errorf("ReadTopologyInstanceBufferable: %+v: %v", instanceKey, err)
		}
		latency.Stop("backend")
		return mdtstruct.NewInstance(), fmt.Errorf("ReadTopologyInstance will not act on invalid instance key: %+v", *instanceKey)
	}

	var waitGroup sync.WaitGroup
	var serverUuidWaitGroup sync.WaitGroup
	readingStartTime := time.Now()
	instance := mdtstruct.NewInstance()
	if instanceKey.ClusterId != "" {
		instance.ClusterId = instanceKey.ClusterId
	}
	instanceFound := false
	partialSuccess := false
	foundByShowSlaveHosts := false
	resolvedHostname := ""
	maxScaleMasterHostname := ""
	isMaxScale := false
	isMaxScale110 := false
	slaveStatusFound := false
	errorChan := make(chan error, 32)
	var resolveErr error

	lastAttemptedCheckTimer := time.AfterFunc(time.Second, func() {
		go base.UpdateInstanceLastAttemptedCheck(instanceKey)
	})

	latency.Start("instance")
	sdb, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	latency.Stop("instance")
	if err != nil {
		goto Cleanup
	}

	instance.Key = *instanceKey

	if isMaxScale, resolvedHostname, err = CheckMaxScale(instance, sdb, latency); err != nil {
		// We do not "goto Cleanup" here, although it should be the correct flow.
		// Reason is 5.7's new security feature that requires GRANTs on performance_schema.session_variables.
		// There is a wrong decision making in this design and the migration path to 5.7 will be difficult.
		// I don't want ham4db to put even more burden on this.
		// If the statement errors, then we are unable to determine that this is maxscale, hence assume it is not.
		// In which case there would be other queries sent to the server that are not affected by 5.7 behavior, and that will fail.

		// Certain errors are not recoverable (for this discovery process) so it's fine to go to Cleanup
		if mutil.UnrecoverableError(err) {
			goto Cleanup
		}
	}

	latency.Start("instance")
	if isMaxScale {
		if strings.Contains(instance.Version, "1.1.0") {
			isMaxScale110 = true

			// Buggy buggy maxscale 1.1.0. Reported Master_Host can be corrupted.
			// Therefore we (currently) take @@hostname (which is masquerading as master host anyhow)
			err = sdb.QueryRow("select @@hostname").Scan(&maxScaleMasterHostname)
			if err != nil {
				goto Cleanup
			}
		}
		if isMaxScale110 {
			// Only this is supported:
			sdb.QueryRow("select @@server_id").Scan(&instance.InstanceId)
		} else {
			sdb.QueryRow("select @@global.server_id").Scan(&instance.InstanceId)
			sdb.QueryRow("select @@global.server_uuid").Scan(&instance.ServerUUID)
		}
	} else {
		// NOT MaxScale

		// We begin with a few operations we can run concurrently, and which do not depend on anything
		{
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				var dummy string
				// show global status works just as well with 5.6 & 5.7 (5.7 moves variables to performance_schema)
				err := sdb.QueryRow("show global status like 'Uptime'").Scan(&dummy, &instance.Uptime)

				if err != nil {
					cache.LogReadTopologyInstanceError(instanceKey, "show global status like 'Uptime'", err)

					// We do not "goto Cleanup" here, although it should be the correct flow.
					// Reason is 5.7's new security feature that requires GRANTs on performance_schema.global_variables.
					// There is a wrong decisionmaking in this design and the migration path to 5.7 will be difficult.
					// I don't want ham4db to put even more burden on this. The 'Uptime' variable is not that important
					// so as to completely fail reading a 5.7 instance.
					// This is supposed to be fixed in 5.7.9
				}
				errorChan <- err
			}()
		}

		var mysqlHostname, mysqlReportHost string
		err = sdb.QueryRow("select @@global.hostname, ifnull(@@global.report_host, ''), @@global.server_id, @@global.version, @@global.version_comment, @@global.read_only, @@global.binlog_format, @@global.log_bin, @@global.log_slave_updates").Scan(
			&mysqlHostname, &mysqlReportHost, &instance.InstanceId, &instance.Version, &instance.VersionComment, &instance.ReadOnly, &instance.Binlog_format, &instance.LogBinEnabled, &instance.LogReplicationUpdatesEnabled)
		if err != nil {
			goto Cleanup
		}
		partialSuccess = true // We at least managed to read something from the server.
		switch strings.ToLower(config.Config.MySQLHostnameResolveMethod) {
		case "none":
			resolvedHostname = instance.Key.Hostname
		case "default", "hostname", "@@hostname":
			resolvedHostname = mysqlHostname
		case "report_host", "@@report_host":
			if mysqlReportHost == "" {
				err = fmt.Errorf("MySQLHostnameResolveMethod configured to use @@report_host but %+v has NULL/empty @@report_host", instanceKey)
				goto Cleanup
			}
			resolvedHostname = mysqlReportHost
		default:
			resolvedHostname = instance.Key.Hostname
		}

		// update port from mysql global variable port
		var mysqlPort int
		if err = sdb.QueryRow("select @@global.port").Scan(&mysqlPort); err != nil {
			goto Cleanup
		}
		instance.Key.Port = mysqlPort

		if instance.LogBinEnabled {
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				err := sqlutil.QueryRowsMap(sdb, "show master status", func(m sqlutil.RowMap) error {
					var err error
					instance.SelfBinlogCoordinates.LogFile = m.GetString("File")
					instance.SelfBinlogCoordinates.LogPos = m.GetInt64("Position")
					return err
				})
				errorChan <- err
			}()
		}

		{
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				semiSyncMasterPluginLoaded := false
				semiSyncReplicaPluginLoaded := false
				err := sqlutil.QueryRowsMap(sdb, "show global variables like 'rpl_semi_sync_%'", func(m sqlutil.RowMap) error {
					if m.GetString("Variable_name") == "rpl_semi_sync_master_enabled" {
						instance.SemiSyncMasterEnabled = (m.GetString("Value") == "ON")
						semiSyncMasterPluginLoaded = true
					} else if m.GetString("Variable_name") == "rpl_semi_sync_master_timeout" {
						instance.SemiSyncMasterTimeout = m.GetUint64("Value")
					} else if m.GetString("Variable_name") == "rpl_semi_sync_master_wait_for_slave_count" {
						instance.SemiSyncMasterWaitForReplicaCount = m.GetUint("Value")
					} else if m.GetString("Variable_name") == "rpl_semi_sync_slave_enabled" {
						instance.SemiSyncReplicaEnabled = (m.GetString("Value") == "ON")
						semiSyncReplicaPluginLoaded = true
					}
					return nil
				})
				instance.SemiSyncAvailable = (semiSyncMasterPluginLoaded && semiSyncReplicaPluginLoaded)
				errorChan <- err
			}()
		}
		{
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				err := sqlutil.QueryRowsMap(sdb, "show global status like 'rpl_semi_sync_%'", func(m sqlutil.RowMap) error {
					if m.GetString("Variable_name") == "Rpl_semi_sync_master_status" {
						instance.SemiSyncMasterStatus = (m.GetString("Value") == "ON")
					} else if m.GetString("Variable_name") == "Rpl_semi_sync_master_clients" {
						instance.SemiSyncMasterClients = m.GetUint("Value")
					} else if m.GetString("Variable_name") == "Rpl_semi_sync_slave_status" {
						instance.SemiSyncReplicaStatus = (m.GetString("Value") == "ON")
					}

					return nil
				})
				errorChan <- err
			}()
		}
		if (instance.IsOracleMySQL() || instance.IsPercona()) && !instance.IsSmallerMajorVersionByString("5.6") {
			waitGroup.Add(1)
			serverUuidWaitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				defer serverUuidWaitGroup.Done()
				var masterInfoRepositoryOnTable bool
				// Stuff only supported on Oracle MySQL >= 5.6
				// ...
				// @@gtid_mode only available in Orcale MySQL >= 5.6
				// Previous version just issued this query brute-force, but I don't like errors being issued where they shouldn't.
				_ = sdb.QueryRow("select @@global.gtid_mode, @@global.server_uuid, @@global.gtid_executed, @@global.gtid_purged, @@global.master_info_repository = 'TABLE', @@global.binlog_row_image").Scan(&instance.GTIDMode, &instance.InstanceId, &instance.ExecutedGtidSet, &instance.GtidPurged, &masterInfoRepositoryOnTable, &instance.BinlogRowImage)
				if instance.GTIDMode != "" && instance.GTIDMode != "OFF" {
					instance.SupportsOracleGTID = true
				}
				if config.Config.ReplicationCredentialsQuery != "" {
					instance.ReplicationCredentialsAvailable = true
				} else if masterInfoRepositoryOnTable {
					_ = sdb.QueryRow("select count(*) > 0 and MAX(User_name) != '' from mysql.slave_master_info").Scan(&instance.ReplicationCredentialsAvailable)
				}
			}()
		}
	}
	if resolvedHostname != instance.Key.Hostname {
		latency.Start("backend")
		base.UpdateResolvedHostname(instance.Key.Hostname, resolvedHostname)
		latency.Stop("backend")
		instance.Key.Hostname = resolvedHostname
	}
	if instance.Key.Hostname == "" {
		err = fmt.Errorf("ReadTopologyInstance: empty hostname (%+v). Bailing out", *instanceKey)
		goto Cleanup
	}
	go base.ResolveHostnameIPs(instance.Key.Hostname)
	if config.Config.DataCenterPattern != "" {
		if pattern, err := regexp.Compile(config.Config.DataCenterPattern); err == nil {
			match := pattern.FindStringSubmatch(instance.Key.Hostname)
			if len(match) != 0 {
				instance.DataCenter = match[1]
			}
		}
		// This can be overriden by later invocation of DetectDataCenterQuery
	}
	if config.Config.RegionPattern != "" {
		if pattern, err := regexp.Compile(config.Config.RegionPattern); err == nil {
			match := pattern.FindStringSubmatch(instance.Key.Hostname)
			if len(match) != 0 {
				instance.Region = match[1]
			}
		}
		// This can be overriden by later invocation of DetectRegionQuery
	}
	if config.Config.EnvironmentPattern != "" {
		if pattern, err := regexp.Compile(config.Config.EnvironmentPattern); err == nil {
			match := pattern.FindStringSubmatch(instance.Key.Hostname)
			if len(match) != 0 {
				instance.Environment = match[1]
			}
		}
		// This can be overriden by later invocation of DetectEnvironmentQuery
	}

	instance.ReplicationIOThreadState = mdtstruct.ReplicationThreadStateNoThread
	instance.ReplicationSQLThreadState = mdtstruct.ReplicationThreadStateNoThread
	err = sqlutil.QueryRowsMap(sdb, "show slave status", func(m sqlutil.RowMap) error {
		instance.HasReplicationCredentials = (m.GetString("Master_User") != "")
		instance.ReplicationIOThreadState = mdtstruct.ReplicationThreadStateFromStatus(m.GetString("Slave_IO_Running"))
		instance.ReplicationSQLThreadState = mdtstruct.ReplicationThreadStateFromStatus(m.GetString("Slave_SQL_Running"))
		instance.ReplicationIOThreadRuning = instance.ReplicationIOThreadState.IsRunning()
		if isMaxScale110 {
			// Covering buggy MaxScale 1.1.0
			instance.ReplicationIOThreadRuning = instance.ReplicationIOThreadRuning && (m.GetString("Slave_IO_State") == "Binlog Dump")
		}
		instance.ReplicationSQLThreadRuning = instance.ReplicationSQLThreadState.IsRunning()
		instance.ReadBinlogCoordinates.LogFile = m.GetString("Master_Log_File")
		instance.ReadBinlogCoordinates.LogPos = m.GetInt64("Read_Master_Log_Pos")
		instance.ExecBinlogCoordinates.LogFile = m.GetString("Relay_Master_Log_File")
		instance.ExecBinlogCoordinates.LogPos = m.GetInt64("Exec_Master_Log_Pos")
		instance.IsDetached, _ = instance.ExecBinlogCoordinates.ExtractDetachedCoordinates()
		instance.RelaylogCoordinates.LogFile = m.GetString("Relay_Log_File")
		instance.RelaylogCoordinates.LogPos = m.GetInt64("Relay_Log_Pos")
		instance.RelaylogCoordinates.Type = constant.RelayLog
		instance.LastSQLError = common.EmptyQuotesRegexp.ReplaceAllString(strconv.QuoteToASCII(m.GetString("Last_SQL_Error")), "")
		instance.LastIOError = common.EmptyQuotesRegexp.ReplaceAllString(strconv.QuoteToASCII(m.GetString("Last_IO_Error")), "")
		instance.SQLDelay = m.GetUintD("SQL_Delay", 0)
		instance.UsingOracleGTID = (m.GetIntD("Auto_Position", 0) == 1)
		instance.UsingMariaDBGTID = (m.GetStringD("Using_Gtid", "No") != "No")
		instance.MasterUUID = m.GetStringD("Master_UUID", "No")
		instance.HasReplicationFilters = ((m.GetStringD("Replicate_Do_DB", "") != "") || (m.GetStringD("Replicate_Ignore_DB", "") != "") || (m.GetStringD("Replicate_Do_Table", "") != "") || (m.GetStringD("Replicate_Ignore_Table", "") != "") || (m.GetStringD("Replicate_Wild_Do_Table", "") != "") || (m.GetStringD("Replicate_Wild_Ignore_Table", "") != ""))

		masterHostname := m.GetString("Master_Host")
		if isMaxScale110 {
			// Buggy buggy maxscale 1.1.0. Reported Master_Host can be corrupted.
			// Therefore we (currently) take @@hostname (which is masquarading as master host anyhow)
			masterHostname = maxScaleMasterHostname
		}
		masterKey, err := base.NewResolveInstanceKey(instance.Key.DBType, masterHostname, m.GetInt("Master_Port"))
		if err != nil {
			cache.LogReadTopologyInstanceError(instanceKey, "NewResolveInstanceKey", err)
		}
		masterKey.Hostname, resolveErr = base.ResolveHostname(masterKey.Hostname)
		if resolveErr != nil {
			cache.LogReadTopologyInstanceError(instanceKey, fmt.Sprintf("ResolveHostname(%q)", masterKey.Hostname), resolveErr)
		}
		masterKey.ClusterId = instance.ClusterId
		instance.UpstreamKey = *masterKey
		instance.IsDetachedMaster = instance.UpstreamKey.IsDetached()
		instance.SecondsBehindMaster = m.GetNullInt64("Seconds_Behind_Master")
		if instance.SecondsBehindMaster.Valid && instance.SecondsBehindMaster.Int64 < 0 {
			log.Warningf("Host: %+v, instance.SecondsBehindMaster < 0 [%+v], correcting to 0", instanceKey, instance.SecondsBehindMaster.Int64)
			instance.SecondsBehindMaster.Int64 = 0
		}
		// And until told otherwise:
		instance.ReplicationLagSeconds = instance.SecondsBehindMaster

		instance.AllowTLS = (m.GetString("Master_SSL_Allowed") == "Yes")
		// Not breaking the flow even on error
		slaveStatusFound = true
		return nil
	})
	if err != nil {
		goto Cleanup
	}
	// Populate GR information for the instance in Oracle MySQL 8.0+. To do this we need to wait for the Server UUID to
	// be populated to be able to find this instance's information in performance_schema.replication_group_members by
	// comparing UUIDs. We could instead resolve the MEMBER_HOST and MEMBER_PORT columns into an InstanceKey and compare
	// those instead, but this could require external calls for name resolving, whereas comparing UUIDs does not.
	serverUuidWaitGroup.Wait()
	if instance.IsOracleMySQL() && !instance.IsSmallerMajorVersionByString("8.0") {
		err := PopulateGroupReplicationInformation(instance, sdb)
		if err != nil {
			goto Cleanup
		}
	}
	if isMaxScale && !slaveStatusFound {
		err = fmt.Errorf("No 'SHOW SLAVE STATUS' output found for a MaxScale instance: %+v", instanceKey)
		goto Cleanup
	}

	if config.Config.ReplicationLagQuery != "" && !isMaxScale {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			if err := sdb.QueryRow(config.Config.ReplicationLagQuery).Scan(&instance.ReplicationLagSeconds); err == nil {
				if instance.ReplicationLagSeconds.Valid && instance.ReplicationLagSeconds.Int64 < 0 {
					log.Warningf("Host: %+v, instance.SlaveLagSeconds < 0 [%+v], correcting to 0", instanceKey, instance.ReplicationLagSeconds.Int64)
					instance.ReplicationLagSeconds.Int64 = 0
				}
			} else {
				instance.ReplicationLagSeconds = instance.SecondsBehindMaster
				cache.LogReadTopologyInstanceError(instanceKey, "ReplicationLagQuery", err)
			}
		}()
	}

	instanceFound = true

	// -------------------------------------------------------------------------
	// Anything after this point does not affect the fact the instance is found.
	// No `goto Cleanup` after this point.
	// -------------------------------------------------------------------------

	// Get replicas, either by SHOW SLAVE HOSTS or via PROCESSLIST
	// MaxScale does not support PROCESSLIST, so SHOW SLAVE HOSTS is the only option
	if config.Config.DiscoverByShowSlaveHosts || isMaxScale {
		err := sqlutil.QueryRowsMap(sdb, `show slave hosts`,
			func(m sqlutil.RowMap) error {
				// MaxScale 1.1 may trigger an error with this command, but
				// also we may see issues if anything on the MySQL server locks up.
				// Consequently it's important to validate the values received look
				// good prior to calling ResolveHostname()
				host := m.GetString("Host")
				port := m.GetIntD("Port", 0)
				if host == "" || port == 0 {
					if isMaxScale && host == "" && port == 0 {
						// MaxScale reports a bad response sometimes so ignore it.
						// - seen in 1.1.0 and 1.4.3.4
						return nil
					}
					// otherwise report the error to the caller
					return fmt.Errorf("ReadTopologyInstance(%+v) 'show slave hosts' returned row with <host,port>: <%v,%v>", instanceKey, host, port)
				}

				replicaKey, err := base.NewResolveInstanceKey(instance.Key.DBType, host, port)
				replicaKey.ClusterId = instance.ClusterId

				if err == nil && replicaKey.IsValid() {
					if !util.RegexpMatchPattern(replicaKey.StringCode(), config.Config.DiscoveryIgnoreReplicaHostnameFilters) {
						instance.AddDownstreamKey(replicaKey)
					}
					foundByShowSlaveHosts = true
				}
				return err
			})

		cache.LogReadTopologyInstanceError(instanceKey, "show slave hosts", err)
	}
	if !foundByShowSlaveHosts && !isMaxScale {
		// Either not configured to read SHOW SLAVE HOSTS or nothing was there.
		// Discover by information_schema.processlist
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := sqlutil.QueryRowsMap(sdb, `
		select
			substring_index(host, ':', 1) as slave_hostname
		from
			information_schema.processlist
		where
          command IN ('Binlog Dump', 'Binlog Dump GTID')
  		`,
				func(m sqlutil.RowMap) error {
					cname, resolveErr := base.ResolveHostname(m.GetString("slave_hostname"))
					if resolveErr != nil {
						cache.LogReadTopologyInstanceError(instanceKey, "ResolveHostname: processlist", resolveErr)
					}
					replicaKey := dtstruct.InstanceKey{DBType: instance.Key.DBType, Hostname: cname, Port: instance.Key.Port, ClusterId: instance.ClusterId}
					if !util.RegexpMatchPattern(replicaKey.StringCode(), config.Config.DiscoveryIgnoreReplicaHostnameFilters) {
						instance.AddDownstreamKey(&replicaKey)
					}
					return err
				})

			cache.LogReadTopologyInstanceError(instanceKey, "processlist", err)
		}()
	}

	if instance.IsNDB() {
		// Discover by ndbinfo about MySQL Cluster SQL nodes
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := sqlutil.QueryRowsMap(sdb, `
      	select
      		substring(service_URI,9) mysql_host
      	from
      		ndbinfo.processes
      	where
          process_name='mysqld'
  		`,
				func(m sqlutil.RowMap) error {
					cname, resolveErr := base.ResolveHostname(m.GetString("mysql_host"))
					if resolveErr != nil {
						cache.LogReadTopologyInstanceError(instanceKey, "ResolveHostname: ndbinfo", resolveErr)
					}
					replicaKey := dtstruct.InstanceKey{DBType: instance.Key.DBType, Hostname: cname, Port: instance.Key.Port, ClusterId: instance.ClusterId}
					instance.AddDownstreamKey(&replicaKey)
					return err
				})

			cache.LogReadTopologyInstanceError(instanceKey, "ndbinfo", err)
		}()
	}

	if config.Config.DetectDataCenterQuery != "" && !isMaxScale {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := sdb.QueryRow(config.Config.DetectDataCenterQuery).Scan(&instance.DataCenter)
			cache.LogReadTopologyInstanceError(instanceKey, "DetectDataCenterQuery", err)
		}()
	}

	if config.Config.DetectRegionQuery != "" && !isMaxScale {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := sdb.QueryRow(config.Config.DetectRegionQuery).Scan(&instance.Region)
			cache.LogReadTopologyInstanceError(instanceKey, "DetectRegionQuery", err)
		}()
	}

	if config.Config.DetectEnvironmentQuery != "" && !isMaxScale {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := sdb.QueryRow(config.Config.DetectEnvironmentQuery).Scan(&instance.Environment)
			cache.LogReadTopologyInstanceError(instanceKey, "DetectEnvironmentQuery", err)
		}()
	}

	if config.Config.DetectInstanceAliasQuery != "" && !isMaxScale {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := sdb.QueryRow(config.Config.DetectInstanceAliasQuery).Scan(&instance.InstanceAlias)
			cache.LogReadTopologyInstanceError(instanceKey, "DetectInstanceAliasQuery", err)
		}()
	}

	if config.Config.DetectSemiSyncEnforcedQuery != "" && !isMaxScale {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			err := sdb.QueryRow(config.Config.DetectSemiSyncEnforcedQuery).Scan(&instance.SemiSyncEnforced)
			cache.LogReadTopologyInstanceError(instanceKey, "DetectSemiSyncEnforcedQuery", err)
		}()
	}

	{
		latency.Start("backend")
		err = base.ReadInstanceClusterAttributes(instance)
		latency.Stop("backend")
		cache.LogReadTopologyInstanceError(instanceKey, "ReadInstanceClusterAttributes", err)
	}

	{
		// Pseudo GTID
		// Depends on ReadInstanceClusterAttributes above
		instance.UsingPseudoGTID = false
		if config.Config.AutoPseudoGTID {
			var err error
			instance.UsingPseudoGTID, err = IsInjectedPseudoGTID(instance.ClusterName)
			log.Errore(err)
		} else if config.Config.DetectPseudoGTIDQuery != "" {
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				if resultData, err := sqlutil.QueryResultData(sdb, config.Config.DetectPseudoGTIDQuery); err == nil {
					if len(resultData) > 0 {
						if len(resultData[0]) > 0 {
							if resultData[0][0].Valid && resultData[0][0].String == "1" {
								instance.UsingPseudoGTID = true
							}
						}
					}
				} else {
					cache.LogReadTopologyInstanceError(instanceKey, "DetectPseudoGTIDQuery", err)
				}
			}()
		}
	}

	// First read the current PromotionRule from candidate_database_instance.
	{
		latency.Start("backend")
		err = base.ReadInstancePromotionRule(instance)
		latency.Stop("backend")
		cache.LogReadTopologyInstanceError(instanceKey, "ReadInstancePromotionRule", err)
	}
	// Then check if the instance wants to set a different PromotionRule.
	// We'll set it here on their behalf so there's no race between the first
	// time an instance is discovered, and setting a rule like "must_not".
	if config.Config.DetectPromotionRuleQuery != "" && !isMaxScale {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			var value string
			err := sdb.QueryRow(config.Config.DetectPromotionRuleQuery).Scan(&value)
			cache.LogReadTopologyInstanceError(instanceKey, "DetectPromotionRuleQuery", err)
			promotionRule, err := dtstruct.ParseCandidatePromotionRule(value)
			cache.LogReadTopologyInstanceError(instanceKey, "ParseCandidatePromotionRule", err)
			if err == nil {
				// We need to update candidate_database_instance.
				// We register the rule even if it hasn't changed,
				// to bump the last_suggested_timestamp time.
				instance.PromotionRule = promotionRule
				err = base.RegisterCandidateInstance(base.WithCurrentTime(dtstruct.NewCandidateDatabaseInstance(*instanceKey, promotionRule)))
				cache.LogReadTopologyInstanceError(instanceKey, "RegisterCandidateInstance", err)
			}
		}()
	}

	base.ReadClusterAliasOverride(instance.Instance)
	if !isMaxScale {
		if instance.SuggestedClusterAlias == "" {
			// Only need to do on masters
			if config.Config.DetectClusterAliasQuery != "" {
				clusterAlias := ""
				if err := sdb.QueryRow(config.Config.DetectClusterAliasQuery).Scan(&clusterAlias); err != nil {
					cache.LogReadTopologyInstanceError(instanceKey, "DetectClusterAliasQuery", err)
				} else {
					instance.SuggestedClusterAlias = clusterAlias
				}
			}
		}
		if instance.SuggestedClusterAlias == "" {
			// Not found by DetectClusterAliasQuery...
			// See if a ClusterNameToAlias configuration applies
			if clusterAlias := dtstruct.MappedClusterNameToAlias(instance.ClusterName); clusterAlias != "" {
				instance.SuggestedClusterAlias = clusterAlias
			}
		}
	}
	if instance.ReplicationDepth == 0 && config.Config.DetectClusterDomainQuery != "" && !isMaxScale {
		// Only need to do on masters
		domainName := ""
		if err := sdb.QueryRow(config.Config.DetectClusterDomainQuery).Scan(&domainName); err != nil {
			domainName = ""
			cache.LogReadTopologyInstanceError(instanceKey, "DetectClusterDomainQuery", err)
		}
		if domainName != "" {
			latency.Start("backend")
			err := base.WriteClusterDomainName(instance.ClusterName, domainName)
			latency.Stop("backend")
			cache.LogReadTopologyInstanceError(instanceKey, "WriteClusterDomainName", err)
		}
	}

Cleanup:
	waitGroup.Wait()
	close(errorChan)
	err = func() error {
		if err != nil {
			return err
		}

		for err := range errorChan {
			if err != nil {
				return err
			}
		}
		return nil
	}()
	if checkOnly {
		return instance, err
	}
	if instanceFound {
		if instance.IsCoUpstream {
			// Take co-master into account, and avoid infinite loop
			instance.AncestryUUID = fmt.Sprintf("%s,%s", instance.MasterUUID, instance.InstanceId)
		} else {
			instance.AncestryUUID = fmt.Sprintf("%s,%s", instance.AncestryUUID, instance.InstanceId)
		}
		// Add replication group ancestry UUID as well. Otherwise, thinks there are errant GTIDs in group
		// members and its slaves, even though they are not.
		instance.AncestryUUID = fmt.Sprintf("%s,%s", instance.AncestryUUID, instance.ReplicationGroupName)
		instance.AncestryUUID = strings.Trim(instance.AncestryUUID, ",")
		if instance.ExecutedGtidSet != "" && instance.MasterExecutedGtidSet != "" {
			// Compare master & replica GTID sets, but ignore the sets that present the master's UUID.
			// This is because ham4db may pool master and replica at an inconvenient timing,
			// such that the replica may _seems_ to have more entries than the master, when in fact
			// it's just that the master's probing is stale.
			redactedExecutedGtidSet, _ := NewOracleGtidSet(instance.ExecutedGtidSet)
			for _, uuid := range strings.Split(instance.AncestryUUID, ",") {
				if uuid != instance.InstanceId {
					redactedExecutedGtidSet.RemoveUUID(uuid)
				}
				if instance.IsCoUpstream && uuid == instance.InstanceId {
					// If this is a co-master, then this server is likely to show its own generated GTIDs as errant,
					// because its co-master has not applied them yet
					redactedExecutedGtidSet.RemoveUUID(uuid)
				}
			}
			// Avoid querying the database if there's no point:
			if !redactedExecutedGtidSet.IsEmpty() {
				redactedMasterExecutedGtidSet, _ := NewOracleGtidSet(instance.MasterExecutedGtidSet)
				redactedMasterExecutedGtidSet.RemoveUUID(instance.MasterUUID)

				sdb.QueryRow("select gtid_subtract(?, ?)", redactedExecutedGtidSet.String(), redactedMasterExecutedGtidSet.String()).Scan(&instance.GtidErrant)
			}
		}
	}

	latency.Stop("instance")
	ReadTopologyInstanceCounter.Inc(1)

	if instanceFound {
		instance.LastDiscoveryLatency = time.Since(readingStartTime)
		instance.IsLastCheckValid = true
		instance.IsRecentlyChecked = true
		instance.IsUpToDate = true
		latency.Start("backend")
		if bufferWrites {
			limiter.EnqueueInstanceWrite(instance, instanceFound, err)
		} else {
			WriteToBackendDB([]dtstruct.InstanceAdaptor{instance}, instanceFound, true)
		}
		lastAttemptedCheckTimer.Stop()
		latency.Stop("backend")
		return instance, nil
	}

	// Something is wrong, could be network-wise. Record that we
	// tried to check the instance. last_attempted_check_timestamp is also
	// updated on success by writeInstance.
	latency.Start("backend")
	_ = base.UpdateInstanceLastChecked(&instance.Key, partialSuccess)
	latency.Stop("backend")
	return nil, err
}

func WriteToBackendDB(instances []dtstruct.InstanceAdaptor, instanceWasActuallyFound bool, updateLastSeen bool) error {
	if len(instances) == 0 {
		return nil
	}

	insertIgnore := false
	if !instanceWasActuallyFound {
		insertIgnore = true
	}

	// for ham_database_instance
	var instance_columns = []string{
		"hostname",
		"port",
		"db_type",
		"cluster_id",
		"last_checked_timestamp",
		"last_attempted_check_timestamp",
		"is_last_check_partial_success",
		"uptime",
		"db_id",
		"db_version",
		"is_read_only",
		"upstream_host",
		"upstream_port",
		"has_replication_filters",
		"replication_downstream_lag",
		"downstream_count",
		"downstream_hosts",
		"cluster_name",
		"cluster_alias",
		"pl_data_center",
		"pl_region",
		"environment",
		"replication_depth",
		"is_co_master",
		"is_replication_credentials_available",
		"has_replication_credentials",
		"is_allow_tls",
		"db_alias",
	}
	var values = make([]string, len(instance_columns), len(instance_columns))
	for i := range instance_columns {
		values[i] = "?"
	}
	values[4] = "NOW()" // last_checked_timestamp
	values[5] = "NOW()" // last_attempted_check_timestamp
	values[6] = "1"     // is_last_check_partial_success
	if updateLastSeen {
		instance_columns = append(instance_columns, "last_seen_timestamp")
		values = append(values, "NOW()")
	}
	disql, err := base.MkInsertOdku("ham_database_instance", instance_columns, values, len(instances), insertIgnore)
	if err != nil {
		return err
	}
	// for mysql_database_instance
	var mysql_instance_columns = []string{
		"di_hostname",
		"di_port",
		"server_uuid",
		"version_major",
		"version_comment",
		"is_binlog_server",
		"binlog_format",
		"binlog_row_image",
		"log_bin",
		"log_slave_updates",
		"binary_log_file",
		"binary_log_pos",
		"slave_sql_running",
		"slave_io_running",
		"replication_sql_thread_state",
		"replication_io_thread_state",
		"supports_oracle_gtid",
		"oracle_gtid",
		"master_uuid",
		"ancestry_uuid",
		"executed_gtid_set",
		"gtid_mode",
		"gtid_purged",
		"gtid_errant",
		"mariadb_gtid",
		"pseudo_gtid",
		"master_log_file",
		"read_master_log_pos",
		"relay_master_log_file",
		"exec_master_log_pos",
		"relay_log_file",
		"relay_log_pos",
		"last_sql_error",
		"last_io_error",
		"seconds_behind_master",
		"sql_delay",
		"last_discovery_latency",
		"semi_sync_enforced",
		"semi_sync_available",
		"semi_sync_master_enabled",
		"semi_sync_master_timeout",
		"semi_sync_master_wait_for_slave_count",
		"semi_sync_replica_enabled",
		"semi_sync_master_status",
		"semi_sync_master_clients",
		"semi_sync_replica_status",
		"replication_group_name",
		"replication_group_is_single_primary_mode",
		"replication_group_member_state",
		"replication_group_member_role",
		"replication_group_members",
		"replication_group_primary_host",
		"replication_group_primary_port",
	}
	var mysqlValues = make([]string, len(mysql_instance_columns), len(mysql_instance_columns))
	for i := range mysql_instance_columns {
		mysqlValues[i] = "?"
	}
	mdisql, err := base.MkInsertOdku("mysql_database_instance", mysql_instance_columns, mysqlValues, len(instances), insertIgnore)
	if err != nil {
		return err
	}

	multiSQL := &dtstruct.MultiSQL{}
	for _, ins := range instances {
		instance := ins.GetInstance()
		mysqlIns := ins.(*mdtstruct.MysqlInstance)

		var argsInstance []interface{}
		argsInstance = append(argsInstance,
			instance.Key.Hostname,
			instance.Key.Port,
			mconstant.DBTMysql,
			instance.ClusterId,
			instance.Uptime,
			mysqlIns.InstanceId,
			instance.Version,
			instance.ReadOnly,
			instance.UpstreamKey.Hostname,
			instance.UpstreamKey.Port,
			instance.HasReplicationFilters,
			instance.SlaveLagSeconds,
			len(instance.DownstreamKeyMap),
			instance.DownstreamKeyMap.ToJSONString(),
			instance.ClusterName,
			instance.SuggestedClusterAlias,
			instance.DataCenter,
			instance.Region,
			instance.Environment,
			instance.ReplicationDepth,
			instance.IsCoUpstream,
			instance.ReplicationCredentialsAvailable,
			instance.HasReplicationCredentials,
			instance.AllowTLS,
			instance.InstanceAlias,
		)

		var argsMysqlInstance []interface{}
		argsMysqlInstance = append(argsMysqlInstance,
			instance.Key.Hostname,
			instance.Key.Port,
			mysqlIns.ServerUUID,
			instance.MajorVersionString(),
			instance.VersionComment,
			mysqlIns.IsReplicaServer(),
			mysqlIns.Binlog_format,
			mysqlIns.BinlogRowImage,
			mysqlIns.LogBinEnabled,
			mysqlIns.LogReplicationUpdatesEnabled,
			mysqlIns.SelfBinlogCoordinates.LogFile,
			mysqlIns.SelfBinlogCoordinates.LogPos,
			mysqlIns.ReplicationSQLThreadRuning,
			mysqlIns.ReplicationIOThreadRuning,
			mysqlIns.ReplicationSQLThreadState,
			mysqlIns.ReplicationIOThreadState,
			mysqlIns.SupportsOracleGTID,
			mysqlIns.UsingOracleGTID,
			mysqlIns.MasterUUID,
			mysqlIns.AncestryUUID,
			mysqlIns.ExecutedGtidSet,
			mysqlIns.GTIDMode,
			mysqlIns.GtidPurged,
			mysqlIns.GtidErrant,
			mysqlIns.UsingMariaDBGTID,
			mysqlIns.UsingPseudoGTID,
			mysqlIns.ReadBinlogCoordinates.LogFile,
			mysqlIns.ReadBinlogCoordinates.LogPos,
			mysqlIns.ExecBinlogCoordinates.LogFile,
			mysqlIns.ExecBinlogCoordinates.LogPos,
			mysqlIns.RelaylogCoordinates.LogFile,
			mysqlIns.RelaylogCoordinates.LogPos,
			mysqlIns.LastSQLError,
			mysqlIns.LastIOError,
			mysqlIns.SecondsBehindMaster,
			mysqlIns.SQLDelay,
			mysqlIns.LastDiscoveryLatency.Nanoseconds(),
			mysqlIns.SemiSyncEnforced,
			mysqlIns.SemiSyncAvailable,
			mysqlIns.SemiSyncMasterEnabled,
			mysqlIns.SemiSyncMasterTimeout,
			mysqlIns.SemiSyncMasterWaitForReplicaCount,
			mysqlIns.SemiSyncReplicaEnabled,
			mysqlIns.SemiSyncMasterStatus,
			mysqlIns.SemiSyncMasterClients,
			mysqlIns.SemiSyncReplicaStatus,
			mysqlIns.ReplicationGroupName,
			mysqlIns.ReplicationGroupIsSinglePrimary,
			mysqlIns.ReplicationGroupMemberState,
			mysqlIns.ReplicationGroupMemberRole,
			mysqlIns.ReplicationGroupMembers.ToJSONString(),
			mysqlIns.ReplicationGroupPrimaryInstanceKey.Hostname,
			mysqlIns.ReplicationGroupPrimaryInstanceKey.Port,
		)

		multiSQL.Query = append(multiSQL.Query, disql)
		multiSQL.Args = append(multiSQL.Args, argsInstance)
		multiSQL.Query = append(multiSQL.Query, mdisql)
		multiSQL.Args = append(multiSQL.Args, argsMysqlInstance)
	}
	if err = db.ExecMultiSQL(multiSQL); err != nil {
		return err
	}

	return nil
}

func Less(this *mdtstruct.MysqlInstance, other *mdtstruct.MysqlInstance) bool {
	if this.ExecBinlogCoordinates.Equals(&other.ExecBinlogCoordinates) {
		// Secondary sorting: "smaller" if not logging replica updates
		if other.LogReplicationUpdatesEnabled && !this.LogReplicationUpdatesEnabled {
			return true
		}
		// Next sorting: "smaller" if of higher version (this will be reversed eventually)
		// Idea is that given 5.6 a& 5.7 both of the exact position, we will want to promote
		// the 5.6 on top of 5.7, as the other way around is invalid
		if other.Instance.IsSmallerMajorVersion(this.Instance) {
			return true
		}
		// Next sorting: "smaller" if of larger binlog-format (this will be reversed eventually)
		// Idea is that given ROW & STATEMENT both of the exact position, we will want to promote
		// the STATEMENT on top of ROW, as the other way around is invalid
		if IsSmallerBinlogFormat(other.Binlog_format, this.Binlog_format) {
			return true
		}
		// Prefer local datacenter:
		if other.DataCenter == this.Instance.DataCenter && this.DataCenter != this.Instance.DataCenter {
			return true
		}
		// Prefer if not having errant GTID
		if other.GtidErrant == "" && this.GtidErrant != "" {
			return true
		}
		// Prefer candidates:
		if other.PromotionRule.BetterThan(this.PromotionRule) {
			return true
		}
	}

	return this.ExecBinlogCoordinates.SmallerThan(&other.ExecBinlogCoordinates)
}

// ResetInstanceRelaylogCoordinatesHistory forgets about the history of an instance. This action is desirable
// when relay logs become obsolete or irrelevant. Such is the case on `CHANGE MASTER TO`: servers gets compeltely
// new relay logs.
func ResetInstanceRelaylogCoordinatesHistory(instanceKey *dtstruct.InstanceKey) error {
	writeFunc := func() error {
		_, err := db.ExecSQL(`
			update mysql_database_instance_coordinate_history
				set relay_log_file='', relay_log_pos=0
			where
				hostname=? and port=?
				`, instanceKey.Hostname, instanceKey.Port,
		)
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}

func GetEquivalentMasterCoordinates(instanceCoordinates *dtstruct.InstanceBinlogCoordinates) (result [](*dtstruct.InstanceBinlogCoordinates), err error) {
	query := `
		select 
				master1_hostname as hostname,
				master1_port as port,
				master1_binary_log_file as binlog_file,
				master1_binary_log_pos as binlog_pos
			from 
				mysql_master_position_equivalence
			where
				master2_hostname = ?
				and master2_port = ?
				and master2_binary_log_file = ?
				and master2_binary_log_pos = ?
		union
		select 
				master2_hostname as hostname,
				master2_port as port,
				master2_binary_log_file as binlog_file,
				master2_binary_log_pos as binlog_pos
			from 
				mysql_master_position_equivalence
			where
				master1_hostname = ?
				and master1_port = ?
				and master1_binary_log_file = ?
				and master1_binary_log_pos = ?
		`
	args := sqlutil.Args(
		instanceCoordinates.Key.Hostname,
		instanceCoordinates.Key.Port,
		instanceCoordinates.Coordinates.LogFile,
		instanceCoordinates.Coordinates.LogPos,
		instanceCoordinates.Key.Hostname,
		instanceCoordinates.Key.Port,
		instanceCoordinates.Coordinates.LogFile,
		instanceCoordinates.Coordinates.LogPos,
	)

	err = db.Query(query, args, func(m sqlutil.RowMap) error {
		equivalentCoordinates := dtstruct.InstanceBinlogCoordinates{}
		equivalentCoordinates.Key.Hostname = m.GetString("hostname")
		equivalentCoordinates.Key.Port = m.GetInt("port")
		equivalentCoordinates.Coordinates.LogFile = m.GetString("binlog_file")
		equivalentCoordinates.Coordinates.LogPos = m.GetInt64("binlog_pos")

		result = append(result, &equivalentCoordinates)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func GetEquivalentBinlogCoordinatesFor(instanceCoordinates *dtstruct.InstanceBinlogCoordinates, belowKey *dtstruct.InstanceKey) (*dtstruct.LogCoordinates, error) {
	possibleCoordinates, err := GetEquivalentMasterCoordinates(instanceCoordinates)
	if err != nil {
		return nil, err
	}
	for _, instanceCoordinate := range possibleCoordinates {
		if instanceCoordinate.Key.Equals(belowKey) {
			return &instanceCoordinate.Coordinates, nil
		}
	}
	return nil, nil
}

// RecordInstanceCoordinatesHistory snapshots the binlog coordinates of instances
func RecordInstanceCoordinatesHistory() error {
	{
		writeFunc := func() error {
			_, err := db.ExecSQL(`
        	delete from mysql_database_instance_coordinate_history
			where
				record_timestamp < NOW() - INTERVAL ? MINUTE
				`, (config.PseudoGTIDCoordinatesHistoryHeuristicMinutes + 2),
			)
			return log.Errore(err)
		}
		db.ExecDBWrite(writeFunc)
	}
	writeFunc := func() error {
		_, err := db.ExecSQL(`
			insert into
				mysql_database_instance_coordinate_history (
					hostname, port,	last_seen_timestamp, record_timestamp,
					binary_log_file, binary_log_pos, relay_log_file, relay_log_pos
				)
			select
				hostname, port, last_seen_timestamp, NOW(),
				binary_log_file, binary_log_pos, relay_log_file, relay_log_pos
			from
				ham_database_instance
			where
				(
					binary_log_file != ''
					or relay_log_file != ''
				)
				`,
		)
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}
