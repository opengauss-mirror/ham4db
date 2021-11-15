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
package base

import (
	"database/sql"
	"fmt"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/cache"
	orcraft "gitee.com/opengauss/ham4db/go/core/consensus/raft"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/core/system/osp"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util"
	"gitee.com/opengauss/ham4db/go/util/math"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"strings"
	"time"
)

// EndDowntime will remove downtime flag from an instance
func EndDowntime(instanceKey *dtstruct.InstanceKey) (unDowntime bool, err error) {
	var res sql.Result
	if res, err = db.ExecSQL(`delete from ham_database_instance_downtime where hostname = ? and port = ?`,
		instanceKey.Hostname, instanceKey.Port,
	); err == nil {
		if affected, _ := res.RowsAffected(); affected > 0 {
			unDowntime = true
			AuditOperation(constant.AuditDowntimeEnd, instanceKey, "", "")
		}
	}
	return
}

// ReadInstanceClusterAttributes will return the cluster name for a given instance by looking at its master
// and getting it from there.
// It is a non-recursive function and so-called-recursion is performed upon periodic reading of
// instances.
func ReadInstanceClusterAttributes(instance dtstruct.InstanceAdaptor) (err error) {
	var masterOrGroupPrimaryInstanceKey dtstruct.InstanceKey
	var masterOrGroupPrimaryClusterName string
	var masterOrGroupPrimarySuggestedClusterAlias string
	var masterOrGroupPrimaryReplicationDepth uint
	masterOrGroupPrimaryDataFound := false

	// Read the cluster_name of the _master_ or _group_primary_ of our instance, derive it from there.
	query := `
			select
				cluster_name, cluster_alias, replication_depth, upstream_host, upstream_port
			from 
				ham_database_instance
			where 
				hostname = ? and port = ?
	`
	// For instances that are part of a replication group, if the host is not the group's primary, we use the
	// information from the group primary. If it is the group primary, we use the information of its master
	// (if it has any). If it is not a group member, we use the information from the host's master.
	masterOrGroupPrimaryInstanceKey = instance.GetInstance().UpstreamKey
	args := sqlutil.Args(masterOrGroupPrimaryInstanceKey.Hostname, masterOrGroupPrimaryInstanceKey.Port)
	err = db.Query(query, args, func(m sqlutil.RowMap) error {
		masterOrGroupPrimaryClusterName = m.GetString("cluster_name")
		masterOrGroupPrimarySuggestedClusterAlias = m.GetString("cluster_alias")
		masterOrGroupPrimaryReplicationDepth = m.GetUint("replication_depth")
		masterOrGroupPrimaryInstanceKey.Hostname = m.GetString("upstream_host")
		masterOrGroupPrimaryInstanceKey.Port = m.GetInt("upstream_port")
		masterOrGroupPrimaryDataFound = true
		return nil
	})
	if err != nil {
		return log.Errore(err)
	}

	var replicationDepth uint = 0
	var clusterName string
	if masterOrGroupPrimaryDataFound {
		replicationDepth = masterOrGroupPrimaryReplicationDepth + 1
		clusterName = masterOrGroupPrimaryClusterName
	}
	clusterNameByInstanceKey := instance.GetInstance().Key.StringCode()
	if clusterName == "" {
		// Nothing from master; we set it to be named after the instance itself
		clusterName = clusterNameByInstanceKey
	}

	isCoMaster := false
	if masterOrGroupPrimaryInstanceKey.Equals(&instance.GetInstance().Key) {
		// co-master calls for special case, in fear of the infinite loop
		isCoMaster = true
		clusterNameByCoMasterKey := instance.GetInstance().UpstreamKey.StringCode()
		if clusterName != clusterNameByInstanceKey && clusterName != clusterNameByCoMasterKey {
			// Can be caused by a co-master topology failover
			log.Errorf("ReadInstanceClusterAttributes: in co-master topology %s is not in (%s, %s). Forcing it to become one of them", clusterName, clusterNameByInstanceKey, clusterNameByCoMasterKey)
			clusterName = math.TernaryString(instance.GetInstance().Key.SmallerThan(&instance.GetInstance().UpstreamKey), clusterNameByInstanceKey, clusterNameByCoMasterKey)
		}
		if clusterName == clusterNameByInstanceKey {
			// circular replication. Avoid infinite ++ on replicationDepth
			replicationDepth = 0
		} // While the other stays "1"
	}
	instance.GetInstance().ClusterName = clusterName
	instance.GetInstance().SuggestedClusterAlias = masterOrGroupPrimarySuggestedClusterAlias
	instance.GetInstance().ReplicationDepth = replicationDepth
	instance.GetInstance().IsCoUpstream = isCoMaster
	//instance.AncestryUUID = ancestryUUID
	//instance.MasterExecutedGtidSet = masterOrGroupPrimaryExecutedGtidSet
	return nil
}

// GetClusterName get cluster name for given instance
func GetClusterName(instanceKey *dtstruct.InstanceKey) (clusterName string, err error) {

	// get from cache
	var found bool
	if clusterName, found = cache.GetCluster(instanceKey.StringCode()); found {
		return clusterName, nil
	}

	// get from database
	err = db.Query(`
			select ifnull(max(cluster_name), '') as cluster_name
			from ham_database_instance
			where hostname = ? and port = ?
		`,
		sqlutil.Args(instanceKey.Hostname, instanceKey.Port),
		func(m sqlutil.RowMap) error {
			clusterName = m.GetString("cluster_name")
			cache.SetCluster(instanceKey.StringCode(), clusterName, constant.CacheExpireDefault)
			return nil
		},
	)
	return
}

/////////////////////////////////////////////TODO

// RecordInstanceCoordinatesHistory snapshots the binlog coordinates of instances
func RecordStaleInstanceBinlogCoordinates(instanceKey *dtstruct.InstanceKey, binlogCoordinates *dtstruct.LogCoordinates) error {
	args := sqlutil.Args(
		instanceKey.Hostname, instanceKey.Port,
		binlogCoordinates.LogFile, binlogCoordinates.LogPos,
	)
	_, err := db.ExecSQL(`
			delete from
				mysql_database_instance_stale_binlog_coordinate
			where
				hostname=? and port=?
				and (
					binary_log_file != ?
					or binary_log_pos != ?
				)
				`,
		args...,
	)
	if err != nil {
		return log.Errore(err)
	}
	_, err = db.ExecSQL(`
			insert ignore into
				mysql_database_instance_stale_binlog_coordinate (
					hostname, port,	binary_log_file, binary_log_pos, first_seen_timestamp
				)
				values (
					?, ?, ?, ?, NOW()
				)`,
		args...)
	return log.Errore(err)
}

func ExpireStaleInstanceBinlogCoordinates() error {
	expireSeconds := config.Config.ReasonableReplicationLagSeconds * 2
	if expireSeconds < config.StaleInstanceCoordinatesExpireSeconds {
		expireSeconds = config.StaleInstanceCoordinatesExpireSeconds
	}
	writeFunc := func() error {
		_, err := db.ExecSQL(`
					delete from mysql_database_instance_stale_binlog_coordinate
					where first_seen_timestamp < NOW() - INTERVAL ? SECOND
					`, expireSeconds,
		)
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}

// BeginDowntime will make mark an instance as downtimed (or override existing downtime period)
func BeginDowntime(downtime *dtstruct.Downtime) (err error) {
	if downtime.Duration == 0 {
		downtime.Duration = config.MaintenanceExpireMinutes * time.Minute
	}
	if downtime.EndsAtString != "" {
		_, err = db.ExecSQL(`
				insert
					into ham_database_instance_downtime (
						hostname, port, downtime_active, begin_timestamp, end_timestamp, owner, reason
					) VALUES (
						?, ?, 1, ?, ?, ?, ?
					)
					on duplicate key update
						downtime_active=values(downtime_active),
						begin_timestamp=values(begin_timestamp),
						end_timestamp=values(end_timestamp),
						owner=values(owner),
						reason=values(reason)
				`,
			downtime.Key.Hostname,
			downtime.Key.Port,
			downtime.BeginsAtString,
			downtime.EndsAtString,
			downtime.Owner,
			downtime.Reason,
		)
	} else {
		if downtime.Ended() {
			// No point in writing it down; it's expired
			return nil
		}

		_, err = db.ExecSQL(`
			insert
				into ham_database_instance_downtime (
					hostname, port, downtime_active, begin_timestamp, end_timestamp, owner, reason
				) VALUES (
					?, ?, 1, NOW(), NOW() + INTERVAL ? SECOND, ?, ?
				)
				on duplicate key update
					downtime_active=values(downtime_active),
					begin_timestamp=values(begin_timestamp),
					end_timestamp=values(end_timestamp),
					owner=values(owner),
					reason=values(reason)
			`,
			downtime.Key.Hostname,
			downtime.Key.Port,
			int(downtime.EndsIn().Seconds()),
			downtime.Owner,
			downtime.Reason,
		)
	}
	if err != nil {
		return log.Errore(err)
	}
	AuditOperation("begin-downtime", downtime.Key, "", fmt.Sprintf("owner: %s, reason: %s", downtime.Owner, downtime.Reason))

	return nil
}

// AttemptRecoveryRegistration tries to add a recovery entry; if this fails that means recovery is already in place.
func AttemptRecoveryRegistration(analysisEntry *dtstruct.ReplicationAnalysis, failIfFailedInstanceInActiveRecovery bool, failIfClusterInActiveRecovery bool) (*dtstruct.TopologyRecovery, error) {
	if failIfFailedInstanceInActiveRecovery {
		// Let's check if this instance has just been promoted recently and is still in active period.
		// If so, we reject recovery registration to avoid flapping.
		recoveries, err := ReadInActivePeriodSuccessorInstanceRecovery(&analysisEntry.AnalyzedInstanceKey)
		if err != nil {
			return nil, log.Errore(err)
		}
		if len(recoveries) > 0 {
			RegisterBlockedRecoveries(analysisEntry, recoveries)
			return nil, log.Errorf("AttemptRecoveryRegistration: instance %+v has recently been promoted (by failover of %+v) and is in active period. It will not be failed over. You may acknowledge the failure on %+v (-c ack-instance-recoveries) to remove this blockage", analysisEntry.AnalyzedInstanceKey, recoveries[0].AnalysisEntry.AnalyzedInstanceKey, recoveries[0].AnalysisEntry.AnalyzedInstanceKey)
		}
	}
	if failIfClusterInActiveRecovery {
		// Let's check if this cluster has just experienced a failover and is still in active period.
		// If so, we reject recovery registration to avoid flapping.
		recoveries, err := ReadInActivePeriodClusterRecovery(analysisEntry.ClusterDetails.ClusterName)
		if err != nil {
			return nil, log.Errore(err)
		}
		if len(recoveries) > 0 {
			RegisterBlockedRecoveries(analysisEntry, recoveries)
			return nil, log.Errorf("AttemptRecoveryRegistration: cluster %+v has recently experienced a failover (of %+v) and is in active period. It will not be failed over again. You may acknowledge the failure on this cluster (-c ack-cluster-recoveries) or on %+v (-c ack-instance-recoveries) to remove this blockage", analysisEntry.ClusterDetails.ClusterName, recoveries[0].AnalysisEntry.AnalyzedInstanceKey, recoveries[0].AnalysisEntry.AnalyzedInstanceKey)
		}
	}
	if !failIfFailedInstanceInActiveRecovery {
		// Implicitly acknowledge this instance's possibly existing active recovery, provided they are completed.
		AcknowledgeInstanceCompletedRecoveries(&analysisEntry.AnalyzedInstanceKey, "ham4db", fmt.Sprintf("implicit acknowledge due to user invocation of recovery on same instance: %+v", analysisEntry.AnalyzedInstanceKey))
		// The fact we only acknowledge a completed recovery solves the possible case of two DBAs simultaneously
		// trying to recover the same instance at the same time
	}

	topologyRecovery := dtstruct.NewTopologyRecovery(*analysisEntry)

	topologyRecovery, err := WriteTopologyRecovery(topologyRecovery)
	if err != nil {
		return nil, log.Errore(err)
	}
	if orcraft.IsRaftEnabled() {
		if _, err := orcraft.PublishCommand("write-recovery", topologyRecovery); err != nil {
			return nil, log.Errore(err)
		}
	}
	return topologyRecovery, nil
}

// ResolveRecovery is called on completion of a recovery process and updates the recovery status.
// It does not clear the "active period" as this still takes place in order to avoid flapping.
func ResolveRecovery(topologyRecovery *dtstruct.TopologyRecovery, successorInstance dtstruct.InstanceAdaptor) error {
	if !util.IsNil(successorInstance) {
		topologyRecovery.SuccessorKey = &successorInstance.GetInstance().Key
		topologyRecovery.SuccessorAlias = successorInstance.GetInstance().InstanceAlias
		topologyRecovery.IsSuccessful = true
	}
	if orcraft.IsRaftEnabled() {
		_, err := orcraft.PublishCommand("resolve-recovery", topologyRecovery)
		return err
	} else {
		return WriteResolveRecovery(topologyRecovery)
	}
}

// AuditTopologyRecovery audits a single step in a topology recovery process.
func AuditTopologyRecovery(topologyRecovery *dtstruct.TopologyRecovery, message string) error {
	log.Infof("topology_recovery: %s", message)
	if topologyRecovery == nil {
		return nil
	}

	recoveryStep := dtstruct.NewTopologyRecoveryStep(topologyRecovery.UID, message)
	if orcraft.IsRaftEnabled() {
		_, err := orcraft.PublishCommand("write-recovery-step", recoveryStep)
		return err
	} else {
		return WriteTopologyRecoveryStep(recoveryStep)
	}
}

// UpdateInstanceLastChecked updates the last_check timestamp in the ham4db backed database
// for a given instance
func UpdateInstanceLastChecked(instanceKey *dtstruct.InstanceKey, partialSuccess bool) error {
	writeFunc := func() error {
		_, err := db.ExecSQL(`
        	update
        		ham_database_instance
        	set
				last_checked_timestamp = now(),
				is_last_check_partial_success = ?
			where
				hostname = ?
				and port = ?
				and db_type =?`,
			partialSuccess,
			instanceKey.Hostname,
			instanceKey.Port,
			instanceKey.DBType,
		)
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}

// UpdateInstanceRoleAndState updates the role and state in the ham4db backed database
// for a given instance
func UpdateInstanceRoleAndState(instanceKey *dtstruct.InstanceKey, role, state string) error {
	writeFunc := func() error {
		_, err := db.ExecSQL(`
        	update
        		ham_database_instance
        	set
				db_role = ?,
				db_state = ?
			where
				hostname = ?
				and port = ?
				and db_type = ?`,
			role,
			state,
			instanceKey.Hostname,
			instanceKey.Port,
			instanceKey.DBType,
		)
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}

// UpdateInstanceLastAttemptedCheck updates the last_attempted_check timestamp in the ham4db backed database
// for a given instance.
// This is used as a failsafe mechanism in case access to the instance gets hung (it happens), in which case
// the entire ReadTopology gets stuck (and no, connection timeout nor driver timeouts don't help. Don't look at me,
// the world is a harsh place to live in).
// And so we make sure to note down *before* we even attempt to access the instance; and this raises a red flag when we
// wish to access the instance again: if last_attempted_check_timestamp is *newer* than last_checked_timestamp, that's bad news and means
// we have a "hanging" issue.
func UpdateInstanceLastAttemptedCheck(instanceKey *dtstruct.InstanceKey) error {
	writeFunc := func() error {
		_, err := db.ExecSQL(`
    	update
    		ham_database_instance
    	set
    		last_attempted_check_timestamp = current_timestamp
		where
			hostname = ?
			and port = ?
			and db_type =?`,
			instanceKey.Hostname,
			instanceKey.Port,
			instanceKey.DBType,
		)
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}
func ReadInstancePromotionRule(instance dtstruct.InstanceAdaptor) (err error) {
	var promotionRule dtstruct.CandidatePromotionRule = dtstruct.NeutralPromoteRule
	query := `
			select
				ifnull(nullif(promotion_rule, ''), 'neutral') as promotion_rule
				from ham_database_instance_candidate
				where hostname=? and port=?
	`
	args := sqlutil.Args(instance.GetHostname(), instance.GetPort())

	err = db.Query(query, args, func(m sqlutil.RowMap) error {
		promotionRule = dtstruct.CandidatePromotionRule(m.GetString("promotion_rule"))
		return nil
	})
	instance.SetPromotionRule(promotionRule)
	return log.Errore(err)
}

// ReadClusterAliasOverride reads and applies SuggestedClusterAlias based on ham_cluster_alias_override
func ReadClusterAliasOverride(instance *dtstruct.Instance) (err error) {
	aliasOverride := ""
	query := `
		select
			alias
		from
			ham_cluster_alias_override
		where
			cluster_name = ?
	`
	err = db.Query(query, sqlutil.Args(instance.ClusterName), func(m sqlutil.RowMap) error {
		aliasOverride = m.GetString("alias")
		return nil
	})
	if aliasOverride != "" {
		instance.SuggestedClusterAlias = aliasOverride
	}
	return err
}

// readRecoveries reads recovery entry/audit entries from topology_recovery
func readRecoveries(whereCondition string, limit string, args []interface{}) ([]*dtstruct.TopologyRecovery, error) {
	var res []*dtstruct.TopologyRecovery
	query := fmt.Sprintf(`
		select
      recovery_id,
			recovery_uid,
      db_type,
      hostname,
      port,
      (IFNULL(end_active_period_unix_time, 0) = 0) as is_active,
      start_active_period_timestamp,
      IFNULL(end_active_period_unix_time, 0) as end_active_period_unix_time,
      IFNULL(end_recovery_timestamp, '') AS end_recovery_timestamp,
      is_successful,
      processing_node_hostname,
      processing_node_token,
      ifnull(successor_hostname, '') as successor_hostname,
      ifnull(successor_port, 0) as successor_port,
      ifnull(successor_alias, '') as successor_alias,
      analysis,
      cluster_name,
      cluster_alias,
      count_affected_downstream,
      downstream_hosts,
      participating_instances,
      lost_downstream,
      all_errors,
      is_acked,
      acked_timestamp,
      acked_by,
      acked_comment,
      last_detection_id
		from
			ham_topology_recovery
		%s
		order by
			recovery_id desc
		%s
		`, whereCondition, limit)
	err := db.Query(query, args, func(m sqlutil.RowMap) error {
		topologyRecovery := *dtstruct.NewTopologyRecovery(dtstruct.ReplicationAnalysis{})
		topologyRecovery.Id = m.GetInt64("recovery_id")
		topologyRecovery.UID = m.GetString("recovery_uid")

		topologyRecovery.IsActive = m.GetBool("is_active")
		topologyRecovery.RecoveryStartTimestamp = m.GetString("start_active_period_timestamp")
		topologyRecovery.RecoveryEndTimestamp = m.GetString("end_recovery_timestamp")
		topologyRecovery.IsSuccessful = m.GetBool("is_successful")
		topologyRecovery.ProcessingNodeHostname = m.GetString("processing_node_hostname")
		topologyRecovery.ProcessingNodeToken = m.GetString("processing_node_token")

		topologyRecovery.AnalysisEntry.AnalyzedInstanceKey.Hostname = m.GetString("hostname")
		topologyRecovery.AnalysisEntry.AnalyzedInstanceKey.Port = m.GetInt("port")
		topologyRecovery.AnalysisEntry.Analysis = dtstruct.AnalysisCode(m.GetString("analysis"))
		topologyRecovery.AnalysisEntry.ClusterDetails.ClusterName = m.GetString("cluster_name")
		topologyRecovery.AnalysisEntry.ClusterDetails.ClusterAlias = m.GetString("cluster_alias")
		topologyRecovery.AnalysisEntry.CountReplicas = m.GetUint("count_affected_downstream")
		topologyRecovery.AnalysisEntry.Downstreams = *dtstruct.NewInstanceKeyMap()
		ReadCommaDelimitedList(&topologyRecovery.AnalysisEntry.Downstreams, m.GetString("db_type"), m.GetString("downstream_hosts"))

		topologyRecovery.SuccessorKey = &dtstruct.InstanceKey{}
		topologyRecovery.SuccessorKey.Hostname = m.GetString("successor_hostname")
		topologyRecovery.SuccessorKey.Port = m.GetInt("successor_port")
		topologyRecovery.SuccessorAlias = m.GetString("successor_alias")

		topologyRecovery.AnalysisEntry.ClusterDetails.ReadRecoveryInfo()

		topologyRecovery.AllErrors = strings.Split(m.GetString("all_errors"), "\n")
		ReadCommaDelimitedList(&topologyRecovery.LostReplicas, m.GetString("db_type"), m.GetString("lost_downstream"))
		ReadCommaDelimitedList(&topologyRecovery.ParticipatingInstanceKeys, m.GetString("db_type"), m.GetString("participating_instances"))

		topologyRecovery.Acknowledged = m.GetBool("is_acked")
		topologyRecovery.AcknowledgedAt = m.GetString("acked_timestamp")
		topologyRecovery.AcknowledgedBy = m.GetString("acked_by")
		topologyRecovery.AcknowledgedComment = m.GetString("acked_comment")

		topologyRecovery.LastDetectionId = m.GetInt64("last_detection_id")

		res = append(res, &topologyRecovery)
		return nil
	})

	return res, log.Errore(err)
}

// ReadActiveRecoveries reads active recovery entry/audit entries from topology_recovery
func ReadActiveClusterRecovery(clusterName string) ([]*dtstruct.TopologyRecovery, error) {
	whereClause := `
		where
			in_active_period=1
			and end_recovery_timestamp is null
			and cluster_name=?`
	return readRecoveries(whereClause, ``, sqlutil.Args(clusterName))
}

// ReadInActivePeriodClusterRecovery reads recoveries (possibly complete!) that are in active period.
// (may be used to block further recoveries on this cluster)
func ReadInActivePeriodClusterRecovery(clusterName string) ([]*dtstruct.TopologyRecovery, error) {
	whereClause := `
		where
			in_active_period=1
			and cluster_name=?`
	return readRecoveries(whereClause, ``, sqlutil.Args(clusterName))
}

// ReadRecentlyActiveClusterRecovery reads recently completed entries for a given cluster
func ReadRecentlyActiveClusterRecovery(dbt string, clusterName string) ([]*dtstruct.TopologyRecovery, error) {
	whereClause := `
		where
			db_type = ?
			and end_recovery_timestamp > now() - interval 5 minute
			and cluster_name=?`
	return readRecoveries(whereClause, ``, sqlutil.Args(dbt, clusterName))
}

// ReadInActivePeriodSuccessorInstanceRecovery reads completed recoveries for a given instance, where said instance
// was promoted as result, still in active period (may be used to block further recoveries should this instance die)
func ReadInActivePeriodSuccessorInstanceRecovery(instanceKey *dtstruct.InstanceKey) ([]*dtstruct.TopologyRecovery, error) {
	whereClause := `
		where
			in_active_period=1
			and
				successor_hostname=? and successor_port=?`
	return readRecoveries(whereClause, ``, sqlutil.Args(instanceKey.Hostname, instanceKey.Port))
}

// ReadRecentlyActiveInstanceRecovery reads recently completed entries for a given instance
func ReadRecentlyActiveInstanceRecovery(instanceKey *dtstruct.InstanceKey) ([]*dtstruct.TopologyRecovery, error) {
	whereClause := `
		where
			end_recovery_timestamp > now() - interval 5 minute
			and
				successor_hostname=? and successor_port=?`
	return readRecoveries(whereClause, ``, sqlutil.Args(instanceKey.Hostname, instanceKey.Port))
}

// ReadActiveRecoveries reads active recovery entry/audit entries from topology_recovery
func ReadActiveRecoveries() ([]*dtstruct.TopologyRecovery, error) {
	return readRecoveries(`
		where
			in_active_period=1
			and end_recovery_timestamp is null`,
		``, sqlutil.Args())
}

// ReadCompletedRecoveries reads completed recovery entry/audit entries from topology_recovery
func ReadCompletedRecoveries(page int) ([]*dtstruct.TopologyRecovery, error) {
	limit := `
		limit ?
		offset ?`
	return readRecoveries(`where end_recovery_timestamp is not null`, limit, sqlutil.Args(config.AuditPageSize, page*config.AuditPageSize))
}

// ReadRecovery reads completed recovery entry/audit entries from topology_recovery
func ReadRecovery(recoveryId int64) ([]*dtstruct.TopologyRecovery, error) {
	whereClause := `where recovery_id = ?`
	return readRecoveries(whereClause, ``, sqlutil.Args(recoveryId))
}

// ReadRecoveryByUID reads completed recovery entry/audit entries from topology_recovery
func ReadRecoveryByUID(recoveryUID string) ([]*dtstruct.TopologyRecovery, error) {
	whereClause := `where recovery_uid = ?`
	return readRecoveries(whereClause, ``, sqlutil.Args(recoveryUID))
}

// ReadCRecoveries reads latest recovery entries from topology_recovery
func ReadRecentRecoveries(dbType string, clusterName string, clusterAlias string, unacknowledgedOnly bool, page int) ([]*dtstruct.TopologyRecovery, error) {
	whereConditions := []string{}
	whereClause := ""
	args := sqlutil.Args()

	if dbType != "" {
		whereConditions = append(whereConditions, "db_type = ?")
		args = append(args, dbType)
	}
	if unacknowledgedOnly {
		whereConditions = append(whereConditions, `is_acked=0`)
	}
	if clusterName != "" {
		whereConditions = append(whereConditions, `cluster_name=?`)
		args = append(args, clusterName)
	} else if clusterAlias != "" {
		whereConditions = append(whereConditions, `cluster_alias=?`)
		args = append(args, clusterAlias)
	}
	if len(whereConditions) > 0 {
		whereClause = fmt.Sprintf("where %s", strings.Join(whereConditions, " and "))
	}
	limit := `
		limit ?
		offset ?`
	args = append(args, config.AuditPageSize, page*config.AuditPageSize)
	return readRecoveries(whereClause, limit, args)
}

// readRecoveries reads recovery entry/audit entries from topology_recovery
func readFailureDetections(whereCondition string, limit string, args []interface{}) ([]*dtstruct.TopologyRecovery, error) {
	res := []*dtstruct.TopologyRecovery{}
	query := fmt.Sprintf(`
		select
		  detection_id,
		  db_type,
		  hostname,
		  port,
		  in_active_period as is_active,
		  start_active_period_timestamp,
		  end_active_period_unix_time,
		  processing_node_hostname,
		  processing_node_token,
		  analysis,
		  cluster_name,
		  cluster_alias,
		  count_affected_downstream,
		  downstream_hosts,
		  (select max(recovery_id) from ham_topology_recovery where ham_topology_recovery.last_detection_id = detection_id) as related_recovery_id
		from
			ham_topology_failure_detection
		%s
		order by
			detection_id desc
		%s
		`, whereCondition, limit)
	err := db.Query(query, args, func(m sqlutil.RowMap) error {
		failureDetection := dtstruct.TopologyRecovery{}
		failureDetection.Id = m.GetInt64("detection_id")

		failureDetection.IsActive = m.GetBool("is_active")
		failureDetection.RecoveryStartTimestamp = m.GetString("start_active_period_timestamp")
		failureDetection.ProcessingNodeHostname = m.GetString("processing_node_hostname")
		failureDetection.ProcessingNodeToken = m.GetString("processing_node_token")

		failureDetection.AnalysisEntry.AnalyzedInstanceKey.Hostname = m.GetString("hostname")
		failureDetection.AnalysisEntry.AnalyzedInstanceKey.Port = m.GetInt("port")
		failureDetection.AnalysisEntry.Analysis = dtstruct.AnalysisCode(m.GetString("analysis"))
		failureDetection.AnalysisEntry.ClusterDetails.ClusterName = m.GetString("cluster_name")
		failureDetection.AnalysisEntry.ClusterDetails.ClusterAlias = m.GetString("cluster_alias")
		failureDetection.AnalysisEntry.CountReplicas = m.GetUint("count_affected_downstream")
		failureDetection.AnalysisEntry.Downstreams = *dtstruct.NewInstanceKeyMap()
		ReadCommaDelimitedList(&failureDetection.AnalysisEntry.Downstreams, m.GetString("db_type"), m.GetString("downstream_hosts"))
		failureDetection.AnalysisEntry.StartActivePeriod = m.GetString("start_active_period_timestamp")

		failureDetection.RelatedRecoveryId = m.GetInt64("related_recovery_id")

		failureDetection.AnalysisEntry.ClusterDetails.ReadRecoveryInfo()

		res = append(res, &failureDetection)
		return nil
	})

	return res, log.Errore(err)
}

// ReadRecentFailureDetections
func ReadRecentFailureDetections(dbType string, clusterAlias string, page int) ([]*dtstruct.TopologyRecovery, error) {
	whereClause := "where 1=1 "
	args := sqlutil.Args()

	// check database type
	if dbType != "" {
		whereClause = whereClause + "and db_type = ? "
		args = append(args, dbType)
	}

	// check cluster alias
	if clusterAlias != "" {
		whereClause = whereClause + "and cluster_alias = ? "
		args = append(args, clusterAlias)
	}
	limit := `
		limit ?
		offset ?`
	args = append(args, config.AuditPageSize, page*config.AuditPageSize)
	return readFailureDetections(whereClause, limit, args)
}

// ReadFailureDetection
func ReadFailureDetection(detectionId int64) ([]*dtstruct.TopologyRecovery, error) {
	whereClause := `where detection_id = ?`
	return readFailureDetections(whereClause, ``, sqlutil.Args(detectionId))
}

// ReadBlockedRecoveries reads blocked recovery entries, potentially filtered by cluster name (empty to unfilter)
func ReadBlockedRecoveries(clusterName string) ([]dtstruct.BlockedTopologyRecovery, error) {
	res := []dtstruct.BlockedTopologyRecovery{}
	whereClause := ""
	args := sqlutil.Args()
	if clusterName != "" {
		whereClause = `where cluster_name = ?`
		args = append(args, clusterName)
	}
	query := fmt.Sprintf(`
		select
				hostname,
				port,
				cluster_name,
				analysis,
				last_blocked_timestamp,
				blocking_recovery_id
			from
				ham_topology_recovery_blocked
			%s
			order by
				last_blocked_timestamp desc
		`, whereClause)
	err := db.Query(query, args, func(m sqlutil.RowMap) error {
		blockedTopologyRecovery := dtstruct.BlockedTopologyRecovery{}
		blockedTopologyRecovery.FailedInstanceKey.Hostname = m.GetString("hostname")
		blockedTopologyRecovery.FailedInstanceKey.Port = m.GetInt("port")
		blockedTopologyRecovery.ClusterName = m.GetString("cluster_name")
		blockedTopologyRecovery.Analysis = dtstruct.AnalysisCode(m.GetString("analysis"))
		blockedTopologyRecovery.LastBlockedTimestamp = m.GetString("last_blocked_timestamp")
		blockedTopologyRecovery.BlockingRecoveryId = m.GetInt64("blocking_recovery_id")

		res = append(res, blockedTopologyRecovery)
		return nil
	})

	return res, log.Errore(err)
}

// WriteTopologyRecoveryStep writes down a single step in a recovery process
func WriteTopologyRecoveryStep(topologyRecoveryStep *dtstruct.TopologyRecoveryStep) error {
	sqlResult, err := db.ExecSQL(`
			insert ignore
				into ham_topology_recovery_step (
					step_id, recovery_uid, audit_timestamp, message
				) values (?, ?, now(), ?)
			`, sqlutil.NilIfZero(topologyRecoveryStep.Id), topologyRecoveryStep.RecoveryUID, topologyRecoveryStep.Message,
	)
	if err != nil {
		return log.Errore(err)
	}
	topologyRecoveryStep.Id, err = sqlResult.LastInsertId()
	return log.Errore(err)
}

// ReadTopologyRecoverySteps reads recovery steps for a given recovery
func ReadTopologyRecoverySteps(recoveryUID string) ([]dtstruct.TopologyRecoveryStep, error) {
	res := []dtstruct.TopologyRecoveryStep{}
	query := `
		select
			step_id, recovery_uid, audit_timestamp, message
		from
			ham_topology_recovery_step
		where
			recovery_uid=?
		order by
			step_id asc
		`
	err := db.Query(query, sqlutil.Args(recoveryUID), func(m sqlutil.RowMap) error {
		recoveryStep := dtstruct.TopologyRecoveryStep{}
		recoveryStep.RecoveryUID = recoveryUID
		recoveryStep.Id = m.GetInt64("step_id")
		recoveryStep.AuditAt = m.GetString("audit_timestamp")
		recoveryStep.Message = m.GetString("message")

		res = append(res, recoveryStep)
		return nil
	})
	return res, log.Errore(err)
}

func ExpireTableData(tableName string, timestampColumn string) error {
	query := fmt.Sprintf("delete from %s where %s < NOW() - INTERVAL ? DAY", tableName, timestampColumn)
	writeFunc := func() error {
		_, err := db.ExecSQL(query, config.Config.AuditPurgeDays)
		return err
	}
	return db.ExecDBWrite(writeFunc)
}

// ExpireFailureDetectionHistory removes old rows from the ham_topology_failure_detection table
func ExpireFailureDetectionHistory() error {
	return ExpireTableData("ham_topology_failure_detection", "start_active_period_timestamp")
}

// ExpireTopologyRecoveryHistory removes old rows from the ham_topology_failure_detection table
func ExpireTopologyRecoveryHistory() error {
	return ExpireTableData("ham_topology_recovery", "start_active_period_timestamp")
}

// ExpireTopologyRecoveryStepsHistory removes old rows from the ham_topology_failure_detection table
func ExpireTopologyRecoveryStepsHistory() error {
	return ExpireTableData("ham_topology_recovery_step", "audit_timestamp")
}

// AcknowledgeInstanceRecoveries marks active recoveries for given instane as acknowledged.
// This also implied clearing their active period, which in turn enables further recoveries on those topologies
func AcknowledgeInstanceRecoveries(instanceKey *dtstruct.InstanceKey, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	whereClause := `
			hostname = ?
			and port = ?
		`
	args := sqlutil.Args(instanceKey.Hostname, instanceKey.Port)
	ClearAcknowledgedFailureDetections(whereClause, args)
	return AcknowledgeRecoveries(owner, comment, false, whereClause, args)
}

// AcknowledgeInstanceCompletedRecoveries marks active and COMPLETED recoveries for given instane as acknowledged.
// This also implied clearing their active period, which in turn enables further recoveries on those topologies
func AcknowledgeInstanceCompletedRecoveries(instanceKey *dtstruct.InstanceKey, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	whereClause := `
			hostname = ?
			and port = ?
			and end_recovery_timestamp is not null
		`
	return AcknowledgeRecoveries(owner, comment, false, whereClause, sqlutil.Args(instanceKey.Hostname, instanceKey.Port))
}

// AcknowledgeCrashedRecoveries marks recoveries whose processing nodes has crashed as acknowledged.
func AcknowledgeCrashedRecoveries() (countAcknowledgedEntries int64, err error) {
	whereClause := `
			in_active_period = 1
			and end_recovery_timestamp is null
			and concat(processing_node_hostname, ':', processing_node_token) not in (
				select concat(hostname, ':', token) from ham_node_health
			)
		`
	return AcknowledgeRecoveries("ham4db", "detected crashed recovery", true, whereClause, sqlutil.Args())
}

// ClearActiveRecoveries clears the "in_active_period" flag for old-enough recoveries, thereby allowing for
// further recoveries on cleared instances.
func ClearActiveRecoveries() error {
	_, err := db.ExecSQL(`
			update ham_topology_recovery set
				in_active_period = 0,
				end_active_period_unix_time = UNIX_TIMESTAMP()
			where
				in_active_period = 1
				AND start_active_period_timestamp < NOW() - INTERVAL ? SECOND
			`,
		config.Config.RecoveryPeriodBlockSeconds,
	)
	return log.Errore(err)
}

// RegisterBlockedRecoveries writes down currently blocked recoveries, and indicates what recovery they are blocked on.
// Recoveries are blocked thru the in_active_period flag, which comes to avoid flapping.
func RegisterBlockedRecoveries(analysisEntry *dtstruct.ReplicationAnalysis, blockingRecoveries []*dtstruct.TopologyRecovery) error {
	for _, recovery := range blockingRecoveries {
		_, err := db.ExecSQL(`
			insert
				into ham_topology_recovery_blocked (
					hostname,
					port,
					cluster_name,
					analysis,
					last_blocked_timestamp,
					blocking_recovery_id
				) values (
					?,
					?,
					?,
					?,
					NOW(),
					?
				)
				on duplicate key update
					cluster_name=values(cluster_name),
					analysis=values(analysis),
					last_blocked_timestamp=values(last_blocked_timestamp),
					blocking_recovery_id=values(blocking_recovery_id)
			`, analysisEntry.AnalyzedInstanceKey.Hostname,
			analysisEntry.AnalyzedInstanceKey.Port,
			analysisEntry.ClusterDetails.ClusterName,
			string(analysisEntry.Analysis),
			recovery.Id,
		)
		if err != nil {
			log.Errore(err)
		}
	}
	return nil
}

// ExpireBlockedRecoveries clears listing of blocked recoveries that are no longer actually blocked.
func ExpireBlockedRecoveries() error {
	// Older recovery is acknowledged by now, hence blocked recovery should be released.
	// Do NOTE that the data in blocked_topology_recovery is only used for auditing: it is NOT the data
	// based on which we make automated decisions.

	query := `
		select
				ham_topology_recovery_blocked.hostname,
				ham_topology_recovery_blocked.port
			from
				ham_topology_recovery_blocked
				left join ham_topology_recovery on (blocking_recovery_id = ham_topology_recovery.recovery_id and is_acked = 0)
			where
				is_acked is null
		`
	expiredKeys := dtstruct.NewInstanceKeyMap()
	err := db.Query(query, sqlutil.Args(), func(m sqlutil.RowMap) error {
		key := dtstruct.InstanceKey{Hostname: m.GetString("hostname"), Port: m.GetInt("port")}
		expiredKeys.AddKey(key)
		return nil
	})

	for _, expiredKey := range expiredKeys.GetInstanceKeys() {
		_, err := db.ExecSQL(`
				delete
					from ham_topology_recovery_blocked
				where
						hostname = ?
						and port = ?
				`,
			expiredKey.Hostname, expiredKey.Port,
		)
		if err != nil {
			return log.Errore(err)
		}
	}

	if err != nil {
		return log.Errore(err)
	}
	// Some oversampling, if a problem has not been noticed for some time (e.g. the server came up alive
	// before action was taken), expire it.
	// Recall that RegisterBlockedRecoveries continuously updates the last_blocked_timestamp column.
	_, err = db.ExecSQL(`
			delete
				from ham_topology_recovery_blocked
				where
					last_blocked_timestamp < NOW() - interval ? second
			`, (config.RecoveryPollSeconds * 2),
	)
	return log.Errore(err)
}

// AcknowledgeRecoveries sets acknowledged* details and clears the in_active_period flags from a set of entries
func AcknowledgeRecoveries(owner string, comment string, markEndRecovery bool, whereClause string, args []interface{}) (countAcknowledgedEntries int64, err error) {
	additionalSet := ``
	if markEndRecovery {
		additionalSet = `
				end_recovery_timestamp=IFNULL(end_recovery_timestamp, NOW()),
			`
	}
	query := fmt.Sprintf(`
			update ham_topology_recovery set
				in_active_period = 0,
				end_active_period_unix_time = case when end_active_period_unix_time = 0 then UNIX_TIMESTAMP() else end_active_period_unix_time end,
				%s
				is_acked = 1,
				acked_timestamp = NOW(),
				acked_by = ?,
				acked_comment = ?
			where
				is_acked = 0
				and
				%s
		`, additionalSet, whereClause)
	args = append(sqlutil.Args(owner, comment), args...)
	sqlResult, err := db.ExecSQL(query, args...)
	if err != nil {
		return 0, log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	return rows, log.Errore(err)
}

// AcknowledgeAllRecoveries acknowledges all unacknowledged recoveries.
func AcknowledgeAllRecoveries(owner string, comment string) (countAcknowledgedEntries int64, err error) {
	whereClause := `1 = 1`
	return AcknowledgeRecoveries(owner, comment, false, whereClause, sqlutil.Args())
}

// AcknowledgeRecovery acknowledges a particular recovery.
// This also implied clearing their active period, which in turn enables further recoveries on those topologies
func AcknowledgeRecovery(recoveryId int64, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	whereClause := `recovery_id = ?`
	return AcknowledgeRecoveries(owner, comment, false, whereClause, sqlutil.Args(recoveryId))
}

// AcknowledgeRecovery acknowledges a particular recovery.
// This also implied clearing their active period, which in turn enables further recoveries on those topologies
func AcknowledgeRecoveryByUID(recoveryUID string, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	whereClause := `recovery_uid = ?`
	return AcknowledgeRecoveries(owner, comment, false, whereClause, sqlutil.Args(recoveryUID))
}

// ClearActiveFailureDetections clears the "in_active_period" flag for old-enough detections, thereby allowing for
// further detections on cleared instances.
func ClearActiveFailureDetections() error {
	_, err := db.ExecSQL(`
			update ham_topology_failure_detection set
				in_active_period = 0,
				end_active_period_unix_time = UNIX_TIMESTAMP()
			where
				in_active_period = 1
				AND start_active_period_timestamp < NOW() - INTERVAL ? MINUTE
			`,
		config.Config.FailureDetectionPeriodBlockMinutes,
	)
	return log.Errore(err)
}
func WriteResolveRecovery(topologyRecovery *dtstruct.TopologyRecovery) error {
	var successorKeyToWrite dtstruct.InstanceKey
	if topologyRecovery.IsSuccessful {
		successorKeyToWrite = *topologyRecovery.SuccessorKey
	}
	_, err := db.ExecSQL(`
			update ham_topology_recovery set
				is_successful = ?,
				successor_hostname = ?,
				successor_port = ?,
				successor_alias = ?,
				lost_downstream = ?,
				participating_instances = ?,
				all_errors = ?,
				end_recovery_timestamp = NOW()
			where
				recovery_uid = ?
			`, topologyRecovery.IsSuccessful, successorKeyToWrite.Hostname, successorKeyToWrite.Port,
		topologyRecovery.SuccessorAlias, topologyRecovery.LostReplicas.ToCommaDelimitedList(),
		topologyRecovery.ParticipatingInstanceKeys.ToCommaDelimitedList(),
		strings.Join(topologyRecovery.AllErrors, "\n"),
		topologyRecovery.UID,
	)
	return log.Errore(err)
}

// AcknowledgeInstanceFailureDetection clears a failure detection for a particular
// instance. This is automated by recovery process: it makes sense to acknowledge
// the detection of an instance just recovered.
func AcknowledgeInstanceFailureDetection(instanceKey *dtstruct.InstanceKey) error {
	whereClause := `
			hostname = ?
			and port = ?
		`
	args := sqlutil.Args(instanceKey.Hostname, instanceKey.Port)
	return ClearAcknowledgedFailureDetections(whereClause, args)
}

// AttemptFailureDetectionRegistration tries to add a failure-detection entry; if this fails that means the problem has already been detected
func AttemptFailureDetectionRegistration(analysisEntry *dtstruct.ReplicationAnalysis) (registrationSuccessful bool, err error) {
	args := sqlutil.Args(
		analysisEntry.AnalyzedInstanceKey.Hostname,
		analysisEntry.AnalyzedInstanceKey.Port,
		analysisEntry.AnalyzedInstanceKey.DBType,
		osp.GetHostname(),
		dtstruct.ProcessToken.Hash,
		string(analysisEntry.Analysis),
		analysisEntry.ClusterDetails.ClusterName,
		analysisEntry.ClusterDetails.ClusterAlias,
		analysisEntry.CountReplicas,
		analysisEntry.Downstreams.ToCommaDelimitedList(),
		analysisEntry.IsActionableRecovery,
	)
	startActivePeriodHint := "now()"
	if analysisEntry.StartActivePeriod != "" {
		startActivePeriodHint = "?"
		args = append(args, analysisEntry.StartActivePeriod)
	}

	query := fmt.Sprintf(`
			insert ignore
				into ham_topology_failure_detection (
					hostname,
					port,
					db_type,
					in_active_period,
					end_active_period_unix_time,
					processing_node_hostname,
					processing_node_token,
					analysis,
					cluster_name,
					cluster_alias,
					count_affected_downstream,
					downstream_hosts,
					is_actionable,
					start_active_period_timestamp
				) values (
					?,
					?,
					?,
					1,
					0,
					?,
					?,
					?,
					?,
					?,
					?,
					?,
					?,
					%s
				)
			`, startActivePeriodHint)

	sqlResult, err := db.ExecSQL(query, args...)
	if err != nil {
		return false, log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return false, log.Errore(err)
	}
	return (rows > 0), nil
}

// ReplaceAliasClusterName replaces alis mapping of one cluster name onto a new cluster name.
// Used in topology failover/recovery
func ReplaceAliasClusterName(oldClusterName string, newClusterName string) (err error) {
	{
		writeFunc := func() error {
			_, err := db.ExecSQL(`
			update ham_cluster_alias
				set cluster_name = ?
				where cluster_name = ?
			`,
				newClusterName, oldClusterName)
			return log.Errore(err)
		}
		err = db.ExecDBWrite(writeFunc)
	}
	{
		writeFunc := func() error {
			_, err := db.ExecSQL(`
			update ham_cluster_alias_override
				set cluster_name = ?
				where cluster_name = ?
			`,
				newClusterName, oldClusterName)
			return log.Errore(err)
		}
		if ferr := db.ExecDBWrite(writeFunc); ferr != nil {
			err = ferr
		}
	}
	return err
}

// ClearAcknowledgedFailureDetections clears the "in_active_period" flag for detections
// that were acknowledged
func ClearAcknowledgedFailureDetections(whereClause string, args []interface{}) error {
	query := fmt.Sprintf(`
			update ham_topology_failure_detection set
				in_active_period = 0,
				end_active_period_unix_time = UNIX_TIMESTAMP()
			where
				in_active_period = 1
				and %s
			`, whereClause)
	_, err := db.ExecSQL(query, args...)
	return log.Errore(err)
}

func WriteTopologyRecovery(topologyRecovery *dtstruct.TopologyRecovery) (*dtstruct.TopologyRecovery, error) {
	analysisEntry := topologyRecovery.AnalysisEntry
	sqlResult, err := db.ExecSQL(`
			insert ignore
				into ham_topology_recovery (
					db_type,
					recovery_id,
					recovery_uid,
					hostname,
					port,
					in_active_period,
					start_active_period_timestamp,
					end_active_period_unix_time,
					processing_node_hostname,
					processing_node_token,
					analysis,
					cluster_name,
					cluster_alias,
					count_affected_downstream,
					downstream_hosts,
					last_detection_id
				) values (
					?,
					?,
					?,
					?,
					?,
					1,
					NOW(),
					0,
					?,
					?,
					?,
					?,
					?,
					?,
					?,
					(select ifnull(max(detection_id), 0) from ham_topology_failure_detection where hostname=? and port=?)
				)
			`,
		analysisEntry.AnalyzedInstanceKey.DBType,
		sqlutil.NilIfZero(topologyRecovery.Id),
		topologyRecovery.UID,
		analysisEntry.AnalyzedInstanceKey.Hostname, analysisEntry.AnalyzedInstanceKey.Port,
		osp.GetHostname(), dtstruct.ProcessToken.Hash,
		string(analysisEntry.Analysis),
		analysisEntry.ClusterDetails.ClusterName,
		analysisEntry.ClusterDetails.ClusterAlias,
		analysisEntry.CountReplicas, analysisEntry.Downstreams.ToCommaDelimitedList(),
		analysisEntry.AnalyzedInstanceKey.Hostname, analysisEntry.AnalyzedInstanceKey.Port,
	)
	if err != nil {
		return nil, err
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rows == 0 {
		return nil, nil
	}
	lastInsertId, err := sqlResult.LastInsertId()
	if err != nil {
		return nil, err
	}
	topologyRecovery.Id = lastInsertId
	return topologyRecovery, nil
}
