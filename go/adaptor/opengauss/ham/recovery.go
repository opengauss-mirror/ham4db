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
	"errors"
	"fmt"
	oconstant "gitee.com/opengauss/ham4db/go/adaptor/opengauss/common/constant"
	aaodtstruct "gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/dtstruct"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/agent"
	"gitee.com/opengauss/ham4db/go/core/cache"
	"gitee.com/opengauss/ham4db/go/core/system/osp"

	odtstruct "gitee.com/opengauss/ham4db/go/adaptor/opengauss/dtstruct"
	outil "gitee.com/opengauss/ham4db/go/adaptor/opengauss/util"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"google.golang.org/grpc"
	"regexp"
	"sort"
	"strconv"
)

// GetReplicationAnalysis will check for replication problems (dead upstream; unreachable upstream; etc)
// clusterId='x' means analysis all clusters
func GetReplicationAnalysis(clusterName, clusterId string, hints *dtstruct.ReplicationAnalysisHints) ([]dtstruct.ReplicationAnalysis, error) {
	var result []dtstruct.ReplicationAnalysis
	analysisQueryClusterClause := ""
	if clusterId != "" {
		analysisQueryClusterClause = fmt.Sprintf("and master_instance.cluster_id = '%s'", clusterId)
	}
	analysisQueryReductionClause := ``
	if config.Config.ReduceReplicationAnalysisCount {
		analysisQueryReductionClause = `
			HAVING
				(
					MIN(
						master_instance.last_checked_timestamp <= master_instance.last_seen_timestamp and master_instance.last_attempted_check_timestamp <= master_instance.last_seen_timestamp + interval ? second
					) = 1
				) = 0
			OR(
				IFNULL(
					SUM(
						replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp
					),
					0
				) > 0
			)
			OR(
				IFNULL(
					SUM(
						replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp
					),
					0
				) < COUNT(replica_instance.port)
			)
			OR(
				IFNULL(
					SUM(
						replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp
					),
					0
				) < COUNT(replica_instance.port)
			)
			OR(
				COUNT(replica_instance.port) > 0
			)
			OR(
				is_master
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
		master_instance.db_role,
		master_instance.db_state,
		MIN(master_instance.is_read_only) AS read_only,
		MIN(master_instance.pl_data_center) AS pl_data_center,
		MIN(master_instance.pl_region) AS pl_region,
		MIN(master_instance.upstream_host) AS upstream_host,
		MIN(master_instance.upstream_port) AS upstream_port,
		MIN(master_instance.cluster_name) AS cluster_name,
		MIN(IFNULL(ham_cluster_alias.alias, master_instance.cluster_name)) AS cluster_alias,
		MIN(IFNULL(ham_cluster_domain_name.domain_name, master_instance.cluster_name)) AS cluster_domain,
		MIN(master_instance.last_checked_timestamp <= master_instance.last_seen_timestamp and master_instance.last_attempted_check_timestamp <= master_instance.last_seen_timestamp + interval ? second) = 1 AS is_last_check_valid,
		MIN((master_instance.upstream_host IN('', '_') OR master_instance.upstream_port = 0 OR substr(master_instance.upstream_host, 1, 2) = '//') AND (master_instance.db_role = 'Primary' OR master_instance.db_role = 'Normal')) AS is_master,
		MIN(CONCAT(master_instance.hostname, ':', master_instance.port) = master_instance.cluster_name) AS is_cluster_master,
		COUNT(replica_instance.hostname) AS count_replicas,
		IFNULL(SUM(replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp), 0) AS count_valid_replicas,
		IFNULL(SUM(replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp), 0) AS count_valid_replicating_replicas,
		IFNULL(SUM(replica_instance.last_checked_timestamp <= replica_instance.last_seen_timestamp), 0) AS count_replicas_failing_to_connect_to_master,
		MIN(master_instance.replication_depth) AS replication_depth,
		SUM(replica_instance.is_co_master) AS count_co_master_replicas,
		GROUP_CONCAT(concat(replica_instance.Hostname, ':', replica_instance.Port)) as downstream_hosts,
		MIN(master_downtime.downtime_active is not null and ifnull(master_downtime.end_timestamp, now()) > now()) AS is_downtimed,
		MIN(IFNULL(master_downtime.end_timestamp, '')) AS downtime_end_timestamp,
		MIN(IFNULL(unix_timestamp() - unix_timestamp(master_downtime.end_timestamp), 0)) AS downtime_remaining_seconds,
		IFNULL(SUM(replica_instance.replication_state > 0), 0) AS count_delayed_replicas,
		IFNULL(SUM(replica_downtime.downtime_active is not null and ifnull(replica_downtime.end_timestamp, now()) > now()), 0) AS count_downtimed_replicas
		FROM
			ham_database_instance master_instance
			LEFT JOIN ham_hostname_resolve ON(master_instance.hostname = ham_hostname_resolve.hostname)
			LEFT JOIN ham_database_instance replica_instance ON(INSTR(master_instance.downstream_hosts,CONCAT('"Hostname":"',replica_instance.hostname,'","Port":',replica_instance.port)) > 0)
			LEFT JOIN ham_database_instance primary_instance ON (
				primary_instance.db_role = 'Primary'
				AND primary_instance.cluster_name = master_instance.cluster_name
				AND primary_instance.hostname <> master_instance.hostname
			)
			LEFT JOIN ham_database_instance_maintenance ON(master_instance.hostname = ham_database_instance_maintenance.hostname AND master_instance.port = ham_database_instance_maintenance.port AND ham_database_instance_maintenance.maintenance_active = 1)
			LEFT JOIN ham_database_instance_downtime as master_downtime ON(master_instance.hostname = master_downtime.hostname AND master_instance.port = master_downtime.port AND master_downtime.downtime_active = 1)
			LEFT JOIN ham_database_instance_downtime as replica_downtime ON(replica_instance.hostname = replica_downtime.hostname AND replica_instance.port = replica_downtime.port AND replica_downtime.downtime_active = 1)
			LEFT JOIN ham_cluster_alias ON(ham_cluster_alias.cluster_name = master_instance.cluster_name)
			LEFT JOIN ham_cluster_domain_name ON(ham_cluster_domain_name.cluster_name = master_instance.cluster_name)
		WHERE
			ham_database_instance_maintenance.maintenance_id IS NULL AND ? IN('', master_instance.cluster_name) AND
			master_instance.db_type = 'opengauss'
			%s
		GROUP BY 
			master_instance.db_type, master_instance.hostname, master_instance.port %s
		ORDER BY 
			is_master DESC, is_cluster_master DESC, count_replicas DESC;
	`,
		analysisQueryClusterClause, analysisQueryReductionClause)
	args := sqlutil.Args(config.ValidSecondsFromSeenToLastAttemptedCheck(), clusterId)
	if config.Config.ReduceReplicationAnalysisCount {
		args = append(args, config.ValidSecondsFromSeenToLastAttemptedCheck())
	}

	err := db.Query(query, args, func(m sqlutil.RowMap) error {
		a := dtstruct.ReplicationAnalysis{
			Analysis:               dtstruct.NoProblem,
			ProcessingNodeHostname: osp.GetHostname(),
			ProcessingNodeToken:    dtstruct.ProcessToken.Hash,
		}
		role := m.GetString("db_role")
		state := m.GetString("db_state")
		a.IsMaster = m.GetBool("is_master")
		a.CountPrimary = m.GetInt("count_primary")
		a.AnalyzedInstanceKey = dtstruct.InstanceKey{DBType: m.GetString("db_type"), Hostname: m.GetString("hostname"), Port: m.GetInt("port"), ClusterId: m.GetString("cluster_id")}
		a.AnalyzedInstanceUpstreamKey = dtstruct.InstanceKey{DBType: m.GetString("db_type"), Hostname: m.GetString("upstream_host"), Port: m.GetInt("upstream_port"), ClusterId: m.GetString("cluster_id")}
		a.AnalyzedInstanceDataCenter = m.GetString("pl_data_center")
		a.AnalyzedInstanceRegion = m.GetString("pl_region")
		a.ClusterDetails.ClusterName = m.GetString("cluster_name")
		a.LastCheckValid = m.GetBool("is_last_check_valid")
		a.CountReplicas = m.GetUint("count_replicas")
		a.CountValidReplicas = m.GetUint("count_valid_replicas")
		a.CountDowntimedReplicas = m.GetUint("count_downtimed_replicas")
		a.ReplicationDepth = m.GetUint("replication_depth")
		a.IsDowntimed = m.GetBool("is_downtimed")
		a.DowntimeEndTimestamp = m.GetString("downtime_end_timestamp")
		a.DowntimeRemainingSeconds = m.GetInt("downtime_remaining_seconds")
		a.ClusterDetails.ReadRecoveryInfo()

		a.Downstreams = *dtstruct.NewInstanceKeyMap()
		base.ReadCommaDelimitedList(&a.Downstreams, m.GetString("db_type"), m.GetString("downstream_hosts"))

		a.CountDelayedReplicas = m.GetUint("count_delayed_replicas")
		a.IsReadOnly = m.GetUint("read_only") == 1

		if !a.LastCheckValid {
			analysisMessage := fmt.Sprintf("analysis: ClusterName: %+v, IsMaster: %+v, LastCheckValid: %+v, LastCheckPartialSuccess: %+v, CountReplicas: %+v, CountValidReplicas: %+v, CountValidReplicatingReplicas: %+v, CountLaggingReplicas: %+v, CountDelayedReplicas: %+v, CountReplicasFailingToConnectToMaster: %+v",
				a.ClusterDetails.ClusterName, a.IsMaster, a.LastCheckValid, a.LastCheckPartialSuccess, a.CountReplicas, a.CountValidReplicas, a.CountValidReplicatingReplicas, a.CountLaggingReplicas, a.CountDelayedReplicas, a.CountReplicasFailingToConnectToMaster,
			)
			if cache.ClearToLog("analysis_dao", analysisMessage) {
				log.Debugf(analysisMessage)
			}
		}
		if a.IsMaster && !a.LastCheckValid && a.CountReplicas == 0 {
			a.Analysis = dtstruct.DeadMasterWithoutReplicas
			a.Description = "Master cannot be reached and has no replica"
			//
		} else if a.IsMaster && !a.LastCheckValid && a.CountValidReplicas == a.CountReplicas {
			a.Analysis = dtstruct.DeadMaster
			a.Description = "Master cannot be reached and none of its replicas is replicating"
			//
		} else if a.IsMaster && !a.LastCheckValid && a.CountReplicas > 0 && a.CountValidReplicas == 0 {
			a.Analysis = dtstruct.DeadMasterAndReplicas
			a.Description = "Master cannot be reached and none of its replicas is replicating"
			//
		} else if a.IsMaster && !a.LastCheckValid && a.CountValidReplicas < a.CountReplicas && a.CountValidReplicas > 0 {
			a.Analysis = dtstruct.DeadMasterAndSomeReplicas
			a.Description = "Master cannot be reached; some of its replicas are unreachable and none of its reachable replicas is replicating"
		} else if !a.IsMaster && !a.LastCheckValid && role == oconstant.DBStandby {
			a.Analysis = dtstruct.DeadStandby
			a.Description = "instance is standby role,and it is not valid"
			//
		} else if !a.IsMaster && !a.LastCheckValid && role == oconstant.DBCascade {
			a.Analysis = dtstruct.DeadCascadeStandby
			a.Description = "instance is cascade_standby role,and it is not valid"
			//
		} else if !a.IsMaster && !a.LastCheckValid && role == oconstant.DBDown {
			a.Analysis = dtstruct.DownInstance
			a.Description = "Master is reachable but none of its replicas is replicating"

		} else if a.IsMaster && a.LastCheckValid && a.CountPrimary > 0 {
			a.Analysis = dtstruct.DuplicatePrimary
			a.Description = "the cluster has more then one primary,should be shutdown"
			//
		} else if !a.IsMaster && a.LastCheckValid && state == oconstant.NeedRepair {
			a.Analysis = dtstruct.NeedRepair
			a.Description = "instance is cascade_standby role,and it is not valid"
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
					dtstruct.MasterSingleReplicaDead:
					a.IsReplicasDowntimed = true
					a.SkippableDueToDowntime = true
				}
			}
			if a.SkippableDueToDowntime && !hints.IncludeDowntimed {
				return
			}
			result = append(result, a)
		}

		if a.IsMaster && a.IsReadOnly {
			a.StructureAnalysis = append(a.StructureAnalysis, dtstruct.NoWriteableMasterStructureWarning)
		}
		appendAnalysis(&a)

		// Interesting enough for analysis
		if a.CountReplicas > 0 && hints.AuditAnalysis {
			go func() {
				_ = base.AuditInstanceAnalysisInChangelog(&a.AnalyzedInstanceKey, a.Analysis)
			}()
		}
		return nil
	})

	return result, log.Errore(err)
}

// GetCheckAndRecoverFunction get function for recover
func GetCheckAndRecoverFunction(analysisCode dtstruct.AnalysisCode, analyzedInstanceKey *dtstruct.InstanceKey) (
	checkAndRecoverFunction func(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *dtstruct.TopologyRecovery, err error),
	isActionableRecovery bool,
) {
	switch analysisCode {
	case dtstruct.DuplicatePrimary:
		return checkAndRecoverDuplicatePrimary, true
	case dtstruct.DeadMasterWithoutReplicas, dtstruct.DeadMasterAndReplicas:
		return checkAndRecoverDeadMasterWithoutReplicas, true
	case dtstruct.DeadMaster, dtstruct.DeadMasterAndSomeReplicas:
		return checkAndRecoverDeadMaster, true
	case dtstruct.DeadStandby, dtstruct.DeadCascadeStandby:
		return checkAndRecoverDeadReplica, true
	case dtstruct.DownInstance:
		return checkAndRecoverDownInstance, true

	//Right now this is mostly causing noise with no clear action.
	//Will revisit this in the future.
	default:
		return CheckAndRecoverGenericProblem, false
	}
}

func isGenerallyValidAsCandidateSiblingOfMaster(sibling *odtstruct.OpenGaussInstance) bool {
	if !sibling.IsLastCheckValid {
		return false
	}
	return true
}

// isValidAsCandidateSiblingOfIntermediateMaster checks to see that the given sibling is capable to take over instance's replicas
func isValidAsCandidateSiblingOfIntermediateMaster(intermediateMasterInstance *odtstruct.OpenGaussInstance, sibling *odtstruct.OpenGaussInstance) bool {
	if sibling.Key.Equals(&intermediateMasterInstance.Key) {
		// same instance
		return false
	}
	if !isGenerallyValidAsCandidateSiblingOfMaster(sibling) {
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
	//if sibling.ExecBinlogCoordinates.SmallerThan(&intermediateMasterInstance.ExecBinlogCoordinates) {
	//	return false
	//}
	return true
}

func CheckAndRecoverGenericProblem(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *dtstruct.TopologyRecovery, error) {
	return false, nil, nil
}

// GetCandidateSiblingOfMaster chooses the best sibling of a dead intermediate master
// to whom the IM's replicas can be moved.
func GetCandidateSiblingOfMaster(topologyRecovery *dtstruct.TopologyRecovery, intermediateMasterInstance *odtstruct.OpenGaussInstance) (*odtstruct.OpenGaussInstance, error) {

	siblings, err := GetDownStreamInstance(intermediateMasterInstance.DownstreamKeyMap.GetInstanceKeys())
	if err != nil {
		return nil, err
	}
	if len(siblings) < 1 {
		return nil, log.Errorf("topology_recovery: no siblings found for %+v", intermediateMasterInstance.Key)
	}

	sort.Sort(sort.Reverse(dtstruct.InstancesByCountReplicas(outil.ToInstanceHandler(siblings))))

	// In the next series of steps we attempt to return a good replacement.
	// None of the below attempts is sure to pick a winning server. Perhaps picked server is not enough up-todate -- but
	// this has small likelihood in the general case, and, well, it's an attempt. It's a Plan A, but we have Plan B & C if this fails.

	// At first, we try to return an "is_candidate" server in same dc & env
	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("searching for the best candidate sibling of dead intermediate master %+v", intermediateMasterInstance.Key))
	for _, candidate := range siblings {
		sibling := candidate
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

// RecoverDeadIntermediateMaster performs intermediate master recovery; complete logic inside
func RecoverDeadMaster(topologyRecovery *dtstruct.TopologyRecovery, skipProcesses bool) (successorInstance *odtstruct.OpenGaussInstance, err error) {
	topologyRecovery.Type = dtstruct.MasterRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey
	recoveryResolved := false

	base.AuditOperation("recover-dead-master", failedInstanceKey, "", "problem found; will recover")
	if !skipProcesses {
		if err := base.ExecuteProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, topologyRecovery.AddError(err)
		}
	}
	// before choose master, try to check and stop the origin master, TODO error should be typed, so it's can be checked
	_, err = NodeOperation(context.TODO(), failedInstanceKey, aaodtstruct.ActionType_STOP)
	if err != nil {
		log.Erroref(err)
	}

	MasterInstance, _, err := ReadFromBackendDB(failedInstanceKey)
	if err != nil {
		return nil, topologyRecovery.AddError(err)
	}

	// 选出primary候选人
	candidateSiblingOfMaster, _ := GetCandidateSiblingOfMaster(topologyRecovery, MasterInstance)
	if candidateSiblingOfMaster == nil {
		return nil, log.Errorf("no candidate instance found for key:%s", failedInstanceKey)
	}

	recoverFunc := func() {
		if candidateSiblingOfMaster == nil {
			return
		}
		//We have a candidate
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: will attempt a candidate master: %+v", candidateSiblingOfMaster.Key))

		//grpc：执行failover
		_, err = NodeOperation(context.TODO(), &candidateSiblingOfMaster.Key, aaodtstruct.ActionType_FAILOVER)
		if err == nil {
			recoveryResolved = true
			successorInstance = candidateSiblingOfMaster
			base.AuditOperation("recover-dead-intermediate-master", failedInstanceKey, "", fmt.Sprintf("failover to candidate instance: %+v", successorInstance.Key))
		}
	}
	// Plan A: find a replacement intermediate master in same Data Center
	if candidateSiblingOfMaster != nil && candidateSiblingOfMaster.DataCenter == MasterInstance.GetInstance().DataCenter {
		recoverFunc()
	}

	if !recoveryResolved {
		if candidateSiblingOfMaster != nil && candidateSiblingOfMaster.DataCenter != MasterInstance.GetInstance().DataCenter {
			base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadIntermediateMaster: will next attempt relocating to another DC server"))
			//relocateReplicasToCandidateSibling()
			recoverFunc()
		}
	}

	if !recoveryResolved {
		successorInstance = nil
	}
	//刷新candidateSiblingOfMaster状态
	//logic.DiscoverInstance(candidateSiblingOfMaster.Key)
	ReadFromBackendDB(&candidateSiblingOfMaster.Key)
	//更新原实例为role为down状态，状态为need repair
	base.UpdateInstanceRoleAndState(&MasterInstance.Key, oconstant.DBDown, oconstant.NeedRepair)
	base.ResolveRecovery(topologyRecovery, successorInstance)

	return successorInstance, err
}

func RecoverDeadMasterWithoutReplicas(topologyRecovery *dtstruct.TopologyRecovery, skipProcesses bool) (successorInstance *odtstruct.OpenGaussInstance, err error) {
	topologyRecovery.Type = dtstruct.MasterRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey

	base.AuditOperation(constant.AuditRecoverDeadMasterWithoutReplica, failedInstanceKey, "", "problem found; will recover")
	if !skipProcesses {
		if err := base.ExecuteProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, topologyRecovery.AddError(err)
		}
	}

	singleMasterInstance, _, err := ReadFromBackendDB(failedInstanceKey)
	if err != nil {
		return nil, topologyRecovery.AddError(err)
	}

	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMasterWithoutReplicas: will attempt a candidate intermediate master: %+v", singleMasterInstance.Key))
	//grpc：对单节点进行重建或者重启？
	agentAddr := singleMasterInstance.Key.Hostname + ":" + strconv.Itoa(oconstant.DefaultAgentPort)
	conn, err := cache.GetGRPC(context.TODO(), agentAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return
	}
	//defer conn.Close()

	oac := aaodtstruct.NewOpengaussAgentClient(conn)
	_, err = oac.ManageAction(context.TODO(), &aaodtstruct.ManageActionRequest{ActionType: aaodtstruct.ActionType_START})
	if err == nil {
		successorInstance = singleMasterInstance
		base.AuditOperation(constant.AuditRecoverDeadMasterWithoutReplica, failedInstanceKey, "", fmt.Sprintf("restart single master instance: %s", singleMasterInstance.Key))
	}

	base.ResolveRecovery(topologyRecovery, successorInstance)
	return successorInstance, err
}

func RecoverDeadReplica(topologyRecovery *dtstruct.TopologyRecovery, skipProcesses bool) (successorInstance *odtstruct.OpenGaussInstance, err error) {
	topologyRecovery.Type = dtstruct.ReplicationGroupMemberRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey

	base.AuditOperation("recover-dead-replica", failedInstanceKey, "", "problem found; will recover")
	if !skipProcesses {
		if err := base.ExecuteProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, topologyRecovery.AddError(err)
		}
	}

	deadReplicaInstance, _, err := ReadFromBackendDB(failedInstanceKey)
	if err != nil {
		return nil, topologyRecovery.AddError(err)
	}

	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadReplica: will attempt a candidate intermediate master: %+v", deadReplicaInstance.Key))
	//grpc：以Standby或者Cascade Standby方式启动
	agentAddr := deadReplicaInstance.Key.Hostname + ":" + strconv.Itoa(oconstant.DefaultAgentPort)
	conn, err := cache.GetGRPC(context.TODO(), agentAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return
	}
	//defer conn.Close()
	var actionType aaodtstruct.ActionType
	if deadReplicaInstance.Role == oconstant.DBCascade {
		actionType = aaodtstruct.ActionType_START_BY_CASCADE
	} else if deadReplicaInstance.Role == oconstant.DBStandby {
		actionType = aaodtstruct.ActionType_START_BY_STANDBY
	} else {
		actionType = aaodtstruct.ActionType_START
	}

	oac := aaodtstruct.NewOpengaussAgentClient(conn)
	_, err = oac.ManageAction(context.TODO(), &aaodtstruct.ManageActionRequest{ActionType: actionType})
	if err == nil {
		successorInstance = deadReplicaInstance
		base.AuditOperation(constant.AuditRecoverDeadMasterWithoutReplica, failedInstanceKey, "", fmt.Sprintf("restart replica instance[%s] as standby", deadReplicaInstance.Key))
	}

	base.ResolveRecovery(topologyRecovery, successorInstance)
	return successorInstance, err
}

func RecoverDownInstance(topologyRecovery *dtstruct.TopologyRecovery, skipProcesses bool) (successorInstance *odtstruct.OpenGaussInstance, err error) {
	topologyRecovery.Type = dtstruct.NotMasterRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey

	base.AuditOperation("recover-down-instance", failedInstanceKey, "", "problem found; will recover")
	if !skipProcesses {
		if err := base.ExecuteProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, topologyRecovery.AddError(err)
		}
	}

	downInstance, _, err := ReadFromBackendDB(failedInstanceKey)
	if err != nil {
		return nil, topologyRecovery.AddError(err)
	}

	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadReplica: will attempt a candidate intermediate master: %+v", downInstance.Key))
	//grpc：以Cascade Standby方式启动
	agentAddr := downInstance.Key.Hostname + ":" + strconv.Itoa(oconstant.DefaultAgentPort)
	conn, err := cache.GetGRPC(context.TODO(), agentAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return
	}
	//defer conn.Close()

	oac := aaodtstruct.NewOpengaussAgentClient(conn)
	_, err = oac.ManageAction(context.TODO(), &aaodtstruct.ManageActionRequest{ActionType: aaodtstruct.ActionType_BUILD})
	if err == nil {
		successorInstance = downInstance
		base.AuditOperation(constant.AuditRecoverDeadMasterWithoutReplica, failedInstanceKey, "", fmt.Sprintf("restart replica instance[%s] as standby", downInstance.Key))
	}

	base.ResolveRecovery(topologyRecovery, successorInstance)
	return successorInstance, err
}

func RecoverDuplicatePrimary(topologyRecovery *dtstruct.TopologyRecovery, skipProcesses bool) (successorInstance *odtstruct.OpenGaussInstance, err error) {
	topologyRecovery.Type = dtstruct.MasterRecovery
	analysisEntry := &topologyRecovery.AnalysisEntry
	failedInstanceKey := &analysisEntry.AnalyzedInstanceKey

	base.AuditOperation("recover-duplicate-primary", failedInstanceKey, "", "problem found; will recover")
	if !skipProcesses {
		if err := base.ExecuteProcesses(config.Config.PreFailoverProcesses, "PreFailoverProcesses", topologyRecovery, true); err != nil {
			return nil, topologyRecovery.AddError(err)
		}
	}

	PrimaryInstance, _, err := ReadFromBackendDB(failedInstanceKey)
	if err != nil {
		return nil, topologyRecovery.AddError(err)
	}

	duplicatePrimaryInstances, err := GetDuplicateInstance(failedInstanceKey, PrimaryInstance.ClusterName)
	if err != nil {
		return nil, topologyRecovery.AddError(err)
	}
	if len(duplicatePrimaryInstances) == 0 {
		return nil, topologyRecovery.AddError(errors.New("duplicatePrimaryInstances is null,do not need recovery"))
	}

	base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDuplicatePrimary: will stop the duplicate primary instance: %+v", duplicatePrimaryInstances[0].Key))

	agentAddr := duplicatePrimaryInstances[0].Key.Hostname + ":" + strconv.Itoa(oconstant.DefaultAgentPort)
	conn, err := cache.GetGRPC(context.TODO(), agentAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return
	}
	//defer conn.Close()

	oac := aaodtstruct.NewOpengaussAgentClient(conn)
	_, err = oac.ManageAction(context.TODO(), &aaodtstruct.ManageActionRequest{ActionType: aaodtstruct.ActionType_STOP})
	if err == nil {
		successorInstance = duplicatePrimaryInstances[0]
		base.AuditOperation("recover-duplicate-primary", failedInstanceKey, "", fmt.Sprintf("stop duplicate primary instance[%v]", duplicatePrimaryInstances[0]))
	}
	base.UpdateInstanceRoleAndState(&duplicatePrimaryInstances[0].Key, oconstant.DBDown, oconstant.NeedRepair)
	base.ResolveRecovery(topologyRecovery, successorInstance)
	return successorInstance, err
}

//checkAndRecoverDeadMasterWithoutReplicas will restart the single gaussdb instance
func checkAndRecoverDeadMasterWithoutReplicas(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *dtstruct.TopologyRecovery, error) {
	topologyRecovery, err := base.AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMasterWithoutReplicas: found an active or recent recovery on %+v. Will not issue another RecoverDeadIntermediateMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	_, err = RecoverDeadMasterWithoutReplicas(topologyRecovery, skipProcesses)
	if err != nil {
		return false, nil, err
	}

	return true, topologyRecovery, nil
}

//checkAndRecoverDeadMasterWithoutReplicas will restart the single gaussdb instance
func checkAndRecoverDuplicatePrimary(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *dtstruct.TopologyRecovery, error) {
	topologyRecovery, err := base.AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMasterWithoutReplicas: found an active or recent recovery on %+v. Will not issue another RecoverDeadIntermediateMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	_, err = RecoverDuplicatePrimary(topologyRecovery, skipProcesses)
	if err != nil {
		return false, nil, err
	}

	return true, topologyRecovery, nil
}

// checkAndRecoverDeadIntermediateMaster checks a given analysis, decides whether to take action, and possibly takes action
// Returns true when action was taken.
func checkAndRecoverDeadMaster(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *dtstruct.TopologyRecovery, error) {
	topologyRecovery, err := base.AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: found an active or recent recovery on %+v. Will not issue another RecoverDeadIntermediateMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	// That's it! We must do recovery!
	//recoverDeadIntermediateMasterCounter.Inc(1)
	promotedReplica, err := RecoverDeadMaster(topologyRecovery, skipProcesses)
	if promotedReplica != nil {
		// success
		//recoverDeadIntermediateMasterSuccessCounter.Inc(1)

		if !skipProcesses {
			// Execute post-master-failover processes
			topologyRecovery.SuccessorKey = &promotedReplica.Key
			topologyRecovery.SuccessorAlias = promotedReplica.InstanceAlias
			base.ExecuteProcesses(config.Config.PostIntermediateMasterFailoverProcesses, "PostMasterFailoverProcesses", topologyRecovery, false)
		}
	} else {
		//recoverDeadIntermediateMasterFailureCounter.Inc(1)
	}
	return true, topologyRecovery, err
}

func checkAndRecoverDeadReplica(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *dtstruct.TopologyRecovery, error) {
	topologyRecovery, err := base.AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		//base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: found an active or recent recovery on %+v. Will not issue another RecoverDeadIntermediateMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	_, err = RecoverDeadReplica(topologyRecovery, skipProcesses)
	if err != nil {
		return false, nil, err
	}

	return true, topologyRecovery, nil
}

func checkAndRecoverDownInstance(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (bool, *dtstruct.TopologyRecovery, error) {
	topologyRecovery, err := base.AttemptRecoveryRegistration(&analysisEntry, !forceInstanceRecovery, !forceInstanceRecovery)
	if topologyRecovery == nil {
		base.AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("- RecoverDeadMaster: found an active or recent recovery on %+v. Will not issue another RecoverDeadIntermediateMaster.", analysisEntry.AnalyzedInstanceKey))
		return false, nil, err
	}

	//TODO:以什么方式启动
	_, err = RecoverDownInstance(topologyRecovery, skipProcesses)
	if err != nil {
		return false, nil, err
	}

	return true, topologyRecovery, nil
}

// GracefulMasterTakeover will demote master of existing topology and promote its direct replica instead.
func GracefulMasterTakeover(clusterName string, designatedKey *dtstruct.InstanceKey, auto bool) (*dtstruct.TopologyRecovery, *dtstruct.LogCoordinates, error) {
	if _, err := NodeOperation(context.TODO(), designatedKey, aaodtstruct.ActionType_SWITCHOVER); err != nil {
		return nil, nil, err
	}
	return &dtstruct.TopologyRecovery{SuccessorKey: designatedKey}, &dtstruct.LogCoordinates{}, nil
}

// ReplicationConfirm check if replication between instance and stream is normal
func ReplicationConfirm(failedKey *dtstruct.InstanceKey, streamKey *dtstruct.InstanceKey, upstream bool) bool {

	// connect to agent
	conn, err := agent.ConnectToAgent(streamKey)
	if err != nil {
		return false
	}

	// confirm replication between instance
	resp, err := aaodtstruct.NewOpengaussAgentClient(conn).ReplicationConfirm(context.TODO(), &aaodtstruct.ReplicationConfirmRequest{ConfirmNodename: failedKey.Hostname, Upstream: upstream})
	if err != nil {
		log.Error("confirm replication between %s and %s failed, error:%s", failedKey, streamKey, err)
		return false
	}

	return resp.Normal
}
