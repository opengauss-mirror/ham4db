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
	"encoding/json"
	"fmt"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/cache"
	orcraft "gitee.com/opengauss/ham4db/go/core/consensus/raft"

	//orcraft "gitee.com/opengauss/ham4db/go/core/consensus/raft"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	ometrics "gitee.com/opengauss/ham4db/go/core/metric"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"github.com/rcrowley/go-metrics"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
)

// This file holds wrappers around routines to check if global
// recovery is disabled or not.
//
// This is determined by looking in the table
// ham4db.global_recovery_disable for a value 1.  Note: for
// recoveries to actually happen this must be configured explicitly
// in ham4db.conf.json. This setting is an emergency brake
// to quickly be able to prevent recoveries happening in some large
// outage type situation.  Maybe this value should be cached etc
// but we won't be doing that many recoveries at once so the load
// on this table is expected to be very low. It should be fine to
// go to the database each time.
var countPendingRecoveries int64
var countPendingRecoveriesGauge = metrics.NewGauge()

func init() {
	metrics.Register("recover.pending", countPendingRecoveriesGauge)
	ometrics.OnMetricTick(func() {
		countPendingRecoveriesGauge.Update(getCountPendingRecoveries())
	})
}

func getCountPendingRecoveries() int64 {
	return atomic.LoadInt64(&countPendingRecoveries)
}

// AcknowledgeClusterRecoveries marks active recoveries for given cluster as acknowledged.
// This also implied clearing their active period, which in turn enables further recoveries on those topologies
func AcknowledgeClusterRecoveries(clusterName string, owner string, comment string) (countAcknowledgedEntries int64, err error) {
	{
		whereClause := `cluster_name = ?`
		args := sqlutil.Args(clusterName)
		ClearAcknowledgedFailureDetections(whereClause, args)
		count, err := AcknowledgeRecoveries(owner, comment, false, whereClause, args)
		if err != nil {
			return count, err
		}
		countAcknowledgedEntries = countAcknowledgedEntries + count
	}
	{
		clusterInfo, err := ReadClusterInfo("", clusterName)
		whereClause := `cluster_alias = ? and cluster_alias != ''`
		args := sqlutil.Args(clusterInfo.ClusterAlias)
		ClearAcknowledgedFailureDetections(whereClause, args)
		count, err := AcknowledgeRecoveries(owner, comment, false, whereClause, args)
		if err != nil {
			return count, err
		}
		countAcknowledgedEntries = countAcknowledgedEntries + count

	}
	return countAcknowledgedEntries, nil
}

// CheckAndRecover is the main entry point for the recovery mechanism
func CheckAndRecover(handler dtstruct.HamHandler, specificInstance *dtstruct.InstanceKey, candidateInstanceKey *dtstruct.InstanceKey, skipProcesses bool) (recoveryAttempted bool, promotedReplicaKey *dtstruct.InstanceKey, err error) {
	// Allow the analysis to run even if we don't want to recover
	replicationAnalysis, err := handler.GetReplicationAnalysis("", "", &dtstruct.ReplicationAnalysisHints{IncludeDowntimed: true, AuditAnalysis: true})
	if err != nil {
		return false, nil, log.Errore(err)
	}
	if *dtstruct.RuntimeCLIFlags.Noop {
		log.Infof("--noop provided; will not execute processes")
		skipProcesses = true
	}
	// intentionally iterating entries in random order
	for _, j := range rand.Perm(len(replicationAnalysis)) {
		analysisEntry := replicationAnalysis[j]
		if specificInstance != nil {
			// We are looking for a specific instance; if this is not the one, skip!
			if !specificInstance.Equals(&analysisEntry.AnalyzedInstanceKey) {
				continue
			}
		}
		if analysisEntry.SkippableDueToDowntime && specificInstance == nil {
			// Only recover a downtimed server if explicitly requested
			continue
		}

		if specificInstance != nil {
			// force mode. Keep it synchronuous
			var topologyRecovery *dtstruct.TopologyRecovery
			recoveryAttempted, topologyRecovery, err = ExecuteCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, true, skipProcesses, handler.GetCheckAndRecoverFunction, handler.RunEmergentOperations, handler.ReplicationConfirm)
			log.Erroref(err)
			if topologyRecovery != nil {
				promotedReplicaKey = topologyRecovery.SuccessorKey
			}
		} else {
			go func() {
				if _, _, err = ExecuteCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, false, skipProcesses, handler.GetCheckAndRecoverFunction, handler.RunEmergentOperations, handler.ReplicationConfirm); err != nil {
					log.Erroref(err)
				}
			}()
		}
	}
	return recoveryAttempted, promotedReplicaKey, err
}

// ExecuteCheckAndRecoverFunction will choose the correct check & recovery function based on analysis.
// It executes the function synchronuously
func ExecuteCheckAndRecoverFunction(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool,
	getCheckAndRecoverFunction func(dtstruct.AnalysisCode, *dtstruct.InstanceKey) (checkAndRecoverFunction func(dtstruct.ReplicationAnalysis, *dtstruct.InstanceKey, bool, bool) (bool, *dtstruct.TopologyRecovery, error), isActionableRecovery bool),
	runEmergentOperations func(*dtstruct.ReplicationAnalysis), pathCheck func(*dtstruct.InstanceKey, *dtstruct.InstanceKey, bool) bool,
) (recoveryAttempted bool, topologyRecovery *dtstruct.TopologyRecovery, err error) {
	atomic.AddInt64(&countPendingRecoveries, 1)
	defer atomic.AddInt64(&countPendingRecoveries, -1)

	checkAndRecoverFunction, isActionableRecovery := getCheckAndRecoverFunction(analysisEntry.Analysis, &analysisEntry.AnalyzedInstanceKey)
	analysisEntry.IsActionableRecovery = isActionableRecovery
	runEmergentOperations(&analysisEntry)

	if checkAndRecoverFunction == nil {
		// Unhandled problem type
		if analysisEntry.Analysis != dtstruct.NoProblem {
			if cache.ClearToLog("executeCheckAndRecoverFunction", analysisEntry.AnalyzedInstanceKey.StringCode()) {
				log.Warningf("executeCheckAndRecoverFunction: ignoring analysisEntry that has no action plan: %+v; key: %+v",
					analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
			}
		}

		return false, nil, nil
	}
	// we have a recovery function; its execution still depends on filters if not disabled.
	if isActionableRecovery || cache.ClearToLog("executeCheckAndRecoverFunction: detection", analysisEntry.AnalyzedInstanceKey.StringCode()) {
		log.Infof("executeCheckAndRecoverFunction: proceeding with %+v detection on %+v; isActionable?: %+v; skipProcesses: %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, isActionableRecovery, skipProcesses)
	}

	// At this point we have validated there's a failure scenario for which we have a recovery path.

	if orcraft.IsRaftEnabled() {
		// with raft, all nodes can (and should) run analysis,
		// but only the leader proceeds to execute detection hooks and then to failover.
		if !orcraft.IsLeader() {
			log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
				"skipProcesses: %v: NOT detecting/recovering host (raft non-leader)",
				analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses)
			return false, nil, err
		}
	}

	// do more check alive by multi path
	if MultiPathCheck(&analysisEntry.AnalyzedInstanceKey, &analysisEntry.AnalyzedInstanceUpstreamKey, analysisEntry.Downstreams, pathCheck) {
		return false, nil, nil
	}

	// Initiate detection:
	registrationSuccess, _, err := checkAndExecuteFailureDetectionProcesses(analysisEntry, skipProcesses)
	if registrationSuccess {
		if orcraft.IsRaftEnabled() {
			_, err := orcraft.PublishCommand("register-failure-detection", analysisEntry)
			log.Errore(err)
		}
	}
	if err != nil {
		log.Errorf("executeCheckAndRecoverFunction: error on failure detection: %+v", err)
		return false, nil, err
	}
	// We don't mind whether detection really executed the processes or not
	// (it may have been silenced due to previous detection). We only care there's no error.

	// We're about to embark on recovery shortly...

	// Check for recovery being disabled globally
	if recoveryDisabledGlobally, err := IsRecoveryDisabled(); err != nil {
		// Unexpected. Shouldn't get this
		log.Errorf("Unable to determine if recovery is disabled globally: %v", err)
	} else if recoveryDisabledGlobally {
		if !forceInstanceRecovery {
			log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
				"skipProcesses: %v: NOT Recovering host (disabled globally)",
				analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses)

			return false, nil, err
		}
		log.Infof("CheckAndRecover: Analysis: %+v, InstanceKey: %+v, candidateInstanceKey: %+v, "+
			"skipProcesses: %v: recoveries disabled globally but forcing this recovery",
			analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, candidateInstanceKey, skipProcesses)
	}

	// Actually attempt recovery:
	if isActionableRecovery || cache.ClearToLog("executeCheckAndRecoverFunction: recovery", analysisEntry.AnalyzedInstanceKey.StringCode()) {
		log.Infof("executeCheckAndRecoverFunction: proceeding with %+v recovery on %+v; isRecoverable?: %+v; skipProcesses: %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey, isActionableRecovery, skipProcesses)
	}
	recoveryAttempted, topologyRecovery, err = checkAndRecoverFunction(analysisEntry, candidateInstanceKey, forceInstanceRecovery, skipProcesses)
	if !recoveryAttempted {
		return recoveryAttempted, topologyRecovery, err
	}
	if topologyRecovery == nil {
		return recoveryAttempted, topologyRecovery, err
	}
	if b, err := json.Marshal(topologyRecovery); err == nil {
		log.Infof("Topology recovery: %+v", string(b))
	} else {
		log.Infof("Topology recovery: %+v", *topologyRecovery)
	}
	if !skipProcesses {
		if topologyRecovery.SuccessorKey == nil {
			// Execute general unsuccessful post failover processes
			ExecuteProcesses(config.Config.PostUnsuccessfulFailoverProcesses, "PostUnsuccessfulFailoverProcesses", topologyRecovery, false)
		} else {
			// Execute general post failover processes
			EndDowntime(topologyRecovery.SuccessorKey)
			ExecuteProcesses(config.Config.PostFailoverProcesses, "PostFailoverProcesses", topologyRecovery, false)
		}
	}
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Waiting for %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))
	topologyRecovery.Wait()
	AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Executed %d postponed functions", topologyRecovery.PostponedFunctionsContainer.Len()))
	if topologyRecovery.PostponedFunctionsContainer.Len() > 0 {
		AuditTopologyRecovery(topologyRecovery, fmt.Sprintf("Executed postponed functions: %+v", strings.Join(topologyRecovery.PostponedFunctionsContainer.Descriptions(), ", ")))
	}
	return recoveryAttempted, topologyRecovery, err
}

// checkAndExecuteFailureDetectionProcesses tries to register for failure detection and potentially executes
// failure-detection processes.
func checkAndExecuteFailureDetectionProcesses(analysisEntry dtstruct.ReplicationAnalysis, skipProcesses bool) (detectionRegistrationSuccess bool, processesExecutionAttempted bool, err error) {
	if ok, _ := AttemptFailureDetectionRegistration(&analysisEntry); !ok {
		if cache.ClearToLog("checkAndExecuteFailureDetectionProcesses", analysisEntry.AnalyzedInstanceKey.StringCode()) {
			log.Infof("checkAndExecuteFailureDetectionProcesses: could not register %+v detection on %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
		}
		return false, false, nil
	}
	log.Infof("topology_recovery: detected %+v failure on %+v", analysisEntry.Analysis, analysisEntry.AnalyzedInstanceKey)
	// Execute on-detection processes
	if skipProcesses {
		return true, false, nil
	}
	err = ExecuteProcesses(config.Config.OnFailureDetectionProcesses, "OnFailureDetectionProcesses", dtstruct.NewTopologyRecovery(analysisEntry), true)
	return true, true, err
}

func MasterFailoverGeographicConstraintSatisfied(analysisEntry *dtstruct.ReplicationAnalysis, suggestedInstance dtstruct.InstanceAdaptor) (satisfied bool, dissatisfiedReason string) {
	if config.Config.PreventCrossDataCenterMasterFailover {
		if suggestedInstance.GetInstance().DataCenter != analysisEntry.AnalyzedInstanceDataCenter {
			return false, fmt.Sprintf("PreventCrossDataCenterMasterFailover: will not promote server in %s when failed server in %s", suggestedInstance.GetInstance().DataCenter, analysisEntry.AnalyzedInstanceDataCenter)
		}
	}
	if config.Config.PreventCrossRegionMasterFailover {
		if suggestedInstance.GetInstance().Region != analysisEntry.AnalyzedInstanceRegion {
			return false, fmt.Sprintf("PreventCrossRegionMasterFailover: will not promote server in %s when failed server in %s", suggestedInstance.GetInstance().Region, analysisEntry.AnalyzedInstanceRegion)
		}
	}
	return true, ""
}

// IsRecoveryDisabled returns true if Recoveries are disabled globally
func IsRecoveryDisabled() (disabled bool, err error) {
	query := `
		SELECT
			COUNT(*) as mycount
		FROM
			ham_global_recovery_disable
		WHERE
			disable_recovery=?
		`
	err = db.Query(query, sqlutil.Args(1), func(m sqlutil.RowMap) error {
		mycount := m.GetInt("mycount")
		disabled = (mycount > 0)
		return nil
	})
	if err != nil {
		err = log.Errorf("recovery.IsRecoveryDisabled(): %v", err)
	}
	return disabled, err
}

// DisableRecovery ensures recoveries are disabled globally
func DisableRecovery() error {
	_, err := db.ExecSQL(`
		INSERT IGNORE INTO ham_global_recovery_disable
			(disable_recovery)
		VALUES  (1)
	`,
	)
	return err
}

// EnableRecovery ensures recoveries are enabled globally
func EnableRecovery() error {
	// The "WHERE" clause is just to avoid full-scan reports by monitoring tools
	_, err := db.ExecSQL(`
		DELETE FROM ham_global_recovery_disable WHERE disable_recovery >= 0
	`,
	)
	return err
}

func SetRecoveryDisabled(disabled bool) error {
	if disabled {
		return DisableRecovery()
	}
	return EnableRecovery()
}

// MultiPathCheck check if instance is alive by its upstream and downstream
func MultiPathCheck(failed *dtstruct.InstanceKey, upstream *dtstruct.InstanceKey, downstreamMap dtstruct.InstanceKeyMap, pathCheck func(*dtstruct.InstanceKey, *dtstruct.InstanceKey, bool) bool) bool {

	wg, resultMap := sync.WaitGroup{}, make(map[string]bool)

	// check by upstream
	wg.Add(1)
	go func() {
		defer wg.Done()
		resultMap[upstream.Hostname+"->"+failed.Hostname] = pathCheck(failed, upstream, true)
	}()

	// check by all downstream
	for key := range downstreamMap {
		downstream := key
		wg.Add(1)
		go func() {
			defer wg.Done()
			resultMap[failed.Hostname+"->"+downstream.Hostname] = pathCheck(failed, &downstream, false)
		}()
	}
	wg.Wait()

	// check if path is success
	for _, v := range resultMap {
		if v {
			return true
		}
	}

	log.Info("multi path check for:%s, result:%v", failed, resultMap)
	return false
}
