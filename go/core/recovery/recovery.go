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
package recovery

import (
	"context"
	"errors"
	"fmt"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/instance"
	"gitee.com/opengauss/ham4db/go/core/log"

	"gitee.com/opengauss/ham4db/go/dtstruct"
)

// GracefulMasterTakeover will demote master of existing topology and promote its
// direct replica instead.
// It expects that replica to have no siblings.
// This function is graceful in that it will first lock down the master, then wait
// for the designated replica to catch up with last position.
// It will point old master at the newly promoted master at the correct coordinates, but will not start replication.
func GracefulMasterTakeover(clusterName string, designatedKey *dtstruct.InstanceKey, auto bool) (topologyRecovery *dtstruct.TopologyRecovery, promotedMasterCoordinates *dtstruct.LogCoordinates, err error) {
	return dtstruct.GetHamHandler(designatedKey.DBType).GracefulMasterTakeover(clusterName, designatedKey, auto)
}

// ForceExecuteRecovery can be called to issue a recovery process even if analysis says there is no recovery case.
// The caller of this function injects the type of analysis it wishes the function to assume.
// By calling this function one takes responsibility for one's actions.
func ForceExecuteRecovery(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *dtstruct.TopologyRecovery, err error) {
	handler := dtstruct.GetHamHandler(analysisEntry.AnalyzedInstanceKey.DBType)
	return base.ExecuteCheckAndRecoverFunction(analysisEntry, candidateInstanceKey, true, skipProcesses, handler.GetCheckAndRecoverFunction, handler.RunEmergentOperations, handler.ReplicationConfirm)
}

// ForceMasterFailOver trust master of given cluster is dead and initiates a fail over
func ForceMasterFailOver(dbt string, clusterName string) (topologyRecovery *dtstruct.TopologyRecovery, err error) {

	// get master for cluster
	masterList, err := instance.ReadClusterMaster(dbt, clusterName)
	if err != nil || len(masterList) != 1 {
		return nil, log.Errore(errors.New(fmt.Sprintf("cannot deduce cluster master for %+v, %s", clusterName, err)))
	}
	clusterMaster := masterList[0]

	// get analysis entry by force
	analysisEntry, err := ForceAnalysisEntry(clusterMaster.GetInstance().Key.DBType, clusterName, dtstruct.DeadMaster, dtstruct.ForceMasterFailoverCommandHint, &clusterMaster.GetInstance().Key)
	if err != nil {
		return nil, err
	}

	// execute recovery on given entry
	err = execForceExecuteRecovery(analysisEntry, nil, false)
	if err != nil {
		return nil, err
	}
	return topologyRecovery, nil
}

// ForceMasterTakeover trust master of given cluster is dead and fails over to designated instance which has to be its direct child.
func ForceMasterTakeover(clusterName string, destination dtstruct.InstanceAdaptor) (topologyRecovery *dtstruct.TopologyRecovery, err error) {

	// get master list from database
	masterList, err := instance.ReadClusterMasterWriteable(destination.GetInstance().Key.DBType, clusterName)
	if err != nil || len(masterList) != 1 {
		return nil, log.Errore(errors.New(fmt.Sprintf("cannot deduce cluster master for %+v, %s", clusterName, err)))
	}
	master := masterList[0]

	// if destination's upstream is not this master, just return
	if !destination.GetInstance().UpstreamKey.Equals(&master.GetInstance().Key) {
		return nil, log.Errore(errors.New(fmt.Sprintf("you may only promote a direct child of the master %+v. The master of %+v is %+v.", master.GetInstance().Key, destination.GetInstance().Key, destination.GetInstance().UpstreamKey)))
	}

	// demote current master and promote destination
	analysisEntry, err := ForceAnalysisEntry(master.GetInstance().Key.DBType, clusterName, dtstruct.DeadMaster, dtstruct.ForceMasterTakeoverCommandHint, &master.GetInstance().Key)
	if err != nil {
		return nil, err
	}

	// execute recovery by force
	err = execForceExecuteRecovery(analysisEntry, &destination.GetInstance().Key, false)
	if err != nil {
		return nil, err
	}
	return topologyRecovery, nil
}

// ForceAnalysisEntry construct replication analysis entry by force using specified analysis code and command hit on given failed instance
func ForceAnalysisEntry(dbt string, clusterName string, analysisCode dtstruct.AnalysisCode, commandHint string, failedInstanceKey *dtstruct.InstanceKey) (analysisEntry dtstruct.ReplicationAnalysis, err error) {

	// get cluster info
	clusterInfo, err := base.ReadClusterInfo(dbt, clusterName)
	if err != nil {
		return analysisEntry, err
	}

	// get replication analysis and find the failed instance
	analysisList, err := dtstruct.GetHamHandler(dbt).GetReplicationAnalysis(clusterInfo.ClusterName, "", &dtstruct.ReplicationAnalysisHints{IncludeDowntimed: true, IncludeNoProblem: true})
	if err != nil {
		return analysisEntry, err
	}
	for _, replAnalysis := range analysisList {
		if replAnalysis.AnalyzedInstanceKey.Equals(failedInstanceKey) {
			analysisEntry = replAnalysis
			break
		}
	}
	analysisEntry.Analysis = analysisCode // we force this analysis
	analysisEntry.CommandHint = commandHint
	analysisEntry.ClusterDetails = *clusterInfo
	analysisEntry.AnalyzedInstanceKey = *failedInstanceKey
	return analysisEntry, nil
}

// RefreshTopologyInstance will synchronously re-read topology instance
func RefreshTopologyInstance(instanceKey *dtstruct.InstanceKey) (*dtstruct.Instance, error) {

	// get instance info from remote database instance, will save info to database
	_, err := instance.GetInfoFromInstance(context.TODO(), instanceKey, "", nil)
	if err != nil {
		return nil, err
	}

	// read instance info from database
	inst, found, err := instance.ReadInstance(instanceKey)
	if err != nil || !found {
		return nil, err
	}

	return inst.GetInstance(), nil
}

// ExecForceExecuteRecovery exec function ForceExecuteRecovery and check the result
func execForceExecuteRecovery(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, skipProcess bool) error {
	recoveryAttempted, topologyRecovery, err := ForceExecuteRecovery(analysisEntry, candidateInstanceKey, skipProcess)
	if err != nil {
		return err
	}
	if !recoveryAttempted {
		return log.Errore(errors.New("unexpected error: recovery not attempted. this should not happen"))
	}
	if topologyRecovery == nil {
		return log.Errore(errors.New("recovery attempted but with no results. this should not happen"))
	}
	if topologyRecovery.SuccessorKey == nil {
		return log.Errore(errors.New("recovery attempted yet no replica promoted"))
	}
	return nil
}
