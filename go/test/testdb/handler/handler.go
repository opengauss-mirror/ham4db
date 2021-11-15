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
package handler

import (
	"context"
	"crypto/tls"
	"database/sql"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/test/testdb/common/constant"
	tdb "gitee.com/opengauss/ham4db/go/test/testdb/db"
	tdtstruct "gitee.com/opengauss/ham4db/go/test/testdb/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"github.com/sjmudd/stopwatch"
	"time"
)

func init() {
	dtstruct.HamHandlerMap[constant.DBTTestDB] = &TestDB{}
}

type TestDB struct {
}

func (t TestDB) ReplicationConfirm(failedKey *dtstruct.InstanceKey, streamKey *dtstruct.InstanceKey, upstream bool) bool {
	panic("implement me")
}

func (t TestDB) GetInfoFromInstance(ctx context.Context, instanceKey *dtstruct.InstanceKey, checkOnly, bufferWrites bool, latency *stopwatch.NamedStopwatch, agent string) (inst dtstruct.InstanceAdaptor, err error) {
	panic("implement me")
}

func (t TestDB) GetSyncInfo(instanceKey *dtstruct.InstanceKey, bufferWrites bool, agent string) (interface{}, error) {
	panic("implement me")
}

func (t TestDB) WriteToBackendDB(ctx context.Context, adaptors []dtstruct.InstanceAdaptor, b bool, b2 bool) error {
	return nil
}

func (t TestDB) ForgetInstance(instanceKey *dtstruct.InstanceKey) error {
	panic("implement me")
}

func (t TestDB) SchemaBase() []string {
	return tdb.GenerateSQLBase()
}

func (t TestDB) SchemaPatch() []string {
	return tdb.GenerateSQLPatch()
}

func (t TestDB) DriveName() string {
	panic("implement me")
}

func (t TestDB) GetDBURI(host string, port int, args ...interface{}) string {
	panic("implement me")
}

func (t TestDB) TLSCheck(s string) bool {
	panic("implement me")
}

func (t TestDB) TLSConfig(config *tls.Config) error {
	panic("implement me")
}

func (t TestDB) GetDBConnPool(uri string) (*sql.DB, error) {
	panic("implement me")
}

func (t TestDB) IsSmallerBinlogFormat(binlogFormat string, otherBinlogFormat string) bool {
	panic("implement me")
}

func (t TestDB) CliCmd(commandMap map[string]dtstruct.CommandDesc, cliParam *dtstruct.CliParam) {
}

func (t TestDB) RegisterAPIRequest() map[string][]interface{} {
	panic("implement me")
}

func (t TestDB) RegisterWebRequest() map[string][]interface{} {
	panic("implement me")
}

func (t TestDB) SQLProblemQuery() string {
	panic("implement me")
}

func (t TestDB) SQLProblemCondition() string {
	panic("implement me")
}

func (t TestDB) SQLProblemArgs(args ...interface{}) []interface{} {
	panic("implement me")
}

func (t TestDB) OpenTopology(host string, port int, args ...interface{}) (*sql.DB, error) {
	panic("implement me")
}

func (t TestDB) ExecSQLOnInstance(instanceKey *dtstruct.InstanceKey, query string, args ...interface{}) (sql.Result, error) {
	panic("implement me")
}

func (t TestDB) ScanInstanceRow(instanceKey *dtstruct.InstanceKey, query string, dest ...interface{}) error {
	panic("implement me")
}

func (t TestDB) EmptyCommitInstance(instanceKey *dtstruct.InstanceKey) error {
	panic("implement me")
}

func (t TestDB) ReadFromBackendDB(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, bool, error) {
	panic("implement me")
}

func (t TestDB) ReadInstanceByCondition(query string, condition string, args []interface{}, sort string) (instanceList []dtstruct.InstanceAdaptor, err error) {
	return
}

func (t TestDB) ReadClusterInstances(clusterName string) ([]dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (t TestDB) ReadReplicaInstances(masterKey *dtstruct.InstanceKey) ([]dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (t TestDB) RowToInstance(rowMap sqlutil.RowMap) dtstruct.InstanceAdaptor {
	panic("implement me")
}

func (t TestDB) StartReplication(ctx context.Context, instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return &tdtstruct.TestDBInstance{Instance: &dtstruct.Instance{Key: *instanceKey}}, nil
}

func (t TestDB) RestartReplication(instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return &tdtstruct.TestDBInstance{Instance: &dtstruct.Instance{Key: *instanceKey}}, nil
}

func (t TestDB) ResetReplication(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return &tdtstruct.TestDBInstance{Instance: &dtstruct.Instance{Key: *instanceKey}}, nil
}

func (t TestDB) CanReplicateFrom(first dtstruct.InstanceAdaptor, other dtstruct.InstanceAdaptor) (bool, error) {
	return false, nil
}

func (t TestDB) StopReplication(instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return &tdtstruct.TestDBInstance{Instance: &dtstruct.Instance{Key: *instanceKey}}, nil
}

func (t TestDB) StopReplicationNicely(instanceKey *dtstruct.InstanceKey, timeout time.Duration) (dtstruct.InstanceAdaptor, error) {
	return &tdtstruct.TestDBInstance{Instance: &dtstruct.Instance{Key: *instanceKey}}, nil
}

func (t TestDB) DelayReplication(instanceKey *dtstruct.InstanceKey, seconds int) error {
	return nil
}

func (t TestDB) SetReadOnly(instanceKey *dtstruct.InstanceKey, readOnly bool) (dtstruct.InstanceAdaptor, error) {
	return &tdtstruct.TestDBInstance{}, nil
}

func (t TestDB) SetSemiSyncOnDownstream(instanceKey *dtstruct.InstanceKey, enableReplica bool) (dtstruct.InstanceAdaptor, error) {
	return &tdtstruct.TestDBInstance{}, nil
}

func (t TestDB) SetSemiSyncOnUpstream(instanceKey *dtstruct.InstanceKey, enableMaster bool) (dtstruct.InstanceAdaptor, error) {
	return &tdtstruct.TestDBInstance{}, nil
}

func (t TestDB) SkipQuery(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return &tdtstruct.TestDBInstance{}, nil
}

func (t TestDB) KillQuery(instanceKey *dtstruct.InstanceKey, process int64) (dtstruct.InstanceAdaptor, error) {
	return &tdtstruct.TestDBInstance{}, nil
}

func (t TestDB) ChangeMasterTo(key *dtstruct.InstanceKey, key2 *dtstruct.InstanceKey, coordinates *dtstruct.LogCoordinates, b bool, s string) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (t TestDB) DetachMaster(key *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (t TestDB) ReattachMaster(key *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (t TestDB) MakeCoMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (t TestDB) MakeMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (t TestDB) TakeMaster(instanceKey *dtstruct.InstanceKey, allowTakingCoMaster bool) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (t TestDB) MakeLocalMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (t TestDB) MoveUp(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (t TestDB) MoveEquivalent(instanceKey, otherKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (t TestDB) MoveBelow(instanceKey, siblingKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (t TestDB) MoveUpReplicas(instanceKey *dtstruct.InstanceKey, pattern string) ([]dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error, []error) {
	panic("implement me")
}

func (t TestDB) MatchUp(instanceKey *dtstruct.InstanceKey, requireInstanceMaintenance bool) (dtstruct.InstanceAdaptor, *dtstruct.LogCoordinates, error) {
	panic("implement me")
}

func (t TestDB) MatchBelow(instanceKey, otherKey *dtstruct.InstanceKey, requireInstanceMaintenance bool) (dtstruct.InstanceAdaptor, *dtstruct.LogCoordinates, error) {
	panic("implement me")
}

func (t TestDB) RelocateBelow(downstreamInstKey, upstreamInstKey *dtstruct.InstanceKey) (interface{}, error) {
	panic("implement me")
}

func (t TestDB) RelocateReplicas(instanceKey, otherKey *dtstruct.InstanceKey, pattern string) (replicas []dtstruct.InstanceAdaptor, other dtstruct.InstanceAdaptor, err error, errs []error) {
	panic("implement me")
}

func (t TestDB) MultiMatchReplicas(masterKey *dtstruct.InstanceKey, belowKey *dtstruct.InstanceKey, pattern string) ([]dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error, []error) {
	panic("implement me")
}

func (t TestDB) MatchUpReplicas(masterKey *dtstruct.InstanceKey, pattern string) ([]dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error, []error) {
	panic("implement me")
}

func (t TestDB) MultiMatchBelow(replicas []dtstruct.InstanceAdaptor, belowKey *dtstruct.InstanceKey, postponedFunctionsContainer *dtstruct.PostponedFunctionsContainer) (matchedReplicas []dtstruct.InstanceAdaptor, belowInstance dtstruct.InstanceAdaptor, err error, errs []error) {
	panic("implement me")
}

func (t TestDB) RematchReplica(instanceKey *dtstruct.InstanceKey, requireInstanceMaintenance bool) (dtstruct.InstanceAdaptor, *dtstruct.LogCoordinates, error) {
	panic("implement me")
}

func (t TestDB) TakeSiblings(instanceKey *dtstruct.InstanceKey) (instance dtstruct.InstanceAdaptor, takenSiblings int, err error) {
	panic("implement me")
}

func (t TestDB) Repoint(key *dtstruct.InstanceKey, key2 *dtstruct.InstanceKey, s string) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (t TestDB) RegroupReplicas(masterKey *dtstruct.InstanceKey, returnReplicaEvenOnFailureToRegroup bool, onCandidateReplicaChosen func(dtstruct.InstanceAdaptor), postponedFunctionsContainer *dtstruct.PostponedFunctionsContainer) (aheadReplicas []dtstruct.InstanceAdaptor, equalReplicas []dtstruct.InstanceAdaptor, laterReplicas []dtstruct.InstanceAdaptor, cannotReplicateReplicas []dtstruct.InstanceAdaptor, instance dtstruct.InstanceAdaptor, err error) {
	panic("implement me")
}

func (t TestDB) RepointReplicasTo(instanceKey *dtstruct.InstanceKey, pattern string, belowKey *dtstruct.InstanceKey) ([]dtstruct.InstanceAdaptor, error, []error) {
	panic("implement me")
}

func (t TestDB) Topology(request *dtstruct.Request, historyTimestampPattern string, tabulated bool, printTags bool) (result interface{}, err error) {
	panic("implement me")
}

func (t TestDB) GetReplicationAnalysis(clusterName, clusterId string, hints *dtstruct.ReplicationAnalysisHints) ([]dtstruct.ReplicationAnalysis, error) {
	return []dtstruct.ReplicationAnalysis{}, nil
}

func (t TestDB) GetCheckAndRecoverFunction(analysisCode dtstruct.AnalysisCode, analyzedInstanceKey *dtstruct.InstanceKey) (checkAndRecoverFunction func(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *dtstruct.TopologyRecovery, err error), isActionableRecovery bool) {
	panic("implement me")
}

func (t TestDB) GetMasterRecoveryType(analysis *dtstruct.ReplicationAnalysis) dtstruct.RecoveryType {
	panic("implement me")
}

func (t TestDB) GetCandidateReplica(key *dtstruct.InstanceKey, b bool) (dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (t TestDB) CategorizeReplication(recovery *dtstruct.TopologyRecovery, key *dtstruct.InstanceKey, f func(dtstruct.InstanceAdaptor, bool) bool) (aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas []dtstruct.InstanceAdaptor, promotedReplica dtstruct.InstanceAdaptor, err error) {
	panic("implement me")
}

func (t TestDB) RunEmergentOperations(analysisEntry *dtstruct.ReplicationAnalysis) {
	panic("implement me")
}

func (t TestDB) CheckIfWouldBeMaster(handler dtstruct.InstanceAdaptor) bool {
	panic("implement me")
}

func (t TestDB) GracefulMasterTakeover(clusterName string, designatedKey *dtstruct.InstanceKey, auto bool) (topologyRecovery *dtstruct.TopologyRecovery, promotedMasterCoordinates *dtstruct.LogCoordinates, err error) {
	panic("implement me")
}

func (t TestDB) BaseInfo() string {
	panic("implement me")
}

func (t TestDB) MasterInfo() string {
	panic("implement me")
}

func (t TestDB) SlaveInfo() string {
	panic("implement me")
}

func (t TestDB) CascadeInfo() string {
	panic("implement me")
}

func (t TestDB) FailOver(instanceKey *dtstruct.InstanceKey) (string, error) {
	panic("implement me")
}

func (t TestDB) RunAsMaster(instanceKey *dtstruct.InstanceKey) (string, error) {
	panic("implement me")
}

func (t TestDB) RunAsCascade(instanceKey *dtstruct.InstanceKey) (string, error) {
	panic("implement me")
}
