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
	"fmt"
	"gitee.com/opengauss/ham4db/go/adaptor/opengauss/app"
	aoconstant "gitee.com/opengauss/ham4db/go/adaptor/opengauss/common/constant"
	"gitee.com/opengauss/ham4db/go/adaptor/opengauss/db"
	"gitee.com/opengauss/ham4db/go/adaptor/opengauss/ham"
	"gitee.com/opengauss/ham4db/go/adaptor/opengauss/util"
	aodtstruct "gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/dtstruct"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"

	"gitee.com/opengauss/ham4db/go/dtstruct"
	"github.com/sjmudd/stopwatch"
	"time"
	//"unicode"
)

type OpenGauss struct {
}

func init() {
	dtstruct.HamHandlerMap[aoconstant.DBTOpenGauss] = &OpenGauss{}
}

func (og *OpenGauss) ReplicationConfirm(failedKey *dtstruct.InstanceKey, streamKey *dtstruct.InstanceKey, upstream bool) bool {
	return ham.ReplicationConfirm(failedKey, streamKey, upstream)
}

func (og *OpenGauss) GetSyncInfo(instanceKey *dtstruct.InstanceKey, bufferWrites bool, agent string) (interface{}, error) {
	return ham.GetSyncInfo(instanceKey)
}
func (og *OpenGauss) ForgetInstance(instanceKey *dtstruct.InstanceKey) error {
	return ham.ForgetInstance(instanceKey)
}

func (og *OpenGauss) ReadClusterInstances(clusterName string) ([]dtstruct.InstanceAdaptor, error) {
	oi, err := ham.ReadInstancesByCondition(constant.DefaultQuery, " cluster_name = ? ", sqlutil.Args(clusterName), "")
	return util.ToInstanceHandler(oi), err
}

func (og *OpenGauss) RegisterAPIRequest() map[string][]interface{} {
	return app.RegisterAPIRequest()
}

func (og *OpenGauss) RegisterWebRequest() map[string][]interface{} {
	return app.RegisterWebRequest()
}

func (og *OpenGauss) OpenTopology(host string, port int, args ...interface{}) (*sql.DB, error) {
	return ham.OpenTopology(host, port, args)
}

func (og *OpenGauss) ExecSQLOnInstance(instanceKey *dtstruct.InstanceKey, query string, args ...interface{}) (sql.Result, error) {
	mdb, err := og.OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, err
	}
	return sqlutil.ExecNoPrepare(mdb, query, args...)
}

func (og *OpenGauss) ScanInstanceRow(instanceKey *dtstruct.InstanceKey, query string, dest ...interface{}) error {
	panic("implement me")
}

func (og *OpenGauss) EmptyCommitInstance(instanceKey *dtstruct.InstanceKey) error {
	panic("implement me")
}

func (og *OpenGauss) KillQuery(instanceKey *dtstruct.InstanceKey, process int64) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) GetReplicationAnalysis(clusterName, clusterId string, hints *dtstruct.ReplicationAnalysisHints) ([]dtstruct.ReplicationAnalysis, error) {
	return ham.GetReplicationAnalysis(clusterName, clusterId, hints)
}

func (og *OpenGauss) GetCheckAndRecoverFunction(analysisCode dtstruct.AnalysisCode, analyzedInstanceKey *dtstruct.InstanceKey) (checkAndRecoverFunction func(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *dtstruct.TopologyRecovery, err error), isActionableRecovery bool) {
	return ham.GetCheckAndRecoverFunction(analysisCode, analyzedInstanceKey)
}

func (og *OpenGauss) GetMasterRecoveryType(analysis *dtstruct.ReplicationAnalysis) dtstruct.RecoveryType {
	panic("implement me")
}

// TODO need to do emergent opt
func (og *OpenGauss) RunEmergentOperations(analysisEntry *dtstruct.ReplicationAnalysis) {
	return
}

func (og *OpenGauss) ReadFromBackendDB(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, bool, error) {
	return ham.ReadFromBackendDB(instanceKey)
}

func (og *OpenGauss) ReadInstanceByCondition(query string, condition string, args []interface{}, sort string) ([]dtstruct.InstanceAdaptor, error) {
	ois, err := ham.ReadInstancesByCondition(query, condition, args, sort)
	return util.ToInstanceHandler(ois), err
}

func (og *OpenGauss) ReadReplicaInstances(masterKey *dtstruct.InstanceKey) ([]dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) GetInfoFromInstance(ctx context.Context, instanceKey *dtstruct.InstanceKey, checkOnly, bufferWrites bool, latency *stopwatch.NamedStopwatch, agent string) (inst dtstruct.InstanceAdaptor, err error) {
	return ham.GetInfoFromInstance(ctx, instanceKey, checkOnly, bufferWrites, latency, agent)
}

func (og *OpenGauss) WriteToBackendDB(ctx context.Context, instances []dtstruct.InstanceAdaptor, instanceWasActuallyFound bool, updateLastSeen bool) error {
	return ham.WriteToBackendDB(ctx, instances, instanceWasActuallyFound, updateLastSeen)
}

func (og *OpenGauss) StartReplication(ctx context.Context, instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return ham.StartReplica(ctx, instanceKey)
}

func (og *OpenGauss) RestartReplication(instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return ham.RestartReplication(context.TODO(), instanceKey)
}

func (og *OpenGauss) ResetReplication(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) CanReplicateFrom(first dtstruct.InstanceAdaptor, other dtstruct.InstanceAdaptor) (bool, error) {
	panic("implement me")
}

func (og *OpenGauss) StopReplication(instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return ham.StopReplication(context.TODO(), instanceKey)
}

func (og *OpenGauss) StopReplicationNicely(instanceKey *dtstruct.InstanceKey, timeout time.Duration) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) SetReadOnly(instanceKey *dtstruct.InstanceKey, readOnly bool) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) SetSemiSyncOnDownstream(instanceKey *dtstruct.InstanceKey, enableReplica bool) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) SetSemiSyncOnUpstream(instanceKey *dtstruct.InstanceKey, enableMaster bool) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) SkipQuery(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) ChangeMasterTo(key *dtstruct.InstanceKey, key2 *dtstruct.InstanceKey, coordinates *dtstruct.LogCoordinates, b bool, s string) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) DetachMaster(key *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) ReattachMaster(key *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) MakeCoMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) MakeMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) TakeMaster(instanceKey *dtstruct.InstanceKey, allowTakingCoMaster bool) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) MakeLocalMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) MoveUp(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) MoveEquivalent(instanceKey, otherKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) MoveBelow(instanceKey, siblingKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) MoveUpReplicas(instanceKey *dtstruct.InstanceKey, pattern string) ([]dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error, []error) {
	panic("implement me")
}

func (og *OpenGauss) MatchUp(instanceKey *dtstruct.InstanceKey, requireInstanceMaintenance bool) (dtstruct.InstanceAdaptor, *dtstruct.LogCoordinates, error) {
	panic("implement me")
}

func (og *OpenGauss) MatchBelow(instanceKey, otherKey *dtstruct.InstanceKey, requireInstanceMaintenance bool) (dtstruct.InstanceAdaptor, *dtstruct.LogCoordinates, error) {
	panic("implement me")
}

func (og *OpenGauss) RelocateBelow(downstreamInstKey, upstreamInstKey *dtstruct.InstanceKey) (interface{}, error) {
	return ham.RelocateBelow(downstreamInstKey, upstreamInstKey)
}

func (og *OpenGauss) RelocateReplicas(instanceKey, otherKey *dtstruct.InstanceKey, pattern string) (replicas []dtstruct.InstanceAdaptor, other dtstruct.InstanceAdaptor, err error, errs []error) {
	panic("implement me")
}

func (og *OpenGauss) MultiMatchReplicas(masterKey *dtstruct.InstanceKey, belowKey *dtstruct.InstanceKey, pattern string) ([]dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error, []error) {
	panic("implement me")
}

func (og *OpenGauss) MatchUpReplicas(masterKey *dtstruct.InstanceKey, pattern string) ([]dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error, []error) {
	panic("implement me")
}

func (og *OpenGauss) MultiMatchBelow(replicas []dtstruct.InstanceAdaptor, belowKey *dtstruct.InstanceKey, postponedFunctionsContainer *dtstruct.PostponedFunctionsContainer) (matchedReplicas []dtstruct.InstanceAdaptor, belowInstance dtstruct.InstanceAdaptor, err error, errs []error) {
	panic("implement me")
}

func (og *OpenGauss) RematchReplica(instanceKey *dtstruct.InstanceKey, requireInstanceMaintenance bool) (dtstruct.InstanceAdaptor, *dtstruct.LogCoordinates, error) {
	panic("implement me")
}

func (og *OpenGauss) TakeSiblings(instanceKey *dtstruct.InstanceKey) (instance dtstruct.InstanceAdaptor, takenSiblings int, err error) {
	panic("implement me")
}

func (og *OpenGauss) Repoint(key *dtstruct.InstanceKey, key2 *dtstruct.InstanceKey, s string) (dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) RegroupReplicas(masterKey *dtstruct.InstanceKey, returnReplicaEvenOnFailureToRegroup bool, onCandidateReplicaChosen func(dtstruct.InstanceAdaptor), postponedFunctionsContainer *dtstruct.PostponedFunctionsContainer) (aheadReplicas []dtstruct.InstanceAdaptor, equalReplicas []dtstruct.InstanceAdaptor, laterReplicas []dtstruct.InstanceAdaptor, cannotReplicateReplicas []dtstruct.InstanceAdaptor, instance dtstruct.InstanceAdaptor, err error) {
	panic("implement me")
}

func (og *OpenGauss) RepointReplicasTo(instanceKey *dtstruct.InstanceKey, pattern string, belowKey *dtstruct.InstanceKey) ([]dtstruct.InstanceAdaptor, error, []error) {
	panic("implement me")
}

func (og *OpenGauss) GetCandidateReplica(key *dtstruct.InstanceKey, b bool) (dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, error) {
	panic("implement me")
}

func (og *OpenGauss) CategorizeReplication(recovery *dtstruct.TopologyRecovery, key *dtstruct.InstanceKey, f func(dtstruct.InstanceAdaptor, bool) bool) (aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas []dtstruct.InstanceAdaptor, promotedReplica dtstruct.InstanceAdaptor, err error) {
	panic("implement me")
}

func (og *OpenGauss) CheckIfWouldBeMaster(handler dtstruct.InstanceAdaptor) bool {
	panic("implement me")
}

func (og *OpenGauss) CliCmd(commandMap map[string]dtstruct.CommandDesc, cliParam *dtstruct.CliParam) {
	app.CliCmd(commandMap, cliParam)
}

func (og *OpenGauss) DelayReplication(instanceKey *dtstruct.InstanceKey, seconds int) error {
	panic("implement me")
}

func (og *OpenGauss) Topology(request *dtstruct.Request, historyTimestampPattern string, tabulated bool, printTags bool) (result interface{}, err error) {
	return ham.Topology(request, historyTimestampPattern, tabulated, printTags)
}

func (og *OpenGauss) RowToInstance(m sqlutil.RowMap) dtstruct.InstanceAdaptor {
	return ham.RowToInstance(m)
}

func (og *OpenGauss) GetInstanceInfo(key *dtstruct.InstanceKey, b bool, stopwatch *stopwatch.NamedStopwatch) (inst dtstruct.InstanceAdaptor, err error) {
	panic("implement me")
}

func (og *OpenGauss) InstanceToBackendDB(instances []dtstruct.InstanceAdaptor, b bool, b2 bool) (string, []interface{}, error) {
	panic("implement me")
}

func (og *OpenGauss) GracefulMasterTakeover(clusterName string, designatedKey *dtstruct.InstanceKey, auto bool) (topologyRecovery *dtstruct.TopologyRecovery, promotedMasterCoordinates *dtstruct.LogCoordinates, err error) {
	return ham.GracefulMasterTakeover(clusterName, designatedKey, auto)
}

func (og *OpenGauss) RecoverDeadMaster(s string) {
	panic("implement me")
}

func (og *OpenGauss) DriveName() string {
	return "opengauss"
}

// GetDBURI "host=127.0.0.1 port=5432 user=gaussdb password=Enmo@123 dbname=postgres sslmode=disable"
func (og *OpenGauss) GetDBURI(host string, port int, args ...interface{}) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
		host,
		port,
		config.Config.MysqlTopologyUser,
		config.Config.MysqlTopologyPassword,
	)
}

func (og *OpenGauss) TLSCheck(uri string) bool {
	return false
}

func (og *OpenGauss) TLSConfig(t *tls.Config) error {
	return nil
}

func (og *OpenGauss) GetDBConnPool(uri string) (*sql.DB, error) {
	panic("implement me")
}

func (og *OpenGauss) SchemaBase() []string {
	return db.GenerateSQLBase()
}

func (og *OpenGauss) SchemaPatch() []string {
	return db.GenerateSQLPatch()
}

func (og *OpenGauss) FailOver(instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return ham.NodeOperation(context.TODO(), instanceKey, aodtstruct.ActionType_FAILOVER)
}

func (og *OpenGauss) RunAsMaster(instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return ham.NodeOperation(context.TODO(), instanceKey, aodtstruct.ActionType_START_BY_PRIMARY)
}

func (og *OpenGauss) RunAsCascade(instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return ham.NodeOperation(context.TODO(), instanceKey, aodtstruct.ActionType_START_BY_CASCADE)
}

// GetReplicationAnalysis will check for replication problems (dead master; unreachable master; etc)
func (og *OpenGauss) SQLProblemQuery() string {
	return constant.DefaultQuery
}

func (og *OpenGauss) SQLProblemCondition() string {
	return `
			db_type = ? 
			and cluster_name LIKE (CASE WHEN ? = '' THEN '%' ELSE ? END)
			and (
				(last_seen_timestamp < last_checked_timestamp)
				or (unix_timestamp() - unix_timestamp(last_checked_timestamp) > ?)
			)
	`
}

func (og *OpenGauss) SQLProblemArgs(sqlArgs ...interface{}) []interface{} {
	return append(sqlutil.Args(sqlArgs...), config.Config.InstancePollSeconds*5)
}

func (og *OpenGauss) BaseInfo() string    { return "base" }
func (og *OpenGauss) MasterInfo() string  { return "master" }
func (og *OpenGauss) SlaveInfo() string   { return "slave" }
func (og *OpenGauss) CascadeInfo() string { return "cascade" }
