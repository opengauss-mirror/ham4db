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
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/app"
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/common/constant"
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/db"
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/ham"
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/util"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"github.com/go-sql-driver/mysql"
	"github.com/sjmudd/stopwatch"
	"strings"
	"time"
)

type Mysql struct {
}

func init() {
	dtstruct.HamHandlerMap[constant.DBTMysql] = &Mysql{}
}

func (m *Mysql) ReplicationConfirm(failedKey *dtstruct.InstanceKey, streamKey *dtstruct.InstanceKey, upstream bool) bool {
	return ham.ReplicationConfirm(failedKey, streamKey, upstream)
}

func (m *Mysql) GetSyncInfo(instanceKey *dtstruct.InstanceKey, bufferWrites bool, agent string) (interface{}, error) {
	panic("implement me")
}

func (m *Mysql) ForgetInstance(instanceKey *dtstruct.InstanceKey) error {
	return ham.ForgetInstance(instanceKey)
}

func (m *Mysql) ReadClusterInstances(clusterName string) ([]dtstruct.InstanceAdaptor, error) {
	mi, err := ham.ReadClusterInstances(clusterName)
	return util.ToInstanceHander(mi), err
}

func (m *Mysql) CliCmd(commandMap map[string]dtstruct.CommandDesc, cliParam *dtstruct.CliParam) {
	app.CliCmd(commandMap, cliParam)
}

func (m *Mysql) RegisterAPIRequest() map[string][]interface{} {
	return app.RegisterAPIRequest()
}

func (m *Mysql) RegisterWebRequest() map[string][]interface{} {
	return app.RegisterWebRequest()
}

func (m *Mysql) KillQuery(instanceKey *dtstruct.InstanceKey, process int64) (dtstruct.InstanceAdaptor, error) {
	return ham.KillQuery(instanceKey, process)
}

func (m *Mysql) EmptyCommitInstance(instanceKey *dtstruct.InstanceKey) error {
	return ham.EmptyCommitInstance(instanceKey)
}

func (m *Mysql) ScanInstanceRow(instanceKey *dtstruct.InstanceKey, query string, dest ...interface{}) error {
	return ham.ScanInstanceRow(instanceKey, query, dest)
}

func (m *Mysql) OpenTopology(host string, port int, args ...interface{}) (*sql.DB, error) {
	return ham.OpenTopology(host, port, args)
}

func (m *Mysql) ExecSQLOnInstance(instanceKey *dtstruct.InstanceKey, query string, args ...interface{}) (sql.Result, error) {
	return ham.ExecSQLOnInstance(instanceKey, query, args)
}

func (m *Mysql) ReadInstanceByCondition(query string, condition string, args []interface{}, sort string) ([]dtstruct.InstanceAdaptor, error) {
	mis, err := ham.ReadInstancesByCondition(query, condition, args, sort)
	return util.ToInstanceHander(mis), err
}

func (m *Mysql) ReadReplicaInstances(masterKey *dtstruct.InstanceKey) ([]dtstruct.InstanceAdaptor, error) {
	mis, err := ham.ReadReplicaInstances(masterKey)
	return util.ToInstanceHander(mis), err
}

func (m *Mysql) ReadFromBackendDB(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, bool, error) {
	return ham.ReadFromBackendDB(instanceKey)
}

func (m *Mysql) SkipQuery(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return ham.SkipQuery(instanceKey)
}

func (m *Mysql) SetSemiSyncOnUpstream(instanceKey *dtstruct.InstanceKey, enableMaster bool) (dtstruct.InstanceAdaptor, error) {
	return ham.SetSemiSyncMaster(instanceKey, enableMaster)
}

func (m *Mysql) SetSemiSyncOnDownstream(instanceKey *dtstruct.InstanceKey, enableReplica bool) (dtstruct.InstanceAdaptor, error) {
	return ham.SetSemiSyncReplica(instanceKey, enableReplica)
}

//func (m *Mysql) CliCmd(command string, strict bool, databaseType string, instance string, destination string, owner string, reason string, duration string, pattern string, clusterAlias string, pool string, hostnameFlag string, instanceKey *dtstruct.InstanceKey, dstKey *dtstruct.InstanceKey, pfc *dtstruct.PostponedFunctionsContainer) {
//	cli.CliCmd(command, strict, databaseType, instance, destination, owner, reason, duration, pattern, clusterAlias, pool, hostnameFlag, instanceKey, dstKey, pfc)
//}

func (m *Mysql) DelayReplication(instanceKey *dtstruct.InstanceKey, seconds int) error {
	return ham.DelayReplication(instanceKey, seconds)
}

func (m *Mysql) RegroupReplicas(masterKey *dtstruct.InstanceKey, returnReplicaEvenOnFailureToRegroup bool, onCandidateReplicaChosen func(dtstruct.InstanceAdaptor), postponedFunctionsContainer *dtstruct.PostponedFunctionsContainer) ([]dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error) {
	aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, instance, err := ham.RegroupReplicas(masterKey, returnReplicaEvenOnFailureToRegroup, onCandidateReplicaChosen, postponedFunctionsContainer)
	return util.ToInstanceHander(aheadReplicas), util.ToInstanceHander(equalReplicas), util.ToInstanceHander(laterReplicas), util.ToInstanceHander(cannotReplicateReplicas), instance, err
}

func (m *Mysql) RepointReplicasTo(instanceKey *dtstruct.InstanceKey, pattern string, belowKey *dtstruct.InstanceKey) ([]dtstruct.InstanceAdaptor, error, []error) {
	mis, err, errs := ham.RepointReplicasTo(instanceKey, pattern, belowKey)
	return util.ToInstanceHander(mis), err, errs
}

func (m *Mysql) Topology(request *dtstruct.Request, historyTimestampPattern string, tabulated bool, printTags bool) (result interface{}, err error) {
	return ham.Topology(request, historyTimestampPattern, tabulated, printTags)
}

func (m *Mysql) Repoint(instanceKey *dtstruct.InstanceKey, masterKey *dtstruct.InstanceKey, gtidHint string) (dtstruct.InstanceAdaptor, error) {
	return ham.Repoint(instanceKey, masterKey, constant.OperationGTIDHint(gtidHint))
}

func (m *Mysql) GetMasterRecoveryType(analysis *dtstruct.ReplicationAnalysis) dtstruct.RecoveryType {
	return ham.GetMasterRecoveryType(analysis)
}

func (m *Mysql) RunEmergentOperations(analysisEntry *dtstruct.ReplicationAnalysis) {
	ham.RunEmergentOperations(analysisEntry)
}

func (m *Mysql) GetReplicationAnalysis(clusterName, clusterId string, hints *dtstruct.ReplicationAnalysisHints) ([]dtstruct.ReplicationAnalysis, error) {
	return ham.GetReplicationAnalysis(clusterName, clusterId, hints)
}

func (m *Mysql) GetCheckAndRecoverFunction(analysisCode dtstruct.AnalysisCode, analyzedInstanceKey *dtstruct.InstanceKey) (checkAndRecoverFunction func(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *dtstruct.TopologyRecovery, err error), isActionableRecovery bool) {
	return ham.GetCheckAndRecoverFunction(analysisCode, analyzedInstanceKey)
}

func (m *Mysql) CheckAndRecoverDeadMaster(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *dtstruct.TopologyRecovery, err error) {
	return ham.CheckAndRecoverDeadMaster(analysisEntry, candidateInstanceKey, forceInstanceRecovery, skipProcesses)
}

func (m *Mysql) GracefulMasterTakeover(clusterName string, designatedKey *dtstruct.InstanceKey, auto bool) (topologyRecovery *dtstruct.TopologyRecovery, promotedMasterCoordinates *dtstruct.LogCoordinates, err error) {
	return ham.GracefulMasterTakeover(clusterName, designatedKey, auto)
}

func (m *Mysql) CheckIfWouldBeMaster(instance dtstruct.InstanceAdaptor) bool {
	return !instance.IsReplicaServer()
}

func (m *Mysql) StopReplicationNicely(instanceKey *dtstruct.InstanceKey, timeout time.Duration) (dtstruct.InstanceAdaptor, error) {
	return ham.StopReplicationNicely(instanceKey, timeout)
}

func (m *Mysql) MakeMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return ham.MakeMaster(instanceKey)
}

func (m *Mysql) TakeMaster(instanceKey *dtstruct.InstanceKey, allowTakingCoMaster bool) (dtstruct.InstanceAdaptor, error) {
	return ham.TakeMaster(instanceKey, allowTakingCoMaster)
}

func (m *Mysql) MakeLocalMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return ham.MakeLocalMaster(instanceKey)
}

func (m *Mysql) MatchUp(instanceKey *dtstruct.InstanceKey, requireInstanceMaintenance bool) (dtstruct.InstanceAdaptor, *dtstruct.LogCoordinates, error) {
	return ham.MatchUp(instanceKey, requireInstanceMaintenance)
}

func (m *Mysql) MatchBelow(instanceKey, otherKey *dtstruct.InstanceKey, requireInstanceMaintenance bool) (dtstruct.InstanceAdaptor, *dtstruct.LogCoordinates, error) {
	return ham.MatchBelow(instanceKey, otherKey, requireInstanceMaintenance)
}

func (m *Mysql) RelocateBelow(instanceKey, otherKey *dtstruct.InstanceKey) (interface{}, error) {
	return ham.RelocateBelow(instanceKey, otherKey)
}

func (m *Mysql) RelocateReplicas(instanceKey, otherKey *dtstruct.InstanceKey, pattern string) ([]dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error, []error) {
	replicas, other, err, errs := ham.RelocateReplicas(instanceKey, otherKey, pattern)
	return util.ToInstanceHander(replicas), other, err, errs
}

func (m *Mysql) MultiMatchReplicas(masterKey *dtstruct.InstanceKey, belowKey *dtstruct.InstanceKey, pattern string) ([]dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error, []error) {
	replicas, masterInstance, err, errors := ham.MultiMatchReplicas(masterKey, belowKey, pattern)
	return util.ToInstanceHander(replicas), masterInstance, err, errors
}

func (m *Mysql) MatchUpReplicas(masterKey *dtstruct.InstanceKey, pattern string) ([]dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error, []error) {
	matchedReplicas, belowInstance, err, errs := ham.MatchUpReplicas(masterKey, pattern)
	return util.ToInstanceHander(matchedReplicas), belowInstance, err, errs
}

func (m *Mysql) MultiMatchBelow(replicas []dtstruct.InstanceAdaptor, belowKey *dtstruct.InstanceKey, postponedFunctionsContainer *dtstruct.PostponedFunctionsContainer) ([]dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error, []error) {
	matchedReplicas, belowInstance, err, errs := ham.MultiMatchBelow(util.ToMysqlInstance(replicas), belowKey, postponedFunctionsContainer)
	return util.ToInstanceHander(matchedReplicas), belowInstance, err, errs
}

func (m *Mysql) RematchReplica(instanceKey *dtstruct.InstanceKey, requireInstanceMaintenance bool) (dtstruct.InstanceAdaptor, *dtstruct.LogCoordinates, error) {
	return ham.RematchReplica(instanceKey, requireInstanceMaintenance)
}

func (m *Mysql) TakeSiblings(instanceKey *dtstruct.InstanceKey) (instance dtstruct.InstanceAdaptor, takenSiblings int, err error) {
	return ham.TakeSiblings(instanceKey)
}

func (m *Mysql) MakeCoMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return ham.MakeCoMaster(instanceKey)
}

func (m *Mysql) ResetReplication(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return ham.ResetReplication(instanceKey)
}

func (m *Mysql) MoveBelow(instanceKey, siblingKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return ham.MoveBelow(instanceKey, siblingKey)
}

func (m *Mysql) MoveUpReplicas(instanceKey *dtstruct.InstanceKey, pattern string) ([]dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error, []error) {
	res, instance, err, errs := ham.MoveUpReplicas(instanceKey, pattern)
	return util.ToInstanceHander(res), instance, err, errs
}

func (m *Mysql) MoveEquivalent(instanceKey, otherKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return ham.MoveEquivalent(instanceKey, otherKey)
}

func (m *Mysql) MoveUp(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return ham.MoveUp(instanceKey)
}

func (m *Mysql) CanReplicateFrom(first dtstruct.InstanceAdaptor, other dtstruct.InstanceAdaptor) (bool, error) {
	return ham.CanReplicateFrom(first, other)
}

func (m *Mysql) IsSmallerBinlogFormat(binlogFormat string, otherBinlogFormat string) bool {
	return ham.IsSmallerBinlogFormat(binlogFormat, otherBinlogFormat)
}

func (m *Mysql) RestartReplication(instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return ham.RestartReplication(instanceKey)
}

func (m *Mysql) SetReadOnly(instanceKey *dtstruct.InstanceKey, readOnly bool) (dtstruct.InstanceAdaptor, error) {
	return ham.SetReadOnly(instanceKey, readOnly)
}

func (m *Mysql) StartReplication(ctx context.Context, instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return ham.StartReplication(ctx, instanceKey)
}

func (m *Mysql) GetCandidateReplica(masterKey *dtstruct.InstanceKey, forRematchPurposes bool) (dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, error) {
	candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := ham.GetCandidateReplica(masterKey, forRematchPurposes)
	return candidateReplica, util.ToInstanceHander(aheadReplicas), util.ToInstanceHander(equalReplicas), util.ToInstanceHander(laterReplicas), util.ToInstanceHander(cannotReplicateReplicas), err
}

func (m *Mysql) ChangeMasterTo(instanceKey *dtstruct.InstanceKey, masterKey *dtstruct.InstanceKey, masterBinlogCoordinates *dtstruct.LogCoordinates, skipUnresolve bool, gtidHint string) (dtstruct.InstanceAdaptor, error) {
	return ham.ChangeMasterTo(instanceKey, masterKey, masterBinlogCoordinates, skipUnresolve, constant.OperationGTIDHint(gtidHint))
}

// DetachMaster detaches a replica from its master by corrupting the Master_Host (in such way that is reversible)
func (m *Mysql) DetachMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return ham.DetachMaster(instanceKey)
}

// ReattachMaster reattaches a replica back onto its master by undoing a DetachMaster operation
func (m *Mysql) ReattachMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return ham.ReattachMaster(instanceKey)
}

func (m2 *Mysql) WriteToBackendDB(ctx context.Context, instances []dtstruct.InstanceAdaptor, instanceWasActuallyFound bool, updateLastSeen bool) error {
	return ham.WriteToBackendDB(instances, instanceWasActuallyFound, updateLastSeen)
}

// readInstanceRow reads a single instance row from the ham4db backend database.
func (m *Mysql) RowToInstance(rowMap sqlutil.RowMap) dtstruct.InstanceAdaptor {
	return ham.RowToInstance(rowMap)
}

func (m *Mysql) GetInfoFromInstance(ctx context.Context, instanceKey *dtstruct.InstanceKey, checkOnly, bufferWrites bool, latency *stopwatch.NamedStopwatch, agent string) (inst dtstruct.InstanceAdaptor, err error) {
	return ham.GetInfoFromInstance(instanceKey, checkOnly, bufferWrites, latency, agent)
}

func (m *Mysql) StopReplication(instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return ham.StopReplication(instanceKey)
}

func (m *Mysql) CategorizeReplication(topologyRecovery *dtstruct.TopologyRecovery, failedInstanceKey *dtstruct.InstanceKey, promotedRepIsIdeal func(dtstruct.InstanceAdaptor, bool) bool) ([]dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error) {
	aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, promotedReplica, err := ham.CategorizeReplication(topologyRecovery, failedInstanceKey, promotedRepIsIdeal)
	return util.ToInstanceHander(aheadReplicas), util.ToInstanceHander(equalReplicas), util.ToInstanceHander(laterReplicas), util.ToInstanceHander(cannotReplicateReplicas), promotedReplica, err
}

func (m *Mysql) DriveName() string {
	return "mysql"
}

func (m *Mysql) GetDBURI(host string, port int, args ...interface{}) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=%ds&readTimeout=%ds&interpolateParams=true",
		config.Config.MysqlTopologyUser,
		config.Config.MysqlTopologyPassword,
		host, port,
		config.Config.ConnectTimeoutSeconds,
		args[0],
	)
}

func (m *Mysql) TLSCheck(uri string) bool {
	db, _, _ := sqlutil.GetDB(uri)
	if err := db.Ping(); err != nil && (strings.Contains(err.Error(), constant.Error3159) || strings.Contains(err.Error(), constant.Error1045)) {
		return true
	}
	return false
}

func (m *Mysql) TLSConfig(tlsConfig *tls.Config) error {
	return mysql.RegisterTLSConfig("topology", tlsConfig)
}

func (m *Mysql) GetDBConnPool(uri string) (db *sql.DB, err error) {
	panic("implement me")
}

func (m *Mysql) SchemaBase() []string {
	return db.GenerateSQLBase()
}

func (m *Mysql) SchemaPatch() []string {
	return db.GenerateSQLPatch()
}

func (m *Mysql) BaseInfo() string {
	panic("implement me")
}

func (m *Mysql) MasterInfo() string {
	panic("implement me")
}

func (m *Mysql) SlaveInfo() string {
	panic("implement me")
}

func (m *Mysql) CascadeInfo() string {
	panic("implement me")
}

func (m *Mysql) SQLProblemQuery() string {
	return constant.MysqlDefaultQuery
}

func (m *Mysql) SQLProblemCondition() string {
	return `
			db_type = ? 
			and cluster_name LIKE (CASE WHEN ? = '' THEN '%' ELSE ? END)
			and (
				(last_seen_timestamp < last_checked_timestamp)
				or (unix_timestamp() - unix_timestamp(last_checked_timestamp) > ?)
				or (replication_sql_thread_state not in (-1 ,1))
				or (replication_io_thread_state not in (-1 ,1))
				or (abs(cast(seconds_behind_master as signed) - cast(sql_delay as signed)) > ?)
				or (abs(cast(replication_downstream_lag as signed) - cast(sql_delay as signed)) > ?)
				or (gtid_errant != '')
				or (replication_group_name != '' and replication_group_member_state != 'ONLINE')
			)
	`
}

func (m *Mysql) SQLProblemArgs(args ...interface{}) []interface{} {
	return append(sqlutil.Args(args...), config.Config.InstancePollSeconds*5, config.Config.ReasonableReplicationLagSeconds, config.Config.ReasonableReplicationLagSeconds)
}
