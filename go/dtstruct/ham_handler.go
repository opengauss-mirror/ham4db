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
package dtstruct

import (
	"context"
	"crypto/tls"
	"database/sql"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"github.com/sjmudd/stopwatch"
	"time"
)

type HamHandler interface {
	SchemaBase() []string
	SchemaPatch() []string

	DriveName() string
	GetDBURI(host string, port int, args ...interface{}) string
	TLSCheck(string) bool
	TLSConfig(*tls.Config) error
	GetDBConnPool(uri string) (*sql.DB, error)

	// register cli command
	CliCmd(commandMap map[string]CommandDesc, cliParam *CliParam)

	// http api
	RegisterAPIRequest() map[string][]interface{}
	RegisterWebRequest() map[string][]interface{}

	// interface for sql in different case
	SQLProblemQuery() string
	SQLProblemCondition() string
	SQLProblemArgs(args ...interface{}) []interface{}

	// exec sql
	OpenTopology(host string, port int, args ...interface{}) (*sql.DB, error)
	ExecSQLOnInstance(instanceKey *InstanceKey, query string, args ...interface{}) (sql.Result, error)
	ScanInstanceRow(instanceKey *InstanceKey, query string, dest ...interface{}) error
	EmptyCommitInstance(instanceKey *InstanceKey) error

	// instance
	ReadFromBackendDB(instanceKey *InstanceKey) (InstanceAdaptor, bool, error)
	ReadInstanceByCondition(query string, condition string, args []interface{}, sort string) ([]InstanceAdaptor, error)
	ReadClusterInstances(clusterName string) ([]InstanceAdaptor, error)
	ReadReplicaInstances(masterKey *InstanceKey) ([]InstanceAdaptor, error)
	GetInfoFromInstance(ctx context.Context, instanceKey *InstanceKey, checkOnly, bufferWrites bool, latency *stopwatch.NamedStopwatch, agent string) (inst InstanceAdaptor, err error)
	GetSyncInfo(instanceKey *InstanceKey, bufferWrites bool, agent string) (interface{}, error)
	RowToInstance(rowMap sqlutil.RowMap) InstanceAdaptor
	WriteToBackendDB(context.Context, []InstanceAdaptor, bool, bool) error
	ForgetInstance(instanceKey *InstanceKey) error

	// replication
	StartReplication(ctx context.Context, instanceKey *InstanceKey) (interface{}, error)
	RestartReplication(instanceKey *InstanceKey) (interface{}, error)
	ResetReplication(instanceKey *InstanceKey) (InstanceAdaptor, error)
	CanReplicateFrom(first InstanceAdaptor, other InstanceAdaptor) (bool, error)
	StopReplication(*InstanceKey) (interface{}, error)
	StopReplicationNicely(instanceKey *InstanceKey, timeout time.Duration) (InstanceAdaptor, error)
	DelayReplication(instanceKey *InstanceKey, seconds int) error
	SetReadOnly(instanceKey *InstanceKey, readOnly bool) (InstanceAdaptor, error)
	SetSemiSyncOnDownstream(instanceKey *InstanceKey, enable bool) (InstanceAdaptor, error)
	SetSemiSyncOnUpstream(instanceKey *InstanceKey, enable bool) (InstanceAdaptor, error)
	SkipQuery(instanceKey *InstanceKey) (InstanceAdaptor, error)
	KillQuery(instanceKey *InstanceKey, process int64) (InstanceAdaptor, error)

	// master
	ChangeMasterTo(*InstanceKey, *InstanceKey, *LogCoordinates, bool, string) (InstanceAdaptor, error)
	DetachMaster(*InstanceKey) (InstanceAdaptor, error)
	ReattachMaster(*InstanceKey) (InstanceAdaptor, error)
	MakeCoMaster(instanceKey *InstanceKey) (InstanceAdaptor, error)
	MakeMaster(instanceKey *InstanceKey) (InstanceAdaptor, error)
	TakeMaster(instanceKey *InstanceKey, allowTakingCoMaster bool) (InstanceAdaptor, error)
	MakeLocalMaster(instanceKey *InstanceKey) (InstanceAdaptor, error)

	// topology
	MoveUp(instanceKey *InstanceKey) (InstanceAdaptor, error)
	MoveEquivalent(instanceKey, otherKey *InstanceKey) (InstanceAdaptor, error)
	MoveBelow(instanceKey, siblingKey *InstanceKey) (InstanceAdaptor, error)
	MoveUpReplicas(instanceKey *InstanceKey, pattern string) ([]InstanceAdaptor, InstanceAdaptor, error, []error)

	MatchUp(instanceKey *InstanceKey, requireInstanceMaintenance bool) (InstanceAdaptor, *LogCoordinates, error)
	MatchBelow(instanceKey, otherKey *InstanceKey, requireInstanceMaintenance bool) (InstanceAdaptor, *LogCoordinates, error)
	RelocateBelow(downstreamInstKey, upstreamInstKey *InstanceKey) (interface{}, error)
	RelocateReplicas(instanceKey, otherKey *InstanceKey, pattern string) (replicas []InstanceAdaptor, other InstanceAdaptor, err error, errs []error)

	MultiMatchReplicas(masterKey *InstanceKey, belowKey *InstanceKey, pattern string) ([]InstanceAdaptor, InstanceAdaptor, error, []error)
	MatchUpReplicas(masterKey *InstanceKey, pattern string) ([]InstanceAdaptor, InstanceAdaptor, error, []error)
	MultiMatchBelow(replicas []InstanceAdaptor, belowKey *InstanceKey, postponedFunctionsContainer *PostponedFunctionsContainer) (matchedReplicas []InstanceAdaptor, belowInstance InstanceAdaptor, err error, errs []error)

	RematchReplica(instanceKey *InstanceKey, requireInstanceMaintenance bool) (InstanceAdaptor, *LogCoordinates, error)
	TakeSiblings(instanceKey *InstanceKey) (instance InstanceAdaptor, takenSiblings int, err error)

	Repoint(*InstanceKey, *InstanceKey, string) (InstanceAdaptor, error)
	RegroupReplicas(masterKey *InstanceKey, returnReplicaEvenOnFailureToRegroup bool, onCandidateReplicaChosen func(InstanceAdaptor), postponedFunctionsContainer *PostponedFunctionsContainer) (aheadReplicas []InstanceAdaptor, equalReplicas []InstanceAdaptor, laterReplicas []InstanceAdaptor, cannotReplicateReplicas []InstanceAdaptor, instance InstanceAdaptor, err error)
	RepointReplicasTo(instanceKey *InstanceKey, pattern string, belowKey *InstanceKey) ([]InstanceAdaptor, error, []error)

	Topology(request *Request, historyTimestampPattern string, tabulated bool, printTags bool) (result interface{}, err error)

	ReplicationConfirm(failedKey *InstanceKey, streamKey *InstanceKey, upstream bool) bool

	// recovery
	GetReplicationAnalysis(clusterName, clusterId string, hints *ReplicationAnalysisHints) ([]ReplicationAnalysis, error)
	GetCheckAndRecoverFunction(analysisCode AnalysisCode, analyzedInstanceKey *InstanceKey) (
		checkAndRecoverFunction func(analysisEntry ReplicationAnalysis, candidateInstanceKey *InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *TopologyRecovery, err error),
		isActionableRecovery bool,
	)
	GetMasterRecoveryType(*ReplicationAnalysis) RecoveryType
	GetCandidateReplica(*InstanceKey, bool) (InstanceAdaptor, []InstanceAdaptor, []InstanceAdaptor, []InstanceAdaptor, []InstanceAdaptor, error)
	CategorizeReplication(*TopologyRecovery, *InstanceKey, func(InstanceAdaptor, bool) bool) (aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas []InstanceAdaptor, promotedReplica InstanceAdaptor, err error)
	RunEmergentOperations(analysisEntry *ReplicationAnalysis)
	CheckIfWouldBeMaster(InstanceAdaptor) bool
	//CheckAndRecoverDeadMaster(analysisEntry dtstruct.ReplicationAnalysis, candidateInstanceKey *dtstruct.InstanceKey, forceInstanceRecovery bool, skipProcesses bool) (recoveryAttempted bool, topologyRecovery *dtstruct.TopologyRecovery, err error)
	GracefulMasterTakeover(clusterName string, designatedKey *InstanceKey, auto bool) (topologyRecovery *TopologyRecovery, promotedMasterCoordinates *LogCoordinates, err error)

	// interface for agent
	BaseInfo() string
	MasterInfo() string
	SlaveInfo() string
	CascadeInfo() string
}
