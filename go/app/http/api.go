/*
   Copyright 2014 Outbrain Inc.

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

package http

import (
	"context"
	"fmt"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/agent"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/cache"
	"gitee.com/opengauss/ham4db/go/core/discover"
	hinstance "gitee.com/opengauss/ham4db/go/core/instance"
	"gitee.com/opengauss/ham4db/go/core/logic"
	"gitee.com/opengauss/ham4db/go/core/maintenance"
	"gitee.com/opengauss/ham4db/go/core/metric"
	"gitee.com/opengauss/ham4db/go/core/recovery"
	"gitee.com/opengauss/ham4db/go/core/replication"
	"gitee.com/opengauss/ham4db/go/core/topology"
	"gitee.com/opengauss/ham4db/go/dtstruct"

	"gitee.com/opengauss/ham4db/go/util"
	"github.com/opentracing/opentracing-go"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/render"

	"gitee.com/opengauss/ham4db/go/core/log"
	goutil "gitee.com/opengauss/ham4db/go/util/text"

	"gitee.com/opengauss/ham4db/go/config"
	orcraft "gitee.com/opengauss/ham4db/go/core/consensus/raft"
	"gitee.com/opengauss/ham4db/go/core/ha/process"
)

var registerApiList []string
var API = dtstruct.HttpAPI{}
var discoveryMetrics = metric.CreateOrReturnCollection("DISCOVERY_METRICS")
var queryMetrics = metric.CreateOrReturnCollection("BACKEND_WRITES")
var writeBufferMetrics = metric.CreateOrReturnCollection("WRITE_BUFFER")

// RegisterAPIRequest register api with proxy
func RegisterAPIRequest(method string, httpApi *dtstruct.HttpAPI, m *martini.ClassicMartini, isTypePre bool, path string, handler martini.Handler) {
	registerSingleAPIRequest(method, httpApi, m, isTypePre, path, handler, true)
}

// registerAPIRequestNoProxy register api without proxy
func registerAPIRequestNoProxy(method string, httpApi *dtstruct.HttpAPI, m *martini.ClassicMartini, isTypePre bool, path string, handler martini.Handler) {
	registerSingleAPIRequest(method, httpApi, m, isTypePre, path, handler, false)
}

// registerSingleAPIRequest register api to martini
func registerSingleAPIRequest(method string, httpApi *dtstruct.HttpAPI, m *martini.ClassicMartini, isTypePre bool, path string, handler martini.Handler, allowProxy bool) {

	// check if need to add type to path
	fullPath := fmt.Sprintf("%s/api/%s", httpApi.URLPrefix, path)
	if isTypePre {
		fullPath = fmt.Sprintf("%s/api/:type/%s", httpApi.URLPrefix, path)
	}

	// get handler for this api
	var handlerList []martini.Handler
	if allowProxy && config.Config.RaftEnabled {
		handlerList = append(handlerList, raftReverseProxy, handler)
	} else {
		handlerList = append(handlerList, handler)
	}

	// add api to martini according to its method
	availableApi := true
	switch method {
	case http.MethodGet:
		m.Get(fullPath, handlerList...)
	case http.MethodPut:
		m.Put(fullPath, handlerList...)
	case http.MethodDelete:
		m.Delete(fullPath, handlerList...)
	case http.MethodPatch:
		m.Patch(fullPath, handlerList...)
	default:
		log.Errorf("method:%s not be supported now, %s", method, fullPath)
		availableApi = false
	}

	// if api is available, append to registered path
	if availableApi {
		registerApiList = append(registerApiList, fullPath+","+method)
	}
}

// RegisterRequests makes for the de-facto list of known API calls
func RegisterRequests(httpApi *dtstruct.HttpAPI, m *martini.ClassicMartini) {

	// show all api registered
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "manage/list", ListAPI)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "manage/cache/hit", CacheHit)

	// new api
	RegisterAPIRequest(http.MethodPatch, httpApi, m, true, "discover/:clusterId/:host/:port", Discover)
	RegisterAPIRequest(http.MethodDelete, httpApi, m, true, "forget/:clusterId/:host/:port", Forget)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "check/:clusterId/:host/:port", Check)

	// topology api
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "topology/:hint", Topology)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "topology/:clusterId/:host/:port", Topology)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "topology/tabulated/:hint", TopologyTabulated)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "topology/tabulated/:clusterId/:host/:port", TopologyTabulated)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "topology/tag/:hint", TopologyWithTag)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "topology/tag/:clusterId/:host/:port", TopologyWithTag)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "topology/snapshot", TopologySnapshot)

	// recovery
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "analysis/:clusterId", ReplicationAnalysis)

	// query
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "info/sync/:clusterId/:host/:port", InfoSync)

	// TODO=======================

	// Smart relocation:
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "relocate/:host/:port/:belowHost/:belowPort", RelocateBelow)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "relocate-below/:host/:port/:belowHost/:belowPort", RelocateBelow)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "relocate-replicas/:host/:port/:belowHost/:belowPort", RelocateReplicas)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "regroup-replicas/:host/:port", RegroupReplicas)

	// Classic file:pos relocation:
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "move-up/:host/:port", MoveUp)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "move-up-replicas/:host/:port", MoveUpReplicas)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "move-below/:host/:port/:siblingHost/:siblingPort", MoveBelow)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "move-equivalent/:host/:port/:belowHost/:belowPort", MoveEquivalent)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "repoint/:host/:port/:belowHost/:belowPort", Repoint)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "repoint-replicas/:host/:port", RepointReplicas)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "make-co-master/:host/:port", MakeCoMaster)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "take-siblings/:host/:port", TakeSiblings)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "take-master/:host/:port", TakeMaster)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "master-equivalent/:host/:port/:logFile/:logPos", MasterEquivalent)

	// Binlog server relocation:
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "regroup-replicas-bls/:host/:port", RegroupReplicasBinlogServers)

	// GTID relocation:
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "move-below-gtid/:host/:port/:belowHost/:belowPort", MoveBelowGTID)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "move-replicas-gtid/:host/:port/:belowHost/:belowPort", MoveReplicasGTID)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "regroup-replicas-gtid/:host/:port", RegroupReplicasGTID)

	// Pseudo-GTID relocation:
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "match/:host/:port/:belowHost/:belowPort", MatchBelow)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "match-below/:host/:port/:belowHost/:belowPort", MatchBelow)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "match-up/:host/:port", MatchUp)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "match-replicas/:host/:port/:belowHost/:belowPort", MultiMatchReplicas)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "match-up-replicas/:host/:port", MatchUpReplicas)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "regroup-replicas-pgtid/:host/:port", RegroupReplicasPseudoGTID)
	// Legacy, need to revisit:
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "make-master/:host/:port", MakeMaster)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "make-local-master/:host/:port", MakeLocalMaster)

	// Replication, general:
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "enable-gtid/:host/:port", EnableGTID)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "disable-gtid/:host/:port", DisableGTID)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "locate-gtid-errant/:host/:port", LocateErrantGTID)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "gtid-errant-reset-master/:host/:port", ErrantGTIDResetMaster)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "gtid-errant-inject-empty/:host/:port", ErrantGTIDInjectEmpty)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "skip-query/:host/:port", SkipQuery)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "start-replica/:host/:port", StartReplication)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "restart-replica/:host/:port", RestartReplication)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "stop-replica/:clusterId/:host/:port", StopReplication)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "stop-replica-nice/:clusterId/:host/:port", StopReplicationNicely)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "reset-replica/:clusterId/:host/:port", ResetReplication)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "detach-replica/:host/:port", DetachReplicaMasterHost)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "reattach-replica/:host/:port", ReattachReplicaMasterHost)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "detach-replica-master-host/:host/:port", DetachReplicaMasterHost)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "reattach-replica-master-host/:host/:port", ReattachReplicaMasterHost)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "flush-binary-logs/:host/:port", FlushBinaryLogs)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "purge-binary-logs/:host/:port/:logFile", PurgeBinaryLogs)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "restart-replica-statements/:host/:port", RestartReplicationStatements)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "enable-semi-sync-master/:host/:port", EnableSemiSyncMaster)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "disable-semi-sync-master/:host/:port", DisableSemiSyncMaster)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "enable-semi-sync-replica/:host/:port", EnableSemiSyncReplica)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "disable-semi-sync-replica/:host/:port", DisableSemiSyncReplica)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "delay-replication/:host/:port/:seconds", DelayReplication)

	// Replication information:
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "can-replicate-from/:host/:port/:belowHost/:belowPort", CanReplicateFrom)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "can-replicate-from-gtid/:host/:port/:belowHost/:belowPort", CanReplicateFromGTID)

	// Instance:
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "set-read-only/:host/:port", SetReadOnly)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "set-writeable/:host/:port", SetWriteable)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "kill-query/:host/:port/:process", KillQuery)

	// Binary logs:
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "last-pseudo-gtid/:host/:port", LastPseudoGTID)

	// Pools:
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "submit-pool-instances/:pool", SubmitPoolInstances)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "cluster-pool-instances/:clusterName", ReadClusterPoolInstancesMap)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "cluster-pool-instances/:clusterName/:pool", ReadClusterPoolInstancesMap)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "heuristic-cluster-pool-instances/:clusterName", GetHeuristicClusterPoolInstances)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "heuristic-cluster-pool-instances/:clusterName/:pool", GetHeuristicClusterPoolInstances)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "heuristic-cluster-pool-lag/:clusterName", GetHeuristicClusterPoolInstancesLag)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "heuristic-cluster-pool-lag/:clusterName/:pool", GetHeuristicClusterPoolInstancesLag)

	// Information:
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "search/:searchString", Search)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "search", Search)

	// Cluster
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "cluster/:clusterHint", Cluster)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "cluster/alias/:clusterAlias", ClusterByAlias)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "cluster/instance/:host/:port", ClusterByInstance)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "cluster-info/:clusterHint", ClusterInfo)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "cluster-info/alias/:clusterAlias", ClusterInfoByAlias)

	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "set-cluster-alias/:clusterName", SetClusterAliasManualOverride)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "clusters", Clusters)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "clusters-info", ClustersInfo)

	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "masters", Masters)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "master/:clusterHint", ClusterMaster)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "instance-replicas/:host/:port", InstanceReplicas)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "list-instance", AllInstances)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "downtimed", Downtimed)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "downtimed/:clusterHint", Downtimed)

	// Key-value:
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "submit-masters-to-kv-stores", SubmitMastersToKvStores)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "submit-masters-to-kv-stores/:clusterHint", SubmitMastersToKvStores)

	// Tags:
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "tagged", Tagged)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "tags/:host/:port", Tags)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "tag-value/:host/:port", TagValue)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "tag-value/:host/:port/:tagName", TagValue)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "tag/:host/:port", Tag)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "tag/:host/:port/:tagName/:tagValue", Tag)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "untag/:host/:port", Untag)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "untag/:host/:port/:tagName", Untag)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "untag-all", UntagAll)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "untag-all/:tagName/:tagValue", UntagAll)

	// Instance management:
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "instance/:host/:port", Instance)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "discover/:host/:port", Discover)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "async-discover/:host/:port", AsyncDiscover)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "refresh/:host/:port", Refresh)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "forget/:host/:port", Forget)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "forget-cluster/:clusterHint", ForgetCluster)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "begin-maintenance/:host/:port/:owner/:reason", BeginMaintenance)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "end-maintenance/:host/:port", EndMaintenanceByInstanceKey)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "in-maintenance/:host/:port", InMaintenance)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "end-maintenance/:maintenanceKey", EndMaintenance)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "maintenance", Maintenance)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "begin-downtime/:host/:port/:owner/:reason", BeginDowntime)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "begin-downtime/:host/:port/:owner/:reason/:duration", BeginDowntime)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "end-downtime/:host/:port", EndDowntime)

	// Recovery:
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "replication-analysis", ReplicationAnalysis)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "replication-analysis/:clusterName", ReplicationAnalysisForCluster)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "replication-analysis/instance/:host/:port", ReplicationAnalysisForKey)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "recover/:host/:port", Recover)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "recover/:host/:port/:candidateHost/:candidatePort", Recover)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "recover-lite/:host/:port", RecoverLite)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "recover-lite/:host/:port/:candidateHost/:candidatePort", RecoverLite)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "graceful-master-takeover/:host/:port", GracefulMasterTakeover)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "graceful-master-takeover/:host/:port/:designatedHost/:designatedPort", GracefulMasterTakeover)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "graceful-master-takeover/:clusterHint", GracefulMasterTakeover)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "graceful-master-takeover/:clusterHint/:designatedHost/:designatedPort", GracefulMasterTakeover)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "graceful-master-takeover-auto/:host/:port", GracefulMasterTakeoverAuto)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "graceful-master-takeover-auto/:host/:port/:designatedHost/:designatedPort", GracefulMasterTakeoverAuto)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "graceful-master-takeover-auto/:clusterHint", GracefulMasterTakeoverAuto)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "graceful-master-takeover-auto/:clusterHint/:designatedHost/:designatedPort", GracefulMasterTakeoverAuto)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "force-master-failover/:host/:port", ForceMasterFailover)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "force-master-failover/:clusterHint", ForceMasterFailover)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "force-master-takeover/:clusterHint/:designatedHost/:designatedPort", ForceMasterTakeover)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "force-master-takeover/:host/:port/:designatedHost/:designatedPort", ForceMasterTakeover)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "register-candidate/:host/:port/:promotionRule", RegisterCandidate)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "automated-recovery-filters", AutomatedRecoveryFilters)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit-failure-detection", AuditFailureDetection)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit-failure-detection/:page", AuditFailureDetection)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit-failure-detection/id/:id", AuditFailureDetection)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit-failure-detection/alias/:clusterAlias", AuditFailureDetection)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit-failure-detection/alias/:clusterAlias/:page", AuditFailureDetection)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "replication-analysis-changelog", ReadReplicationAnalysisChangelog)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit-recovery", AuditRecovery)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit-recovery/:page", AuditRecovery)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit-recovery/id/:id", AuditRecovery)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit-recovery/uid/:uid", AuditRecovery)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit-recovery/cluster/:clusterName", AuditRecovery)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit-recovery/cluster/:clusterName/:page", AuditRecovery)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit-recovery/alias/:clusterAlias", AuditRecovery)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit-recovery/alias/:clusterAlias/:page", AuditRecovery)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit-recovery-steps/:uid", AuditRecoverySteps)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "active-cluster-recovery/:clusterName", ActiveClusterRecovery)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "recently-active-cluster-recovery/:clusterName", RecentlyActiveClusterRecovery)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "recently-active-instance-recovery/:host/:port", RecentlyActiveInstanceRecovery)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "ack-recovery/cluster/:clusterHint", AcknowledgeClusterRecoveries)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "ack-recovery/cluster/alias/:clusterAlias", AcknowledgeClusterRecoveries)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "ack-recovery/instance/:host/:port", AcknowledgeInstanceRecoveries)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "ack-recovery/:recoveryId", AcknowledgeRecovery)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "ack-recovery/uid/:uid", AcknowledgeRecovery)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "ack-all-recoveries", AcknowledgeAllRecoveries)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "blocked-recoveries", BlockedRecoveries)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "blocked-recoveries/cluster/:clusterName", BlockedRecoveries)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "disable-global-recoveries", DisableGlobalRecoveries)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "enable-global-recoveries", EnableGlobalRecoveries)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "check-global-recoveries", CheckGlobalRecoveries)

	// General
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "problems", Problems)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "problems/:clusterName", Problems)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit", Audit)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit/:page", Audit)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit/instance/:host/:port", Audit)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "audit/instance/:host/:port/:page", Audit)
	RegisterAPIRequest(http.MethodGet, httpApi, m, true, "resolve/:host/:port", Resolve)

	// Meta, no proxy
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "headers", Headers)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "health", Health)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "lb-check", LBCheck)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "_ping", LBCheck)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "leader-check", LeaderCheck)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "leader-check/:errorStatusCode", LeaderCheck)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "grab-election", GrabElection)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "raft-add-peer/:addr", RaftAddPeer)       // delegated to the raft leader
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "raft-remove-peer/:addr", RaftRemovePeer) // delegated to the raft leader
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "raft-yield/:node", RaftYield)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "raft-yield-hint/:hint", RaftYieldHint)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "raft-peers", RaftPeers)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "raft-state", RaftState)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "raft-leader", RaftLeader)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "raft-health", RaftHealth)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "raft-status", RaftStatus)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "raft-snapshot", RaftSnapshot)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "raft-follower-health-report/:authenticationToken/:raftBind/:raftAdvertise", RaftFollowerHealthReport)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "reload-configuration", ReloadConfiguration)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "hostname-resolve-cache", HostnameResolveCache)
	registerAPIRequestNoProxy(http.MethodGet, httpApi, m, false, "reset-hostname-resolve-cache", ResetHostnameResolveCache)
	// Meta
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "routed-leader-check", LeaderCheck)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "reelect", Reelect)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "reload-cluster-alias", ReloadClusterAlias)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "deregister-hostname-unresolve/:host/:port", DeregisterHostnameUnresolve)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "register-hostname-unresolve/:host/:port/:virtualname", RegisterHostnameUnresolve)

	// Bulk access to information
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "bulk-instances", BulkInstances)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "bulk-promotion-rules", BulkPromotionRules)

	// Monitoring
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "discovery-metric-raw/:seconds", DiscoveryMetricsRaw)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "discovery-metric-aggregated/:seconds", DiscoveryMetricsAggregated)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "discovery-queue-metric-raw/:seconds", DiscoveryQueueMetricsRaw)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "discovery-queue-metric-aggregated/:seconds", DiscoveryQueueMetricsAggregated)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "backend-query-metric-raw/:seconds", BackendQueryMetricsRaw)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "backend-query-metric-aggregated/:seconds", BackendQueryMetricsAggregated)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "write-buffer-metric-raw/:seconds", WriteBufferMetricsRaw)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "write-buffer-metric-aggregated/:seconds", WriteBufferMetricsAggregated)

	// TODO double check
	//// Agents
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "agents", Agents)
	RegisterAPIRequest(http.MethodGet, httpApi, m, false, "agent/health/:ip/:hostname/:port/:interval", AgentHealthCheck)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "agent/:host", Agent)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "agent-umount/:host", AgentUnmount)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "agent-mount/:host", AgentMountLV)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "agent-create-snapshot/:host", AgentCreateSnapshot)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "agent-removelv/:host", AgentRemoveLV)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "agent-mysql-stop/:host", AgentMySQLStop)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "agent-mysql-start/:host", AgentMySQLStart)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "agent-seed/:targetHost/:sourceHost", AgentSeed)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "agent-active-seeds/:host", AgentActiveSeeds)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "agent-recent-seeds/:host", AgentRecentSeeds)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "agent-seed-details/:seedId", AgentSeedDetails)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "agent-seed-states/:seedId", AgentSeedStates)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "agent-abort-seed/:seedId", AbortSeed)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "agent-custom-command/:host/:command", AgentCustomCommand)
	//RegisterAPIRequest(http.MethodGet,httpApi,m, "seeds", Seeds)

	// Configurable status check endpoint
	if config.Config.StatusEndpoint == constant.DefaultStatusAPIEndpoint {
		registerAPIRequestNoProxy(http.MethodGet, httpApi, m, true, "status", StatusCheck)
	} else {
		m.Get(config.Config.StatusEndpoint, StatusCheck)
	}
}

// Show all api registered
func ListAPI(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	// user authorized check
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, constant.HttpRespMsgUnAuth, nil))
		return
	}

	// response with api list sorted
	sort.Strings(registerApiList)
	base.Respond(r, dtstruct.NewApiResponse(dtstruct.OK, "", registerApiList))
}

// CacheHit show cache hit rate
func CacheHit(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	// user authorized check
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, constant.HttpRespMsgUnAuth, nil))
		return
	}

	// response with cache hit
	base.Respond(r, dtstruct.NewApiResponse(dtstruct.OK, "", cache.ShowCacheHit()))
}

// Discover issues a synchronous read on an instance
func Discover(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	span, ctx := opentracing.StartSpanFromContext(context.TODO(), "Discover")
	defer span.Finish()

	// check and generate instance key
	instanceKey, err := base.AuthCheckAndGetInstanceKey(params, r, req, user, true)
	if instanceKey == nil {
		return
	}

	// get remote instance info
	instance, err := hinstance.GetInfoFromInstance(ctx, instanceKey, params["agent"], nil)
	if err != nil {
		base.Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, err.Error(), ""))
		return
	}

	// put instance key to discover queue
	if orcraft.IsRaftEnabled() {
		orcraft.PublishCommand(constant.RaftCommandDiscover, instanceKey)
	} else {
		discover.DiscoverInstance(ctx, *instanceKey)
	}

	// response this
	base.Respond(r, dtstruct.NewApiResponse(dtstruct.OK, fmt.Sprintf("instance discovered: %+v", instanceKey), instance))
}

// Check issues a synchronous read on an instance
func Check(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	span, ctx := opentracing.StartSpanFromContext(context.TODO(), "Check")
	defer span.Finish()

	// check and generate instance key
	instanceKey, err := base.AuthCheckAndGetInstanceKey(params, r, req, user, true)
	if instanceKey == nil {
		return
	}

	// get remote instance info
	instance, err := hinstance.CheckInstance(ctx, instanceKey, params["agent"], nil)
	if err != nil {
		base.Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, err.Error(), ""))
		return
	}

	// put instance key to discover queue
	//if orcraft.IsRaftEnabled() {
	//	orcraft.PublishCommand(constant.RaftCommandDiscover, instanceKey)
	//} else {
	//	logic.DiscoverInstance(*instanceKey)
	//}

	// response this
	base.Respond(r, dtstruct.NewApiResponse(dtstruct.OK, fmt.Sprintf("instance discovered: %+v", instanceKey), instance))
}

// Forget removes an instance entry fro backend database
func Forget(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	// check and generate instance key
	instanceKey, err := base.AuthCheckAndGetInstanceKeyWithClusterId(params, r, req, user, true)
	if instanceKey == nil {
		return
	}

	// forget instance by instance key
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand(constant.RaftCommandForget, instanceKey)
	} else {
		err = hinstance.ForgetInstance(instanceKey)
	}
	if err != nil {
		base.Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, err.Error(), nil))
		return
	}

	// response this
	base.Respond(r, dtstruct.NewApiResponse(dtstruct.OK, fmt.Sprintf("instance forgotten: %+v", instanceKey), instanceKey))
}

// Topology returns an graph of cluster's instances
func Topology(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	asciiTopology(params, r, req, user, false, false)
}

// TopologyTabulated returns an graph of cluster's instances
func TopologyTabulated(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	asciiTopology(params, r, req, user, true, false)
}

// TopologyWithTag returns an graph of cluster's instances and instance tags
func TopologyWithTag(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	asciiTopology(params, r, req, user, false, true)
}

// asciiTopology returns an graph of cluster's instances
func asciiTopology(params martini.Params, r render.Render, req *http.Request, user auth.User, tabulated bool, printTags bool) {

	var request *dtstruct.Request

	// auth check
	if request, _ = base.AuthCheckAndGetRequest(params, r, req, user); request == nil {
		return
	}

	// get topology
	tplgyInfo, err := topology.Topology(request, "", tabulated, printTags)
	if err != nil {
		base.Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, fmt.Sprintf("%+v", err), ""))
		return
	}

	// response it
	base.Respond(r, dtstruct.NewApiResponse(dtstruct.OK, fmt.Sprintf("topology for cluster %s", request), tplgyInfo))
}

// ReplicationAnalysis return list of issues
func ReplicationAnalysis(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	replicationAnalysis("", nil, params, r, req, user)
}

// replicationAnalysis return list of issues for cluster or instance
func replicationAnalysis(clusterName string, instanceKey *dtstruct.InstanceKey, params martini.Params, r render.Render, req *http.Request, user auth.User) {

	var request *dtstruct.Request

	// auth check
	if request, _ = base.AuthCheckAndGetRequest(params, r, req, user); request == nil {
		return
	}

	// get database type and check
	if instanceKey != nil {
		request.DBType = instanceKey.DBType
	}
	if !dtstruct.IsTypeValid(request.DBType) {
		base.Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, fmt.Sprintf("database type:%s is invalid", request.DBType), nil))
	}

	// get all cluster replication analysis
	analysis, err := replication.GetReplicationAnalysis(request.DBType, clusterName, params["clusterId"], &dtstruct.ReplicationAnalysisHints{IncludeDowntimed: true, IncludeNoProblem: true})
	if err != nil {
		base.Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, fmt.Sprintf("cannot get analysis: %+v", err), nil))
		return
	}

	// possibly filter single instance
	if instanceKey != nil {
		filtered := analysis[:0]
		for _, analysisEntry := range analysis {
			if instanceKey.Equals(&analysisEntry.AnalyzedInstanceKey) {
				filtered = append(filtered, analysisEntry)
			}
		}
		analysis = filtered
	}

	// response it
	base.Respond(r, dtstruct.NewApiResponse(dtstruct.OK, fmt.Sprintf("analysis for database type:%s", request.DBType), analysis))
}

// InfoSync returns sync info for node
func InfoSync(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	// check and generate instance key
	instanceKey, err := base.AuthCheckAndGetInstanceKeyWithClusterId(params, r, req, user, true)
	if instanceKey == nil {
		return
	}

	// get topology
	syncInfo, err := hinstance.GetInfoSync(instanceKey, "")
	if err != nil {
		base.Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, fmt.Sprintf("%+v", err), ""))
		return
	}

	// response it
	base.Respond(r, dtstruct.NewApiResponse(dtstruct.OK, fmt.Sprintf("sync info for instance %s", instanceKey), struct{ Percent int }{syncInfo.(int)}))
}

//TODO----------------------

// TopologySnapshot triggers ham4db to record a snapshot of host/master for all known hosts.
func TopologySnapshot(params martini.Params, r render.Render, req *http.Request) {
	start := time.Now()
	if err := base.SnapshotTopologies(); err != nil {
		base.Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, fmt.Sprintf("%+v", err), fmt.Sprintf("took %v", time.Since(start))))
		return
	}

	base.Respond(r, dtstruct.NewApiResponse(dtstruct.OK, "topology Snapshot completed", fmt.Sprintf("took %v", time.Since(start))))
}

// ReplicationAnalysis retuens list of issues
func ReplicationAnalysisForCluster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	clusterName := params["clusterName"]

	var err error
	if clusterName, err = base.DeduceClusterName(params["clusterName"]); err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot get analysis: %+v", err)})
		return
	}
	if clusterName == "" {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot get cluster name: %+v", params["clusterName"])})
		return
	}
	replicationAnalysis(clusterName, nil, params, r, req, user)
}

// ReplicationAnalysis return list of issues
func ReplicationAnalysisForKey(params martini.Params, r render.Render, req *http.Request, user auth.User) {

	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot get analysis: %+v", err)})
		return
	}
	if !instanceKey.IsValid() {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot get analysis: invalid key %+v", instanceKey)})
		return
	}
	replicationAnalysis("", &instanceKey, params, r, req, user)
}

func getTag(params martini.Params, req *http.Request) (tag *dtstruct.Tag, err error) {
	tagString := req.URL.Query().Get("tag")
	if tagString != "" {
		return dtstruct.ParseTag(tagString)
	}
	return dtstruct.NewTag(params["tagName"], params["tagValue"])
}

// InstanceReplicas lists all replicas of given instance
func InstanceReplicas(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	// TODO why need database type
	replicas, err := hinstance.ReadInstanceDownStream("", &instanceKey)

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", instanceKey)})
		return
	}
	r.JSON(http.StatusOK, replicas)
}

// Instance reads and returns an instance's details.
func Instance(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, found, err := hinstance.ReadInstance(&instanceKey)
	if (!found) || (err != nil) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", instanceKey)})
		return
	}
	r.JSON(http.StatusOK, instance)
}

// AsyncDiscover issues an asynchronous read on an instance. This is
// useful for bulk loads of a new set of instances and will not block
// if the instance is slow to respond or not reachable.
func AsyncDiscover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	go Discover(params, r, req, user)

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Asynchronous discovery initiated for Instance: %+v", instanceKey)})
}

// Refresh synchronuously re-reads a topology instance
func Refresh(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	_, err = recovery.RefreshTopologyInstance(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Instance refreshed: %+v", instanceKey), Detail: instanceKey})
}

// ForgetCluster forgets all instacnes of a cluster
func ForgetCluster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	clusterName, err := base.FigureClusterNameByHint(util.GetClusterHint(params))
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	if orcraft.IsRaftEnabled() {
		orcraft.PublishCommand("forget-cluster", clusterName)
	} else {
		// TODO why need database type
		hinstance.ForgetClusterInstance("", clusterName)
	}
	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Cluster forgotten: %+v", clusterName)})
}

// Resolve tries to resolve hostname and then checks to see if port is open on that host.
func Resolve(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	if conn, err := net.Dial("tcp", instanceKey.DisplayString()); err == nil {
		conn.Close()
	} else {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: "Instance resolved", Detail: instanceKey})
}

// BeginMaintenance begins maintenance mode for given instance
func BeginMaintenance(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	key, err := base.BeginBoundedMaintenance(&instanceKey, params["owner"], params["reason"], 0, true)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error(), Detail: key})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Maintenance begun: %+v", instanceKey), Detail: instanceKey})
}

// EndMaintenance terminates maintenance mode
func EndMaintenance(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	maintenanceKey, err := strconv.ParseInt(params["maintenanceKey"], 10, 0)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	_, err = base.EndMaintenance(maintenanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Maintenance ended: %+v", maintenanceKey), Detail: maintenanceKey})
}

// EndMaintenanceByInstanceKey terminates maintenance mode for given instance
func EndMaintenanceByInstanceKey(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	_, err = base.EndMaintenanceByInstanceKey(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Maintenance ended: %+v", instanceKey), Detail: instanceKey})
}

// EndMaintenanceByInstanceKey terminates maintenance mode for given instance
func InMaintenance(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	inMaintenance, err := base.InMaintenance(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	responseDetails := ""
	if inMaintenance {
		responseDetails = instanceKey.StringCode()
	}
	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("%+v", inMaintenance), Detail: responseDetails})
}

// Maintenance provides list of instance under active maintenance
func Maintenance(params martini.Params, r render.Render, req *http.Request) {
	maintenanceList, err := base.ReadActiveMaintenance()

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, maintenanceList)
}

// BeginDowntime sets a downtime flag with default duration
func BeginDowntime(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	var durationSeconds int = 0
	if params["duration"] != "" {
		durationSeconds, err = goutil.SimpleTimeToSeconds(params["duration"])
		if durationSeconds < 0 {
			err = fmt.Errorf("Duration value must be non-negative. Given value: %d", durationSeconds)
		}
		if err != nil {
			base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
			return
		}
	}
	duration := time.Duration(durationSeconds) * time.Second
	dwntime := dtstruct.NewDowntime(&instanceKey, params["owner"], params["reason"], duration)
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("begin-downtime", dwntime)
	} else {
		err = base.BeginDowntime(dwntime)
	}

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error(), Detail: instanceKey})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Downtime begun: %+v", instanceKey), Detail: instanceKey})
}

// EndDowntime terminates downtime (removes downtime flag) for an instance
func EndDowntime(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("end-downtime", instanceKey)
	} else {
		_, err = base.EndDowntime(&instanceKey)
	}
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Downtime ended: %+v", instanceKey), Detail: instanceKey})
}

// MoveUp attempts to move an instance up the topology
func MoveUp(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := topology.MoveUp(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Instance %+v moved up", instanceKey), Detail: instance})
}

// MoveUpReplicas attempts to move up all replicas of an instance
func MoveUpReplicas(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	replicas, newMaster, err, errs := topology.MoveUpReplicas(&instanceKey, req.URL.Query().Get("pattern"))
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Moved up %d replicas of %+v below %+v; %d errors: %+v", len(replicas), instanceKey, newMaster.GetInstance().Key, len(errs), errs), Detail: replicas})
}

// MoveUpReplicas attempts to move up all replicas of an instance
func RepointReplicas(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	replicas, err, _ := topology.RepointReplicasTo(&instanceKey, req.URL.Query().Get("pattern"), nil)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Repointed %d replicas of %+v", len(replicas), instanceKey), Detail: replicas})
}

// MakeCoMaster attempts to make an instance co-master with its own master
func MakeCoMaster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := topology.MakeCoMaster(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Instance made co-master: %+v", instance.GetInstance().Key), Detail: instance})
}

// ResetReplication makes a replica forget about its master, effectively breaking the replication
func ResetReplication(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	instanceKey.ClusterId = params["clusterId"]
	instance, err := replication.ResetReplication(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Replica reset on %+v", instance.GetInstance().Key), Detail: instance})
}

// DetachReplicaMasterHost detaches a replica from its master by setting an invalid
// (yet revertible) host name
func DetachReplicaMasterHost(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := topology.DetachMaster(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Replica detached: %+v", instance.GetInstance().Key), Detail: instance})
}

// ReattachReplicaMasterHost reverts a detachReplicaMasterHost command
// by resoting the original master hostname in CHANGE MASTER TO
func ReattachReplicaMasterHost(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := topology.ReattachMaster(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Replica reattached: %+v", instance.GetInstance().Key), Detail: instance})
}

// MoveBelow attempts to move an instance below its supposed sibling
func MoveBelow(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	siblingKey, err := base.GetInstanceKey(params["type"], params["siblingHost"], params["siblingPort"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	instance, err := topology.MoveBelow(&instanceKey, &siblingKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Instance %+v moved below %+v", instanceKey, siblingKey), Detail: instance})
}

// TakeSiblings
func TakeSiblings(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	instance, count, err := topology.TakeSiblings(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Took %d siblings of %+v", count, instanceKey), Detail: instance})
}

// TakeMaster
func TakeMaster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	instance, err := topology.TakeMaster(&instanceKey, false)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("%+v took its master", instanceKey), Detail: instance})
}

// RelocateBelow attempts to move an instance below another, ham4db choosing the best (potentially multi-step)
// relocation method
func RelocateBelow(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	belowKey, err := base.GetInstanceKey(params["type"], params["belowHost"], params["belowPort"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	instance, err := topology.RelocateBelow(&instanceKey, &belowKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Instance %+v relocated below %+v", instanceKey, belowKey), Detail: instance})
}

// Relocates attempts to smartly relocate replicas of a given instance below another
func RelocateReplicas(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	belowKey, err := base.GetInstanceKey(params["type"], params["belowHost"], params["belowPort"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	replicas, _, err, errs := topology.RelocateReplicas(&instanceKey, &belowKey, req.URL.Query().Get("pattern"))
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Relocated %d replicas of %+v below %+v; %d errors: %+v", len(replicas), instanceKey, belowKey, len(errs), errs), Detail: replicas})
}

// MoveEquivalent attempts to move an instance below another, baseed on known equivalence master coordinates
func MoveEquivalent(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	belowKey, err := base.GetInstanceKey(params["type"], params["belowHost"], params["belowPort"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	instance, err := topology.MoveEquivalent(&instanceKey, &belowKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Instance %+v relocated via equivalence coordinates below %+v", instanceKey, belowKey), Detail: instance})
}

// MatchBelow attempts to move an instance below another via pseudo GTID matching of binlog entries
func MatchBelow(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	belowKey, err := base.GetInstanceKey(params["type"], params["belowHost"], params["belowPort"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	instance, matchedCoordinates, err := topology.MatchBelow(&instanceKey, &belowKey, true)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Instance %+v matched below %+v at %+v", instanceKey, belowKey, *matchedCoordinates), Detail: instance})
}

// MatchBelow attempts to move an instance below another via pseudo GTID matching of binlog entries
func MatchUp(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	instance, matchedCoordinates, err := topology.MatchUp(&instanceKey, true)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Instance %+v matched up at %+v", instanceKey, *matchedCoordinates), Detail: instance})
}

// MultiMatchReplicas attempts to match all replicas of a given instance below another, efficiently
func MultiMatchReplicas(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	belowKey, err := base.GetInstanceKey(params["type"], params["belowHost"], params["belowPort"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	replicas, newMaster, err, errs := topology.MultiMatchReplicas(&instanceKey, &belowKey, req.URL.Query().Get("pattern"))
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Matched %d replicas of %+v below %+v; %d errors: %+v", len(replicas), instanceKey, newMaster.GetInstance().Key, len(errs), errs), Detail: newMaster.GetInstance().Key})
}

// MatchUpReplicas attempts to match up all replicas of an instance
func MatchUpReplicas(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	replicas, newMaster, err, errs := topology.MatchUpReplicas(&instanceKey, req.URL.Query().Get("pattern"))
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Matched up %d replicas of %+v below %+v; %d errors: %+v", len(replicas), instanceKey, newMaster.GetInstance().Key, len(errs), errs), Detail: newMaster.GetInstance().Key})
}

// RegroupReplicas attempts to pick a replica of a given instance and make it take its siblings, using any
// method possible (GTID, Pseudo-GTID, binlog servers)
func RegroupReplicas(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	lostReplicas, equalReplicas, aheadReplicas, cannotReplicateReplicas, promotedReplica, err := topology.RegroupReplicas(&instanceKey, false, nil, nil)
	lostReplicas = append(lostReplicas, cannotReplicateReplicas...)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("promoted replica: %s, lost: %d, trivial: %d, pseudo-gtid: %d",
		promotedReplica.GetInstance().Key.DisplayString(), len(lostReplicas), len(equalReplicas), len(aheadReplicas)), Detail: promotedReplica.GetInstance().Key})
}

// MakeMaster attempts to make the given instance a master, and match its siblings to be its replicas
func MakeMaster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	instance, err := topology.MakeMaster(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Instance %+v now made master", instanceKey), Detail: instance})
}

// MakeLocalMaster attempts to make the given instance a local master: take over its master by
// enslaving its siblings and replicating from its grandparent.
func MakeLocalMaster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	instance, err := topology.MakeLocalMaster(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Instance %+v now made local master", instanceKey), Detail: instance})
}

// SkipQuery skips a single query on a failed replication instance
func SkipQuery(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := maintenance.SkipQuery(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Query skipped on %+v", instance.GetInstance().Key), Detail: instance})
}

// StartReplication starts replication on given instance
func StartReplication(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := replication.StartReplication(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Replica started: %+v", instanceKey), Detail: instance})
}

// RestartReplication stops & starts replication on given instance
func RestartReplication(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := replication.RestartReplication(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Replica restarted: %+v", instanceKey), Detail: instance})
}

// StopReplication stops replication on given instance
func StopReplication(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	instanceKey.ClusterId = params["clusterId"]
	instance, err := replication.StopReplication(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Replica stopped: %+v", instanceKey), Detail: instance})
}

// StopReplicationNicely stops replication on given instance, such that sql thead is aligned with IO thread
func StopReplicationNicely(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	instanceKey.ClusterId = params["clusterId"]
	instance, err := replication.StopReplicationNicely(&instanceKey, 0)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Replica stopped nicely: %+v", instanceKey), Detail: instance})
}

// CanReplicateFrom attempts to move an instance below another via pseudo GTID matching of binlog entries
func CanReplicateFrom(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, found, err := hinstance.ReadInstance(&instanceKey)
	if (!found) || (err != nil) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", instanceKey)})
		return
	}
	belowKey, err := base.GetInstanceKey(params["type"], params["belowHost"], params["belowPort"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	belowInstance, found, err := hinstance.ReadInstance(&belowKey)
	if (!found) || (err != nil) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", belowKey)})
		return
	}

	canReplicate, err := replication.CanReplicateFrom(instance, belowInstance)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("%t", canReplicate), Detail: belowKey})
}

// setSemiSyncMaster
func setSemiSyncMaster(params martini.Params, r render.Render, req *http.Request, user auth.User, enable bool) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := replication.SetSemiSyncOnUpstream(&instanceKey, enable)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("master semi-sync set to %t", enable), Detail: instance})
}

func EnableSemiSyncMaster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	setSemiSyncMaster(params, r, req, user, true)
}
func DisableSemiSyncMaster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	setSemiSyncMaster(params, r, req, user, false)
}

// setSemiSyncMaster
func setSemiSyncReplica(params martini.Params, r render.Render, req *http.Request, user auth.User, enable bool) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := replication.SetSemiSyncOnDownstream(&instanceKey, enable)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("replica semi-sync set to %t", enable), Detail: instance})
}

func EnableSemiSyncReplica(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	setSemiSyncReplica(params, r, req, user, true)
}

func DisableSemiSyncReplica(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	setSemiSyncReplica(params, r, req, user, false)
}

// DelayReplication delays replication on given instance with given seconds
func DelayReplication(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	seconds, err := strconv.Atoi(params["seconds"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Invalid value provided for seconds"})
		return
	}
	err = replication.DelayReplication(&instanceKey, seconds)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Replication delayed: %+v", instanceKey), Detail: seconds})
}

// SetReadOnly sets the global read_only variable
func SetReadOnly(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := replication.SetReadOnly(&instanceKey, true)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: "Server set as read-only", Detail: instance})
}

// SetWriteable clear the global read_only variable
func SetWriteable(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := replication.SetReadOnly(&instanceKey, false)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: "Server set as writeable", Detail: instance})
}

// KillQuery kills a query running on a server
func KillQuery(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	processId, err := strconv.ParseInt(params["process"], 10, 0)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := maintenance.KillQuery(&instanceKey, processId)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Query killed on : %+v", instance.GetInstance().Key), Detail: instance})
}

// Cluster provides list of instances in given cluster
func Cluster(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := base.FigureClusterNameByHint(util.GetClusterHint(params))
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	// TODO why need database type
	instances, err := hinstance.ReadClusterInstance(params["type"], clusterName)

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}

// ClusterByAlias provides list of instances in given cluster
func ClusterByAlias(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := base.ReadClusterNameByAlias(params["clusterAlias"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	params["clusterName"] = clusterName
	Cluster(params, r, req)
}

// ClusterByInstance provides list of instances in cluster an instance belongs to
func ClusterByInstance(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, found, err := hinstance.ReadInstance(&instanceKey)
	if (!found) || (err != nil) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", instanceKey)})
		return
	}

	params["clusterName"] = instance.GetInstance().ClusterName
	Cluster(params, r, req)
}

// ClusterInfo provides details of a given cluster
func ClusterInfo(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := base.FigureClusterNameByHint(util.GetClusterHint(params))
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	clusterInfo, err := base.ReadClusterInfo(params["type"], clusterName)

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, clusterInfo)
}

// Cluster provides list of instances in given cluster
func ClusterInfoByAlias(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := base.ReadClusterNameByAlias(params["clusterAlias"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	params["clusterName"] = clusterName
	ClusterInfo(params, r, req)
}

// SetClusterAlias will change an alias for a given clustername
func SetClusterAliasManualOverride(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	clusterName := params["clusterName"]
	alias := req.URL.Query().Get("alias")

	var err error
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("set-cluster-alias-manual-override", []string{clusterName, alias})
	} else {
		err = base.WriteClusterAliasManualOverride(clusterName, alias)
	}

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Cluster %s now has alias '%s'", clusterName, alias)})
}

// Clusters provides list of known clusters
func Clusters(params martini.Params, r render.Render, req *http.Request) {
	clusterNames, err := base.ReadClusters("")

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, clusterNames)
}

// ClustersInfo provides list of known clusters, along with some added metadata per cluster
func ClustersInfo(params martini.Params, r render.Render, req *http.Request) {
	clustersInfo, err := base.ReadAllClusterInfo(params["type"], "")

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, clustersInfo)
}

// Tags lists existing tags for a given instance
func Tags(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	tags, err := base.ReadInstanceTags(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	tagStrings := []string{}
	for _, tg := range tags {
		tagStrings = append(tagStrings, tg.String())
	}
	r.JSON(http.StatusOK, tagStrings)
}

// TagValue returns a given tag's value for a specific instance
func TagValue(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	tg, err := getTag(params, req)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	tagExists, err := base.ReadInstanceTag(&instanceKey, tg)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	if tagExists {
		r.JSON(http.StatusOK, tg.TagValue)
	} else {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("tag %s not found for %+v", tg.TagName, instanceKey)})
	}
}

// Tagged return instance keys tagged by "tag" query param
func Tagged(params martini.Params, r render.Render, req *http.Request) {
	tagsString := req.URL.Query().Get("tag")
	instanceKeyMap, err := base.GetInstanceKeysByTags(tagsString)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	r.JSON(http.StatusOK, instanceKeyMap.GetInstanceKeys())
}

// Tags adds a tag to a given instance
func Tag(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	tg, err := getTag(params, req)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("put-instance-tag", dtstruct.InstanceTag{Key: instanceKey, T: *tg})
	} else {
		err = base.PutInstanceTag(&instanceKey, tg)
	}
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("%+v tagged with %s", instanceKey, tg.String()), Detail: instanceKey})
}

// Untag removes a tag from an instance
func Untag(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	tg, err := getTag(params, req)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	untagged, err := base.Untag(&instanceKey, tg)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("%s removed from %+v instances", tg.TagName, len(*untagged)), Detail: untagged.GetInstanceKeys()})
}

// UntagAll removes a tag from all matching instances
func UntagAll(params martini.Params, r render.Render, req *http.Request) {
	tg, err := getTag(params, req)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	untagged, err := base.Untag(nil, tg)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("%s removed from %+v instances", tg.TagName, len(*untagged)), Detail: untagged.GetInstanceKeys()})
}

// Write a cluster's master (or all clusters masters) to kv stores.
// This should generally only happen once in a lifetime of a cluster. Otherwise KV
// stores are updated via failovers.
func SubmitMastersToKvStores(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := getClusterNameIfExists(params)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	kvPairs, submittedCount, err := logic.SubmitMastersToKvStores(clusterName, true)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Submitted %d masters", submittedCount), Detail: kvPairs})
}

// Clusters provides list of known masters
func Masters(params martini.Params, r render.Render, req *http.Request) {

	// TODO why need database type
	instances, err := hinstance.ReadMasterWriteable()

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}

// ClusterMaster returns the writable master of a given cluster
func ClusterMaster(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := base.FigureClusterNameByHint(util.GetClusterHint(params))
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	// TODO why need database type
	masters, err := hinstance.ReadClusterMaster("", clusterName)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	if len(masters) == 0 {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("No masters found for %+v", clusterName)})
		return
	}

	r.JSON(http.StatusOK, masters[0])
}

// Downtimed lists downtimed instances, potentially filtered by cluster
func Downtimed(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := getClusterNameIfExists(params)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	// TODO why need database type
	instances, err := hinstance.ReadInstanceDowntime("", clusterName)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}

// AllInstances lists all known instances
func AllInstances(params martini.Params, r render.Render, req *http.Request) {

	// TODO why need database type
	instances, err := hinstance.SearchInstance("", "")

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}

// Search provides list of instances matching given search param via various criteria.
func Search(params martini.Params, r render.Render, req *http.Request) {
	searchString := params["searchString"]
	if searchString == "" {
		searchString = req.URL.Query().Get("s")
	}

	// TODO why need database type
	instances, err := hinstance.SearchInstance(params["type"], searchString)

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}

// Problems provides list of instances with known problems
func Problems(params martini.Params, r render.Render, req *http.Request) {
	clusterName := params["clusterName"]
	instances, err := hinstance.ReadInstanceProblem(params["type"], clusterName)

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}

// Audit provides list of audit entries by given page number
func Audit(params martini.Params, r render.Render, req *http.Request) {
	page, err := strconv.Atoi(params["page"])
	if err != nil || page < 0 {
		page = 0
	}
	var auditedInstanceKey *dtstruct.InstanceKey
	if instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"]); err == nil {
		auditedInstanceKey = &instanceKey
	}

	audits, err := base.ReadRecentAudit(auditedInstanceKey, page)

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, audits)
}

// HostnameResolveCache shows content of in-memory hostname cache
func HostnameResolveCache(params martini.Params, r render.Render, req *http.Request) {
	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: "Cache retrieved", Detail: cache.ItemHostname()})
}

// ResetHostnameResolveCache clears in-memory hostname resovle cache
func ResetHostnameResolveCache(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	err := base.ResetHostnameResolveCache()

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: "Hostname cache cleared"})
}

// DeregisterHostnameUnresolve deregisters the unresolve name used previously
func DeregisterHostnameUnresolve(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}

	var instanceKey *dtstruct.InstanceKey
	if instKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"]); err == nil {
		instanceKey = &instKey
	}

	var err error
	registration := dtstruct.NewHostnameDeregistration(instanceKey)
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("register-hostname-unresolve", registration)
	} else {
		err = base.RegisterHostnameUnresolve(registration)
	}
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: "Hostname deregister unresolve completed", Detail: instanceKey})
}

// RegisterHostnameUnresolve registers the unresolve name to use
func RegisterHostnameUnresolve(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}

	var instanceKey *dtstruct.InstanceKey
	if instKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"]); err == nil {
		instanceKey = &instKey
	}

	hostname := params["virtualname"]
	var err error
	registration := dtstruct.NewHostnameRegistration(instanceKey, hostname)
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("register-hostname-unresolve", registration)
	} else {
		err = base.RegisterHostnameUnresolve(registration)
	}
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: "Hostname register unresolve completed", Detail: instanceKey})
}

// SubmitPoolInstances (re-)applies the list of hostnames for a given pool
func SubmitPoolInstances(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	pool := params["pool"]
	instances := req.URL.Query().Get("instances")

	var err error
	submission := dtstruct.NewPoolInstancesSubmission(params["type"], pool, instances)
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("submit-pool-instances", submission)
	} else {
		err = base.ApplyPoolInstance(submission)
	}
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Applied %s pool instances", pool), Detail: pool})
}

// SubmitPoolHostnames (re-)applies the list of hostnames for a given pool
func ReadClusterPoolInstancesMap(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	clusterName := params["clusterName"]
	pool := params["pool"]

	poolInstancesMap, err := base.ReadClusterPoolInstancesMap(clusterName, pool)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Read pool instances for cluster %s", clusterName), Detail: poolInstancesMap})
}

// GetHeuristicClusterPoolInstances returns instances belonging to a cluster's pool
func GetHeuristicClusterPoolInstances(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	clusterName, err := base.FigureClusterNameByHint(util.GetClusterHint(params))
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	pool := params["pool"]

	// TODO why need database type
	instances, err := hinstance.GetHeuristicClusterPoolInstance("", clusterName, pool)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Heuristic pool instances for cluster %s", clusterName), Detail: instances})
}

// GetHeuristicClusterPoolInstances returns instances belonging to a cluster's pool
func GetHeuristicClusterPoolInstancesLag(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	clusterName, err := base.ReadClusterNameByAlias(params["clusterName"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	pool := params["pool"]

	// TODO why need database type
	lag, err := hinstance.GetHeuristicClusterPoolInstanceLag("", clusterName, pool)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Heuristic pool lag for cluster %s", clusterName), Detail: lag})
}

// ReloadClusterAlias clears in-memory hostname resovle cache
func ReloadClusterAlias(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "This API call has been retired"})
}

// BulkPromotionRules returns a list of the known promotion rules for each instance
func BulkPromotionRules(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}

	promotionRules, err := base.BulkReadCandidateDatabaseInstance()
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, promotionRules)
}

// BulkInstances returns a list of all known instances
func BulkInstances(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}

	// TODO why need database type
	instances, err := hinstance.BulkReadInstance("")
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}

// DiscoveryMetricsRaw will return the last X seconds worth of discovery information in time based order as a JSON array
func DiscoveryMetricsRaw(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, err := strconv.Atoi(params["seconds"])
	if err != nil || seconds <= 0 {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Invalid value provided for seconds"})
		return
	}

	refTime := time.Now().Add(-time.Duration(seconds) * time.Second)
	json, err := discoveryMetrics.Since(refTime)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unable to determine start time. Perhaps seconds value is wrong?"})
		return
	}
	log.Debugf("DiscoveryMetricsRaw data: retrieved %d entries from discovery.MC", len(json))

	r.JSON(http.StatusOK, json)
}

// DiscoveryMetricsAggregated will return a single set of aggregated metric for raw values collected since the
// specified time.
func DiscoveryMetricsAggregated(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, err := strconv.Atoi(params["seconds"])

	refTime := time.Now().Add(-time.Duration(seconds) * time.Second)
	aggregated, err := metric.AggregatedDiscoverSince(discoveryMetrics, refTime)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unable to generate aggregated discovery metric"})
		return
	}
	// log.Debugf("DiscoveryMetricsAggregated data: %+v", aggregated)
	r.JSON(http.StatusOK, aggregated)
}

// DiscoveryQueueMetricsRaw returns the raw queue metric (active and
// queued values), data taken secondly for the last N seconds.
func DiscoveryQueueMetricsRaw(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, err := strconv.Atoi(params["seconds"])
	log.Debugf("DiscoveryQueueMetricsRaw: seconds: %d", seconds)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unable to generate discovery queue  aggregated metric"})
		return
	}

	queue := dtstruct.CreateOrReturnQueue("DEFAULT", config.Config.DiscoveryQueueCapacity, config.Config.DiscoveryQueueMaxStatisticsSize, config.Config.InstancePollSeconds)
	metrics := queue.DiscoveryQueueMetrics(seconds)
	log.Debugf("DiscoveryQueueMetricsRaw data: %+v", metrics)

	r.JSON(http.StatusOK, metrics)
}

// DiscoveryQueueMetricsAggregated returns a single value showing the metric of the discovery queue over the last N seconds.
// This is expected to be called every 60 seconds (?) and the config setting of the retention period is currently hard-coded.
// See go/discovery/ for more information.
func DiscoveryQueueMetricsAggregated(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, err := strconv.Atoi(params["seconds"])
	log.Debugf("DiscoveryQueueMetricsAggregated: seconds: %d", seconds)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unable to generate discovery queue aggregated metric"})
		return
	}

	queue := dtstruct.CreateOrReturnQueue("DEFAULT", config.Config.DiscoveryQueueCapacity, config.Config.DiscoveryQueueMaxStatisticsSize, config.Config.InstancePollSeconds)
	aggregated := queue.AggregatedDiscoveryQueueMetrics(seconds)
	log.Debugf("DiscoveryQueueMetricsAggregated data: %+v", aggregated)

	r.JSON(http.StatusOK, aggregated)
}

// BackendQueryMetricsRaw returns the raw backend query metric
func BackendQueryMetricsRaw(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, err := strconv.Atoi(params["seconds"])
	log.Debugf("BackendQueryMetricsRaw: seconds: %d", seconds)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unable to generate raw backend query metric"})
		return
	}

	refTime := time.Now().Add(-time.Duration(seconds) * time.Second)
	m, err := queryMetrics.Since(refTime)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unable to return backend query metric"})
		return
	}

	log.Debugf("BackendQueryMetricsRaw data: %+v", m)

	r.JSON(http.StatusOK, m)
}

func BackendQueryMetricsAggregated(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, err := strconv.Atoi(params["seconds"])
	log.Debugf("BackendQueryMetricsAggregated: seconds: %d", seconds)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unable to aggregated generate backend query metric"})
		return
	}

	refTime := time.Now().Add(-time.Duration(seconds) * time.Second)
	aggregated := metric.AggregatedQuerySince(queryMetrics, refTime)
	log.Debugf("BackendQueryMetricsAggregated data: %+v", aggregated)

	r.JSON(http.StatusOK, aggregated)
}

// WriteBufferMetricsRaw returns the raw instance write buffer metric
func WriteBufferMetricsRaw(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, err := strconv.Atoi(params["seconds"])
	log.Debugf("WriteBufferMetricsRaw: seconds: %d", seconds)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unable to generate raw instance write buffer metric"})
		return
	}

	refTime := time.Now().Add(-time.Duration(seconds) * time.Second)
	m, err := writeBufferMetrics.Since(refTime)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unable to return instance write buffermetrics"})
		return
	}

	log.Debugf("WriteBufferMetricsRaw data: %+v", m)

	r.JSON(http.StatusOK, m)
}

// WriteBufferMetricsAggregated provides aggregate metric of instance write buffer metric
func WriteBufferMetricsAggregated(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, err := strconv.Atoi(params["seconds"])
	log.Debugf("WriteBufferMetricsAggregated: seconds: %d", seconds)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unable to aggregated instance write buffer metric"})
		return
	}

	refTime := time.Now().Add(-time.Duration(seconds) * time.Second)
	aggregated := dtstruct.AggregatedSince(writeBufferMetrics, refTime)
	log.Debugf("WriteBufferMetricsAggregated data: %+v", aggregated)

	r.JSON(http.StatusOK, aggregated)
}

// Agent returns complete information of a given agent
func AgentHealthCheck(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}

	err := agent.HealthCheck(params["ip"], params["hostname"], params["port"], params["interval"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, time.Now())
}

//// Agents provides complete list of registered agents (See https://gitee.com/opengauss/ham4db-agent)
//func  Agents(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	agents, err := agent.ReadAgents()
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, agents)
//}

//// Agent returns complete information of a given agent
//func  Agent(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	agent, err := agent.GetAgent(params["host"])
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, agent)
//}

//// AgentUnmount instructs an agent to unmount the designated mount point
//func  AgentUnmount(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	output, err := agent.Unmount(params["host"])
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, output)
//}
//
//// AgentMountLV instructs an agent to mount a given volume on the designated mount point
//func  AgentMountLV(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	output, err := agent.MountLV(params["host"], req.URL.Query().Get("lv"))
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, output)
//}

//// AgentCreateSnapshot instructs an agent to create a new snapshot. Agent's DIY implementation.
//func  AgentCreateSnapshot(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	output, err := agent.CreateSnapshot(params["host"])
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, output)
//}
//
//// AgentRemoveLV instructs an agent to remove a logical volume
//func  AgentRemoveLV(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	output, err := agent.RemoveLV(params["host"], req.URL.Query().Get("lv"))
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, output)
//}

//// AgentMySQLStop stops MySQL service on agent
//func  AgentMySQLStop(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	output, err := agent.MySQLStop(params["host"])
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, output)
//}

//// AgentMySQLStart starts MySQL service on agent
//func  AgentMySQLStart(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	output, err := agent.MySQLStart(params["host"])
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, output)
//}

//func  AgentCustomCommand(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	output, err := agent.CustomCommand(params["host"], params["command"])
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, output)
//}
//
//// AgentSeed completely seeds a host with another host's snapshots. This is a complex operation
//// governed by ham4db and executed by the two agents involved.
//func  AgentSeed(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	output, err := agent.Seed(params["targetHost"], params["sourceHost"])
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, output)
//}
//
//// AgentActiveSeeds lists active seeds and their state
//func  AgentActiveSeeds(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	output, err := agent.ReadActiveSeedsForHost(params["host"])
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, output)
//}
//
//// AgentRecentSeeds lists recent seeds of a given agent
//func  AgentRecentSeeds(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	output, err := agent.ReadRecentCompletedSeedsForHost(params["host"])
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, output)
//}
//
//// AgentSeedDetails provides details of a given seed
//func  AgentSeedDetails(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	seedId, err := strconv.ParseInt(params["seedId"], 10, 0)
//	output, err := agent.AgentSeedDetails(seedId)
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, output)
//}
//
//// AgentSeedStates returns the breakdown of states (steps) of a given seed
//func  AgentSeedStates(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	seedId, err := strconv.ParseInt(params["seedId"], 10, 0)
//	output, err := agent.ReadSeedStates(seedId)
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, output)
//}

//// Seeds retruns all recent seeds
//func  Seeds(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	output, err := agent.ReadRecentSeeds()
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, output)
//}

//// AbortSeed instructs agents to abort an active seed
//func  AbortSeed(params martini.Params, r render.Render, req *http.Request, user auth.User) {
//	if !base.IsAuthorizedForAction(req, user) {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
//		return
//	}
//	if !config.Config.ServeAgentsHttp {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Agents not served"})
//		return
//	}
//
//	seedId, err := strconv.ParseInt(params["seedId"], 10, 0)
//	err = agent.AbortSeed(seedId)
//
//	if err != nil {
//		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
//		return
//	}
//
//	r.JSON(http.StatusOK, err == nil)
//}

// Headers is a self-test call which returns HTTP headers
func Headers(params martini.Params, r render.Render, req *http.Request) {
	r.JSON(http.StatusOK, req.Header)
}

// Health performs a self test
func Health(params martini.Params, r render.Render, req *http.Request) {
	health, err := process.HealthTest()
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Application node is unhealthy %+v", err), Detail: health})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Application node is healthy"), Detail: health})

}

// LBCheck returns a constant respnse, and this can be used by load balancers that expect a given string.
func LBCheck(params martini.Params, r render.Render, req *http.Request) {
	r.JSON(http.StatusOK, "OK")
}

// LBCheck returns a constant respnse, and this can be used by load balancers that expect a given string.
func LeaderCheck(params martini.Params, r render.Render, req *http.Request) {
	respondStatus, err := strconv.Atoi(params["errorStatusCode"])
	if err != nil || respondStatus < 0 {
		respondStatus = http.StatusNotFound
	}

	if base.IsLeader() {
		r.JSON(http.StatusOK, "OK")
	} else {
		r.JSON(respondStatus, "Not leader")
	}
}

// A configurable endpoint that can be for regular status checks or whatever.  While similar to
// Health() this returns 500 on failure.  This will prevent issues for those that have come to
// expect a 200
// It might be a good idea to deprecate the current Health() behavior and roll this in at some
// point
func StatusCheck(params martini.Params, r render.Render, req *http.Request) {
	health, err := process.HealthTest()
	if err != nil {
		r.JSON(500, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Application node is unhealthy %+v", err), Detail: health})
		return
	}
	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Application node is healthy"), Detail: health})
}

// GrabElection forcibly grabs leadership. Use with care!!
func GrabElection(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	err := process.GrabElection()
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Unable to grab election: %+v", err)})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Node elected as leader")})
}

// Reelect causes re-elections for an active node
func Reelect(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	err := process.Reelect()
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Unable to re-elect: %+v", err)})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Set re-elections")})
}

// RaftAddPeer adds a new node to the raft cluster
func RaftAddPeer(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	if !orcraft.IsRaftEnabled() {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "raft-add-peer: not running with raft setup"})
		return
	}
	addr, err := orcraft.AddPeer(params["addr"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot add raft peer: %+v", err)})
		return
	}

	r.JSON(http.StatusOK, addr)
}

// RaftAddPeer removes a node fro the raft cluster
func RaftRemovePeer(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	if !orcraft.IsRaftEnabled() {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "raft-remove-peer: not running with raft setup"})
		return
	}
	addr, err := orcraft.RemovePeer(params["addr"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot remove raft peer: %+v", err)})
		return
	}

	r.JSON(http.StatusOK, addr)
}

// RaftYield yields to a specified host
func RaftYield(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	if !orcraft.IsRaftEnabled() {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "raft-yield: not running with raft setup"})
		return
	}
	orcraft.PublishYield(params["node"])
	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Asynchronously yielded")})
}

// RaftYieldHint yields to a host whose name contains given hint (e.g. DC)
func RaftYieldHint(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	if !orcraft.IsRaftEnabled() {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "raft-yield-hint: not running with raft setup"})
		return
	}
	hint := params["hint"]
	orcraft.PublishYieldHostnameHint(hint)
	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Asynchronously yielded by hint %s", hint), Detail: hint})
}

// RaftPeers returns the list of peers in a raft setup
func RaftPeers(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !orcraft.IsRaftEnabled() {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "raft-nodes: not running with raft setup"})
		return
	}

	peers, err := orcraft.GetPeers()
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot get raft peers: %+v", err)})
		return
	}

	r.JSON(http.StatusOK, peers)
}

// RaftState returns the state of this raft node
func RaftState(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !orcraft.IsRaftEnabled() {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "raft-state: not running with raft setup"})
		return
	}

	state := orcraft.GetState().String()
	r.JSON(http.StatusOK, state)
}

// RaftLeader returns the identify of the leader, if possible
func RaftLeader(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !orcraft.IsRaftEnabled() {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "raft-leader: not running with raft setup"})
		return
	}

	leader := orcraft.GetLeader()
	r.JSON(http.StatusOK, leader)
}

// RaftHealth indicates whether this node is part of a healthy raft group
func RaftHealth(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !orcraft.IsRaftEnabled() {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "raft-state: not running with raft setup"})
		return
	}
	if !orcraft.IsHealthy() {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "unhealthy"})
		return
	}
	r.JSON(http.StatusOK, "healthy")
}

// RaftStatus exports a status summary for a raft node
func RaftStatus(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !orcraft.IsRaftEnabled() {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "raft-state: not running with raft setup"})
		return
	}
	peers, err := orcraft.GetPeers()
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot get raft peers: %+v", err)})
		return
	}

	status := struct {
		RaftBind       string
		RaftAdvertise  string
		State          string
		Healthy        bool
		IsPartOfQuorum bool
		Leader         string
		LeaderURI      string
		Peers          []string
	}{
		RaftBind:       orcraft.GetRaftBind(),
		RaftAdvertise:  orcraft.GetRaftAdvertise(),
		State:          orcraft.GetState().String(),
		Healthy:        orcraft.IsHealthy(),
		IsPartOfQuorum: orcraft.IsPartOfQuorum(),
		Leader:         orcraft.GetLeader(),
		LeaderURI:      orcraft.LeaderURI.Get(),
		Peers:          peers,
	}
	r.JSON(http.StatusOK, status)
}

// RaftFollowerHealthReport is initiated by followers to report their identity and health to the raft leader.
func RaftFollowerHealthReport(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !orcraft.IsRaftEnabled() {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "raft-state: not running with raft setup"})
		return
	}
	err := orcraft.OnHealthReport(params["authenticationToken"], params["raftBind"], params["raftAdvertise"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot create snapshot: %+v", err)})
		return
	}
	r.JSON(http.StatusOK, "health reported")
}

// RaftSnapshot instructs raft to take a snapshot
func RaftSnapshot(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !orcraft.IsRaftEnabled() {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "raft-leader: not running with raft setup"})
		return
	}
	err := orcraft.Snapshot()
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot create snapshot: %+v", err)})
		return
	}
	r.JSON(http.StatusOK, "snapshot created")
}

// ReloadConfiguration reloads confiug settings (not all of which will apply after change)
func ReloadConfiguration(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	extraConfigFile := req.URL.Query().Get("config")
	config.Reload(extraConfigFile)
	base.AuditOperation("reload-configuration", nil, "", "Triggered via API")

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Config reloaded"), Detail: extraConfigFile})
}

// RecoverLite attempts recovery on a given instance, without executing external processes
func RecoverLite(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	params["skipProcesses"] = "true"
	Recover(params, r, req, user)
}

// Recover attempts recovery on a given instance
func Recover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	var candidateKey *dtstruct.InstanceKey
	if key, err := base.GetInstanceKey(params["type"], params["candidateHost"], params["candidatePort"]); err == nil {
		candidateKey = &key
	}

	skipProcesses := (req.URL.Query().Get("skipProcesses") == "true") || (params["skipProcesses"] == "true")
	recoveryAttempted, promotedInstanceKey, err := base.CheckAndRecover(dtstruct.GetHamHandler(instanceKey.DBType), &instanceKey, candidateKey, skipProcesses)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error(), Detail: instanceKey})
		return
	}
	if !recoveryAttempted {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Recovery not attempted", Detail: instanceKey})
		return
	}
	if promotedInstanceKey == nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Recovery attempted but no instance promoted", Detail: instanceKey})
		return
	}
	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Recovery executed on %+v", instanceKey), Detail: *promotedInstanceKey})
}

// GracefulMasterTakeover gracefully fails over a master onto its single replica.
func gracefulMasterTakeover(params martini.Params, r render.Render, req *http.Request, user auth.User, auto bool) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, _ := base.GetInstanceKey(params["type"], params["host"], params["port"])
	clusterName, err := base.FigureClusterName("", &instanceKey, nil)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	designatedKey, _ := base.GetInstanceKey(params["type"], params["designatedHost"], params["designatedPort"])

	// designatedKey may be empty/invalid
	topologyRecovery, _, err := recovery.GracefulMasterTakeover(clusterName, &designatedKey, auto)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error(), Detail: topologyRecovery})
		return
	}
	if topologyRecovery == nil || topologyRecovery.SuccessorKey == nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "graceful-master-takeover: no successor promoted", Detail: topologyRecovery})
		return
	}
	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: "graceful-master-takeover: successor promoted", Detail: topologyRecovery})
}

// GracefulMasterTakeover gracefully fails over a master, either:
// - onto its single replica, or
// - onto a replica indicated by the user
func GracefulMasterTakeover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	gracefulMasterTakeover(params, r, req, user, false)
}

// GracefulMasterTakeoverAuto gracefully fails over a master onto a replica of ham4db's choosing
func GracefulMasterTakeoverAuto(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	gracefulMasterTakeover(params, r, req, user, true)
}

// ForceMasterFailover fails over a master (even if there's no particular problem with the master)
func ForceMasterFailover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	clusterName, err := base.FigureClusterNameByHint(util.GetClusterHint(params))
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	topologyRecovery, err := recovery.ForceMasterFailOver(params["type"], clusterName)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	if topologyRecovery.SuccessorKey != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: "Master failed over", Detail: topologyRecovery})
	} else {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Master not failed over", Detail: topologyRecovery})
	}
}

// ForceMasterTakeover fails over a master (even if there's no particular problem with the master)
func ForceMasterTakeover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	clusterName, err := base.FigureClusterNameByHint(util.GetClusterHint(params))
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	designatedKey, _ := base.GetInstanceKey(params["type"], params["designatedHost"], params["designatedPort"])
	designatedInstance, _, err := hinstance.ReadInstance(&designatedKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	if designatedInstance == nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Instance not found"})
		return
	}

	topologyRecovery, err := recovery.ForceMasterTakeover(clusterName, designatedInstance)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	if topologyRecovery.SuccessorKey != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: "Master failed over", Detail: topologyRecovery})
	} else {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Master not failed over", Detail: topologyRecovery})
	}
}

// Registers promotion preference for given instance
func RegisterCandidate(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	promotionRule, err := dtstruct.ParseCandidatePromotionRule(params["promotionRule"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	candidate := base.WithCurrentTime(dtstruct.NewCandidateDatabaseInstance(instanceKey, promotionRule))

	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("register-candidate", candidate)
	} else {
		err = base.RegisterCandidateInstance(candidate)
	}

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: "Registered candidate", Detail: instanceKey})
}

// AutomatedRecoveryFilters retuens list of clusters which are configured with automated recovery
func AutomatedRecoveryFilters(params martini.Params, r render.Render, req *http.Request) {
	automatedRecoveryMap := make(map[string]interface{})
	automatedRecoveryMap["RecoverMasterClusterFilters"] = config.Config.RecoverMasterClusterFilters
	automatedRecoveryMap["RecoverIntermediateMasterClusterFilters"] = config.Config.RecoverIntermediateMasterClusterFilters
	automatedRecoveryMap["RecoveryIgnoreHostnameFilters"] = config.Config.RecoveryIgnoreHostnameFilters

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Automated recovery configuration details"), Detail: automatedRecoveryMap})
}

// AuditFailureDetection provides list of tham_opology_failure_detection entries
func AuditFailureDetection(params martini.Params, r render.Render, req *http.Request) {

	var audits []*dtstruct.TopologyRecovery
	var err error

	if detectionId, derr := strconv.ParseInt(params["id"], 10, 0); derr == nil && detectionId > 0 {
		audits, err = base.ReadFailureDetection(detectionId)
	} else {
		page, derr := strconv.Atoi(params["page"])
		if derr != nil || page < 0 {
			page = 0
		}
		audits, err = base.ReadRecentFailureDetections(params["type"], params["clusterAlias"], page)
	}

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, audits)
}

// AuditRecoverySteps returns audited steps of a given recovery
func AuditRecoverySteps(params martini.Params, r render.Render, req *http.Request) {
	recoveryUID := params["uid"]
	audits, err := base.ReadTopologyRecoverySteps(recoveryUID)

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, audits)
}

// ReadReplicationAnalysisChangelog lists instances and their analysis changelog
func ReadReplicationAnalysisChangelog(params martini.Params, r render.Render, req *http.Request) {
	changelogs, err := base.ReadReplicationAnalysisChangelog()

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, changelogs)
}

// AuditRecovery provides list of topology-recovery entries
func AuditRecovery(params martini.Params, r render.Render, req *http.Request) {
	var audits []*dtstruct.TopologyRecovery
	var err error

	if recoveryUID := params["uid"]; recoveryUID != "" {
		audits, err = base.ReadRecoveryByUID(recoveryUID)
	} else if recoveryId, derr := strconv.ParseInt(params["id"], 10, 0); derr == nil && recoveryId > 0 {
		audits, err = base.ReadRecovery(recoveryId)
	} else {
		page, derr := strconv.Atoi(params["page"])
		if derr != nil || page < 0 {
			page = 0
		}
		unacknowledgedOnly := (req.URL.Query().Get("unacknowledged") == "true")

		audits, err = base.ReadRecentRecoveries(params["type"], params["clusterName"], params["clusterAlias"], unacknowledgedOnly, page)
	}

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, audits)
}

// ActiveClusterRecovery returns recoveries in-progress for a given cluster
func ActiveClusterRecovery(params martini.Params, r render.Render, req *http.Request) {
	recoveries, err := base.ReadActiveClusterRecovery(params["clusterName"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, recoveries)
}

// RecentlyActiveClusterRecovery returns recoveries in-progress for a given cluster
func RecentlyActiveClusterRecovery(params martini.Params, r render.Render, req *http.Request) {
	recoveries, err := base.ReadRecentlyActiveClusterRecovery(params["type"], params["clusterName"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, recoveries)
}

// RecentlyActiveClusterRecovery returns recoveries in-progress for a given cluster
func RecentlyActiveInstanceRecovery(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	recoveries, err := base.ReadRecentlyActiveInstanceRecovery(&instanceKey)

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, recoveries)
}

// ClusterInfo provides details of a given cluster
func AcknowledgeClusterRecoveries(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}

	var clusterName string
	var err error
	if params["clusterAlias"] != "" {
		clusterName, err = base.ReadClusterNameByAlias(params["clusterAlias"])
	} else {
		clusterName, err = base.FigureClusterNameByHint(util.GetClusterHint(params))
	}

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	comment := strings.TrimSpace(req.URL.Query().Get("comment"))
	if comment == "" {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("No acknowledge comment given")})
		return
	}
	userId := getUserId(req, user)
	if userId == "" {
		userId = dtstruct.GetMaintenanceOwner()
	}
	if orcraft.IsRaftEnabled() {
		ack := dtstruct.NewRecoveryAcknowledgement(userId, comment)
		ack.ClusterName = clusterName
		_, err = orcraft.PublishCommand("ack-recovery", ack)
	} else {
		_, err = base.AcknowledgeClusterRecoveries(clusterName, userId, comment)
	}
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Acknowledged cluster recoveries"), Detail: clusterName})
}

// ClusterInfo provides details of a given cluster
func AcknowledgeInstanceRecoveries(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}

	instanceKey, err := base.GetInstanceKey(params["type"], params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	comment := strings.TrimSpace(req.URL.Query().Get("comment"))
	if comment == "" {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("No acknowledge comment given")})
		return
	}
	userId := getUserId(req, user)
	if userId == "" {
		userId = dtstruct.GetMaintenanceOwner()
	}
	if orcraft.IsRaftEnabled() {
		ack := dtstruct.NewRecoveryAcknowledgement(userId, comment)
		ack.Key = instanceKey
		_, err = orcraft.PublishCommand("ack-recovery", ack)
	} else {
		_, err = base.AcknowledgeInstanceRecoveries(&instanceKey, userId, comment)
	}
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Acknowledged instance recoveries"), Detail: instanceKey})
}

// ClusterInfo provides details of a given cluster
func AcknowledgeRecovery(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	var err error
	var recoveryId int64
	var idParam string

	// Ack either via id or uid
	recoveryUid := params["uid"]
	if recoveryUid == "" {
		idParam = params["recoveryId"]
		recoveryId, err = strconv.ParseInt(idParam, 10, 0)
		if err != nil {
			base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
			return
		}
	} else {
		idParam = recoveryUid
	}
	comment := strings.TrimSpace(req.URL.Query().Get("comment"))
	if comment == "" {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("No acknowledge comment given")})
		return
	}
	userId := getUserId(req, user)
	if userId == "" {
		userId = dtstruct.GetMaintenanceOwner()
	}
	if orcraft.IsRaftEnabled() {
		ack := dtstruct.NewRecoveryAcknowledgement(userId, comment)
		ack.Id = recoveryId
		ack.UID = recoveryUid
		_, err = orcraft.PublishCommand("ack-recovery", ack)
	} else {
		if recoveryUid != "" {
			_, err = base.AcknowledgeRecoveryByUID(recoveryUid, userId, comment)
		} else {
			_, err = base.AcknowledgeRecovery(recoveryId, userId, comment)
		}
	}

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Acknowledged recovery"), Detail: idParam})
}

// ClusterInfo provides details of a given cluster
func AcknowledgeAllRecoveries(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}

	comment := strings.TrimSpace(req.URL.Query().Get("comment"))
	if comment == "" {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("No acknowledge comment given")})
		return
	}
	userId := getUserId(req, user)
	if userId == "" {
		userId = dtstruct.GetMaintenanceOwner()
	}
	var err error
	if orcraft.IsRaftEnabled() {
		ack := dtstruct.NewRecoveryAcknowledgement(userId, comment)
		ack.AllRecoveries = true
		_, err = orcraft.PublishCommand("ack-recovery", ack)
	} else {
		_, err = base.AcknowledgeAllRecoveries(userId, comment)
	}
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Acknowledged all recoveries"), Detail: comment})
}

// BlockedRecoveries reads list of currently blocked recoveries, optionally filtered by cluster name
func BlockedRecoveries(params martini.Params, r render.Render, req *http.Request) {
	blockedRecoveries, err := base.ReadBlockedRecoveries(params["clusterName"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, blockedRecoveries)
}

// DisableGlobalRecoveries globally disables recoveries
func DisableGlobalRecoveries(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}

	var err error
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("disable-global-recoveries", 0)
	} else {
		err = base.DisableRecovery()
	}

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: "Globally disabled recoveries", Detail: "disabled"})
}

// EnableGlobalRecoveries globally enables recoveries
func EnableGlobalRecoveries(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}

	var err error
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("enable-global-recoveries", 0)
	} else {
		err = base.EnableRecovery()
	}
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: "Globally enabled recoveries", Detail: "enabled"})
}

// CheckGlobalRecoveries checks whether
func CheckGlobalRecoveries(params martini.Params, r render.Render, req *http.Request) {
	isDisabled, err := base.IsRecoveryDisabled()

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	details := "enabled"
	if isDisabled {
		details = "disabled"
	}
	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Global recoveries %+v", details), Detail: details})
}
