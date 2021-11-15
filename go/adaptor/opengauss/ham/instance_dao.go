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
	"fmt"
	oconstant "gitee.com/opengauss/ham4db/go/adaptor/opengauss/common/constant"
	outil "gitee.com/opengauss/ham4db/go/adaptor/opengauss/util"
	aodtstruct "gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/dtstruct"
	"gitee.com/opengauss/ham4db/go/core/agent"
	"gitee.com/opengauss/ham4db/go/core/cache"
	"gitee.com/opengauss/ham4db/go/core/limiter"
	"gitee.com/opengauss/ham4db/go/util"
	"github.com/opentracing/opentracing-go"
	"github.com/sjmudd/stopwatch"
	"google.golang.org/grpc"
	"strings"
	"time"

	odtstruct "gitee.com/opengauss/ham4db/go/adaptor/opengauss/dtstruct"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
)

var instanceReadChan = make(chan bool, constant.ConcurrencyBackendDBRead)

// GetInfoFromInstance create new instance according to instanceKey, get instance info by agent and write to backend database
func GetInfoFromInstance(ctx context.Context, instanceKey *dtstruct.InstanceKey, checkOnly, bufferWrites bool, latency *stopwatch.NamedStopwatch, agentAddr string) (inst *odtstruct.OpenGaussInstance, err error) {

	// trace this
	span, ctx := opentracing.StartSpanFromContext(ctx, "Get Instance Info")
	defer span.Finish()

	// if get error, recover it
	defer func() {
		if r := recover(); r != nil {
			err = cache.LogReadTopologyInstanceError(instanceKey, "unexpected, aborting", fmt.Errorf("%+v", r))
		}
	}()

	// check if instance is valid, update last attempt timestamp if invalid
	if !instanceKey.IsValid() {
		latency.Start("backend")
		if err = base.UpdateInstanceLastAttemptedCheck(instanceKey); err != nil {
			log.Error("update last attempt timestamp: %+v, error: %v", instanceKey, err)
		}
		latency.Stop("backend")
		return odtstruct.NewInstance(), fmt.Errorf("get instance info will not act on invalid instance key: %+v", *instanceKey)
	}

	// time to record discovery latency
	startToGetInfo := time.Now()

	// instance for this time
	instance := odtstruct.NewInstance()
	instance.Key = *instanceKey
	if instanceKey.ClusterId != "" {
		instance.ClusterId = instanceKey.ClusterId
	}

	instanceFound, partialSuccess, updateLastSeen := false, false, true
	resolvedHostname := ""
	var info *aodtstruct.DatabaseInfoResponse

	// update last attempt timestamp in background
	lastAttemptedCheckTimer := time.AfterFunc(time.Second, func() {
		go func() {
			err = base.UpdateInstanceLastAttemptedCheck(instanceKey)
		}()
	})

	latency.Start("instance")

	// get agent addr using instance hostname(ip or hostname)
	agt, err := agent.GetLatestAgent(instance.Key.Hostname)
	if err == nil {
		agentAddr = agt.GetAddr()
	}
	conn, err := cache.GetGRPC(ctx, agentAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Error("failed to connect agent,err:%v", err)
		goto END
	}
	func() {
		cspan, _ := opentracing.StartSpanFromContext(ctx, "Grpc Call Agent Collect Info")
		defer cspan.Finish()
		info, err = aodtstruct.NewOpengaussAgentClient(conn).CollectInfo(ctx, &aodtstruct.DatabaseInfoRequest{})
	}()
	if err != nil {
		log.Error("rpc call:%s collect info failed,err:%v", agentAddr, err)
		goto END
	}

	// instance is not available now
	if outil.NeedRecover(info.NodeInfo.BaseInfo.InstanceState) {
		updateLastSeen = false
		goto END
	}

	latency.Stop("instance")
	partialSuccess = true

	// update instance physical location(data and region and env)
	outil.UpdatePhysicalLocation(instance, config.Config.DataCenterPattern, "DataCenter")
	outil.UpdatePhysicalLocation(instance, config.Config.RegionPattern, "Region")
	outil.UpdatePhysicalLocation(instance, config.Config.EnvironmentPattern, "Environment")

	if info.NodeInfo.SyncInfo.LocalRole == oconstant.DBDown || info.GetNodeInfo().GetSyncInfo().GetSyncMode() != "" {
		//此时gaussdb可能正处于关机过程中，slavehosts信息无法获取，为了保留之前的slavehosts信息，此处不更新database_instance
		//return dtstruct.NewInstance(), log.Errorf("instance: %+v is stopping, do not update ham_database_instance!")
		log.Error("instance: %+v is stopping, do not update ham_database_instance", instanceKey)
		goto END
	}
	instanceFound = true
	instance.Key.Hostname = info.NodeInfo.BaseInfo.NodeName
	instance.Key.Port = int(info.NodeInfo.BaseInfo.DatanodePort)
	instance.InstanceId = info.NodeInfo.BaseInfo.InstanceId
	instance.Role = info.NodeInfo.BaseInfo.InstanceRole
	instance.DBState = info.NodeInfo.BaseInfo.InstanceState
	instance.ReplicationState = info.NodeInfo.BaseInfo.HaState
	instance.Version = info.NodeInfo.BaseInfo.GaussVersion
	instance.ClusterName = info.NodeInfo.BaseInfo.GsClusterName

	// set upstream instance
	instance.UpstreamKey = dtstruct.InstanceKey{
		Hostname:  info.NodeInfo.Upstream.NodeName,
		Port:      int(info.NodeInfo.Upstream.DatanodePort),
		DBType:    oconstant.DBTOpenGauss,
		ClusterId: instance.ClusterId,
	}

	// set downstream instance
	for _, downstream := range info.NodeInfo.DownstreamList {
		var replicaKey *dtstruct.InstanceKey
		replicaKey, err = base.NewResolveInstanceKey(oconstant.DBTOpenGauss, downstream.NodeName, int(downstream.DatanodePort))
		replicaKey.ClusterId = instance.ClusterId

		// key is valid and not match discover ignore filter
		if err == nil && replicaKey.IsValid() && !util.RegexpMatchPattern(replicaKey.StringCode(), config.Config.DiscoveryIgnoreReplicaHostnameFilters) {
			instance.AddDownstreamKey(replicaKey)
			continue
		}
		if err != nil {
			log.Error("resolve instance key:%s, error:%s", replicaKey, err)
		}
		if !replicaKey.IsValid() {
			log.Warning("invalid instance key:%s", replicaKey)
		}
		if util.RegexpMatchPattern(replicaKey.StringCode(), config.Config.DiscoveryIgnoreReplicaHostnameFilters) {
			log.Info("instance key:%s match discover ignore filter", replicaKey)
		}
	}

	// resolve hostname
	resolvedHostname = info.NodeInfo.BaseInfo.NodeName
	if resolvedHostname != instance.Key.Hostname {
		base.UpdateResolvedHostname(instance.Key.Hostname, resolvedHostname)
		instance.Key.Hostname = resolvedHostname
	}
	go func() {
		_ = base.ResolveHostnameIPs(instance.Key.Hostname)
	}()

	// read the current PromotionRule from candidate_database_instance.
	_ = cache.LogReadTopologyInstanceError(instanceKey, "ReadInstancePromotionRule", base.ReadInstancePromotionRule(instance))

	// update instance cluster alias
	_ = base.ReadClusterAliasOverride(instance.GetInstance())

	// instance is normal if it has downstream when it is downstream or it has downstream when it is downstream
	if (instance.Role == oconstant.DBPrimary && len(instance.DownstreamKeyMap) > 0) || (instance.UpstreamKey.Hostname != "" && instance.UpstreamKey.Port != 0) {
		instance.ReplicationState = oconstant.Normal
	}

	// update sync info
	if syncInfo := info.GetNodeInfo().GetSyncInfo(); syncInfo != nil {
		instance.ReceiverWriteLocation = syncInfo.GetReceiverWriteLocation()
		instance.ReceiverFlushLocation = syncInfo.GetReceiverFlushLocation()
		instance.ReceiverReceivedLocation = syncInfo.GetReceiverReceivedLocation()
		instance.ReceiverReplayLocation = syncInfo.GetReceiverReplayLocation()
		instance.SenderWriteLocation = syncInfo.GetSenderWriteLocation()
		instance.SenderFlushLocation = syncInfo.GetSenderFlushLocation()
		instance.SenderSentLocation = syncInfo.GetSenderSentLocation()
		instance.SenderReplayLocation = syncInfo.GetSenderReplayLocation()
		instance.SyncPercent = syncInfo.GetSyncPercent()
		instance.Channel = syncInfo.GetChannel()
	}

END:

	// only check, not need to write to backend database
	if checkOnly {
		return instance, err
	}

	// if found, set check status and write instance to backend database
	if instanceFound {

		// set check status
		instance.LastDiscoveryLatency = time.Since(startToGetInfo)
		instance.IsLastCheckValid = true
		instance.IsRecentlyChecked = true
		instance.IsUpToDate = true

		// stop updating last attempt check
		lastAttemptedCheckTimer.Stop()

		// write to buffer or backend database directly
		if bufferWrites {
			limiter.EnqueueInstanceWrite(instance, instanceFound, err)
		} else {
			if err = WriteToBackendDB(ctx, []dtstruct.InstanceAdaptor{instance}, instanceFound, updateLastSeen); err != nil {
				return nil, err
			}
		}

		// update downstream and downstream async
		go func() {

			// process downstream
			for downstream, ok := range instance.DownstreamKeyMap {
				if ok {
					UpdateInstanceCheck(&instance.Key, &downstream)
				}
			}

			// process downstream
			if instance.UpstreamKey.Hostname != "" {
				UpdateInstanceCheck(&dtstruct.InstanceKey{}, &instance.UpstreamKey)
			}
		}()

		return instance, nil
	}

	// Something is wrong, could be network-wise. Record that we
	// tried to check the instance. last_attempted_check is also
	// updated on success by writeInstance.
	return nil, base.UpdateInstanceLastChecked(&instance.Key, partialSuccess)
}

// ForgetInstance delete instance from table
func ForgetInstance(instanceKey *dtstruct.InstanceKey) error {

	// check if instance key is nil
	if instanceKey == nil {
		return log.Errorf("cannot forget nil instance")
	}

	// delete from table opengauss_database_instance and ham_database_instance
	multiSQL := &dtstruct.MultiSQL{}
	multiSQL.Query = append(multiSQL.Query, `delete from opengauss_database_instance where hostname = ? and port = ?`)
	multiSQL.Args = append(multiSQL.Args, []interface{}{instanceKey.Hostname, instanceKey.Port})
	multiSQL.Query = append(multiSQL.Query, `delete from ham_database_instance where hostname = ? and port = ? and db_type = ?`)
	multiSQL.Args = append(multiSQL.Args, []interface{}{instanceKey.Hostname, instanceKey.Port, instanceKey.DBType})
	if err := db.ExecMultiSQL(multiSQL); err != nil {
		return err
	}

	// write to audit
	base.AuditOperation("forget", instanceKey, "", "")
	return nil
}

// GetSyncInfo get sync percent for instance
func GetSyncInfo(instanceKey *dtstruct.InstanceKey) (interface{}, error) {

	// check if instance key is nil
	if instanceKey == nil {
		return nil, log.Errorf("cannot get sync for nil instance")
	}

	// select from table opengauss_database_instance
	percent := 0
	if err := db.Query(`
		select
			sync_percent
		from 
			opengauss_database_instance 
		where 
			hostname = ? and port = ?
	`, sqlutil.Args(instanceKey.Hostname, instanceKey.Port), func(rowMap sqlutil.RowMap) error {
		percent = rowMap.GetInt("sync_percent")
		return nil
	}); err != nil {
		return nil, err
	}
	return percent, nil
}

// GetDownStreamInstance get all downstream instances for upstream
func GetDownStreamInstance(downstreamKeyList []dtstruct.InstanceKey) ([]*odtstruct.OpenGaussInstance, error) {

	// if no downstream, just return
	if len(downstreamKeyList) == 0 {
		return []*odtstruct.OpenGaussInstance{}, nil
	}

	// for sql args
	var args []interface{}
	var orCond []string

	// get condition and args
	condition := "db_type = ? and ("
	args = append(args, downstreamKeyList[0].DBType)
	for _, key := range downstreamKeyList {
		orCond = append(orCond, "(hostname = ? and port = ?)")
		args = append(args, key.Hostname, key.Port)
	}

	// read from backend database
	downstreamList, err := ReadInstancesByCondition(oconstant.QuerySQL, condition+strings.Join(orCond, "or")+")", args, "")
	if err != nil {
		return nil, err
	}
	if len(downstreamList) == 0 {
		return nil, log.Errorf("can not find downstream:%v from backend database", downstreamKeyList)
	}
	return downstreamList, nil
}

// ReadInstancesByCondition is a generic function to read instances from the backend database
func ReadInstancesByCondition(query string, condition string, args []interface{}, sort string) ([]*odtstruct.OpenGaussInstance, error) {
	readFunc := func() ([]*odtstruct.OpenGaussInstance, error) {
		var instanceList []*odtstruct.OpenGaussInstance
		if sort == "" {
			sort = `hostname, port`
		}
		sql := fmt.Sprintf(`%s where %s order by %s`, query, condition, sort)
		if err := db.Query(sql, args, func(m sqlutil.RowMap) error {
			instance := RowToInstance(m)
			instanceList = append(instanceList, instance)
			return nil
		}); err != nil {
			return []*odtstruct.OpenGaussInstance{}, log.Errore(err)
		}
		return instanceList, nil
	}
	instanceReadChan <- true
	instanceList, err := readFunc()
	<-instanceReadChan
	return instanceList, err
}

// RowToInstance reads a single instance row from the backend database.
func RowToInstance(m sqlutil.RowMap) *odtstruct.OpenGaussInstance {
	instance := odtstruct.NewInstance()
	instance.Key.DBType = dtstruct.GetDatabaseType(m.GetString("db_type"))
	instance.Key.Hostname = m.GetString("hostname")
	instance.Key.Port = m.GetInt("port")
	instance.Uptime = m.GetUint("uptime")
	instance.ClusterId = m.GetString("cluster_id")
	instance.InstanceId = m.GetString("db_id")
	instance.Version = m.GetString("db_version")
	instance.ReadOnly = m.GetBool("is_read_only")
	instance.Role = m.GetString("db_role")
	instance.DBState = m.GetString("db_state")
	instance.UpstreamKey.Hostname = m.GetString("upstream_host")
	instance.UpstreamKey.Port = m.GetInt("upstream_port")
	instance.IsDetachedMaster = instance.UpstreamKey.IsDetached()
	instance.HasReplicationFilters = m.GetBool("has_replication_filters")
	instance.ReplicationLagSeconds = m.GetNullInt64("replication_downstream_lag")
	replicasJSON := m.GetString("downstream_hosts")
	instance.ClusterName = m.GetString("cluster_name")
	instance.SuggestedClusterAlias = m.GetString("cluster_alias")
	instance.DataCenter = m.GetString("pl_data_center")
	instance.Region = m.GetString("pl_region")
	instance.Environment = m.GetString("environment")
	instance.ReplicationDepth = m.GetUint("replication_depth")
	instance.ReplicationState = m.GetString("replication_state")
	instance.IsCoUpstream = m.GetBool("is_co_master")
	instance.ReplicationCredentialsAvailable = m.GetBool("is_replication_credentials_available")
	instance.HasReplicationCredentials = m.GetBool("has_replication_credentials")
	instance.IsUpToDate = m.GetUint("seconds_since_last_checked") <= config.Config.InstancePollSeconds
	instance.IsRecentlyChecked = m.GetUint("seconds_since_last_checked") <= config.Config.InstancePollSeconds*5
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
	_ = instance.DownstreamKeyMap.ReadJson(replicasJSON)
	instance.SetFlavorName()

	// problems
	if !instance.IsLastCheckValid {
		instance.Problems = append(instance.Problems, "last_check_invalid")
	} else if !instance.IsRecentlyChecked {
		instance.Problems = append(instance.Problems, "not_recently_checked")
	}
	return instance
}

// WriteToBackendDB write all instance to backend database
func WriteToBackendDB(ctx context.Context, instances []dtstruct.InstanceAdaptor, instanceWasActuallyFound bool, updateLastSeen bool) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "Write To Backend DB")
	defer span.Finish()

	// no instance, just return
	if len(instances) == 0 {
		return nil
	}

	// for ham_database_instance
	baseInstanceColumns := []string{
		"db_type",
		"hostname",
		"port",
		"last_attempted_check_timestamp",
		"last_checked_timestamp",
		"is_last_check_partial_success",
		"uptime",
		"db_id",
		"db_role",
		"db_state",
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
		"replication_state",
		"is_co_master",
		"is_replication_credentials_available",
		"has_replication_credentials",
		"is_allow_tls",
	}
	baseInstanceValues := make([]string, len(baseInstanceColumns), len(baseInstanceColumns))
	for i := range baseInstanceColumns {
		baseInstanceValues[i] = "?"
	}
	baseInstanceValues[3] = "now()" // last_checked_timestamp
	baseInstanceValues[4] = "now()" // last_attempted_check_timestamp
	baseInstanceValues[5] = "1"     // is_last_check_partial_success
	if updateLastSeen {
		baseInstanceColumns = append(baseInstanceColumns, "last_seen_timestamp")
		baseInstanceValues = append(baseInstanceValues, "now()")
	}

	// for opengauss_database_instance
	opengaussInstanceColumns := []string{
		"db_id",
		"hostname",
		"port",
		"xlog_sender_sent_location",
		"xlog_sender_write_location",
		"xlog_sender_flush_location",
		"xlog_sender_replay_location",
		"xlog_receiver_received_location",
		"xlog_receiver_write_location",
		"xlog_receiver_flush_location",
		"xlog_receiver_replay_location",
		"sync_percent",
		"channel",
		"last_update_timestamp",
	}
	opengaussValues := make([]string, len(opengaussInstanceColumns), len(opengaussInstanceColumns))
	for i := range opengaussInstanceColumns {
		opengaussValues[i] = "?"
	}
	opengaussValues[len(opengaussInstanceColumns)-1] = "NOW()"
	odisql, err := base.MkInsertOdku("opengauss_database_instance", opengaussInstanceColumns, opengaussValues, 1, !instanceWasActuallyFound)
	if err != nil {
		return err
	}

	// generate multi sql for table ham_database_instance and opengauss_database_instance
	multiSQL := &dtstruct.MultiSQL{}
	for _, ins := range instances {
		instance := ins.GetInstance()
		opengaussIns := ins.(*odtstruct.OpenGaussInstance)

		// for ham_database_instance
		var argsInstance []interface{}
		argsInstance = append(argsInstance, instance.Key.DBType)
		argsInstance = append(argsInstance, instance.Key.Hostname)
		argsInstance = append(argsInstance, instance.Key.Port)
		argsInstance = append(argsInstance, instance.Uptime)
		argsInstance = append(argsInstance, instance.InstanceId)
		argsInstance = append(argsInstance, instance.Role)
		argsInstance = append(argsInstance, instance.DBState)
		argsInstance = append(argsInstance, instance.Version)
		argsInstance = append(argsInstance, instance.ReadOnly)
		argsInstance = append(argsInstance, instance.UpstreamKey.Hostname)
		argsInstance = append(argsInstance, instance.UpstreamKey.Port)
		argsInstance = append(argsInstance, instance.HasReplicationFilters)
		argsInstance = append(argsInstance, instance.ReplicationLagSeconds)
		argsInstance = append(argsInstance, len(instance.DownstreamKeyMap))
		argsInstance = append(argsInstance, instance.DownstreamKeyMap.ToJSONString())
		argsInstance = append(argsInstance, instance.ClusterName)
		argsInstance = append(argsInstance, instance.SuggestedClusterAlias)
		argsInstance = append(argsInstance, instance.DataCenter)
		argsInstance = append(argsInstance, instance.Region)
		argsInstance = append(argsInstance, instance.Environment)
		argsInstance = append(argsInstance, instance.ReplicationDepth)
		argsInstance = append(argsInstance, instance.ReplicationState)
		argsInstance = append(argsInstance, instance.IsCoUpstream)
		argsInstance = append(argsInstance, instance.ReplicationCredentialsAvailable)
		argsInstance = append(argsInstance, instance.HasReplicationCredentials)
		argsInstance = append(argsInstance, instance.AllowTLS)

		// for opengauss_database_instance
		var argsOpengaussInstance []interface{}
		argsOpengaussInstance = append(argsOpengaussInstance, instance.InstanceId)
		argsOpengaussInstance = append(argsOpengaussInstance, instance.Key.Hostname)
		argsOpengaussInstance = append(argsOpengaussInstance, instance.Key.Port)
		argsOpengaussInstance = append(argsOpengaussInstance, opengaussIns.SenderSentLocation)
		argsOpengaussInstance = append(argsOpengaussInstance, opengaussIns.SenderWriteLocation)
		argsOpengaussInstance = append(argsOpengaussInstance, opengaussIns.SenderFlushLocation)
		argsOpengaussInstance = append(argsOpengaussInstance, opengaussIns.SenderReplayLocation)
		argsOpengaussInstance = append(argsOpengaussInstance, opengaussIns.ReceiverReceivedLocation)
		argsOpengaussInstance = append(argsOpengaussInstance, opengaussIns.ReceiverWriteLocation)
		argsOpengaussInstance = append(argsOpengaussInstance, opengaussIns.ReceiverFlushLocation)
		argsOpengaussInstance = append(argsOpengaussInstance, opengaussIns.ReceiverReplayLocation)
		argsOpengaussInstance = append(argsOpengaussInstance, opengaussIns.SyncPercent)
		argsOpengaussInstance = append(argsOpengaussInstance, opengaussIns.Channel)

		// check if update cluster id
		instanceColumns := baseInstanceColumns
		instanceValues := baseInstanceValues
		if instance.ClusterId != "" {
			instanceColumns = append(baseInstanceColumns, "cluster_id")
			argsInstance = append(argsInstance, instance.ClusterId)
			instanceValues = append(baseInstanceValues, "?")
		}

		// get sql for table ham_database_instance
		var disql string
		disql, err = base.MkInsertOdku("ham_database_instance", instanceColumns, instanceValues, 1, !instanceWasActuallyFound)
		if err != nil {
			return err
		}

		//	update multi sql query and args
		multiSQL.Query = append(multiSQL.Query, disql)
		multiSQL.Args = append(multiSQL.Args, argsInstance)
		multiSQL.Query = append(multiSQL.Query, odisql)
		multiSQL.Args = append(multiSQL.Args, argsOpengaussInstance)
	}

	// exec multi sql to insert new record to table ham_database_instance and opengauss_database_instance
	if err = db.ExecMultiSQL(multiSQL); err != nil {
		return err
	}
	return nil
}

// ReadFromBackendDB reads an instance from the backend database
// We know there will be at most one (hostname & port are PK) and we expect to find one
func ReadFromBackendDB(instanceKey *dtstruct.InstanceKey) (*odtstruct.OpenGaussInstance, bool, error) {
	condition := `
		hostname = ? and port = ?
	`
	instanceList, err := ReadInstancesByCondition(oconstant.QuerySQL, condition, sqlutil.Args(instanceKey.Hostname, instanceKey.Port), "")
	if len(instanceList) == 0 || err != nil {
		return nil, false, err
	}
	return instanceList[0], true, nil
}

// GetDuplicateInstance
func GetDuplicateInstance(instanceKey *dtstruct.InstanceKey, clusterName string) ([]*odtstruct.OpenGaussInstance, error) {
	query := `
		select * from ham_database_instance
	`
	condition := `
		cluster_name = ? and hostname <> ? and role = ?
	`
	return ReadInstancesByCondition(query, condition, sqlutil.Args(clusterName, instanceKey.Hostname, oconstant.DBPrimary), "")
}

// UpdateInstanceCheck insert or update downstream instance to backend database
func UpdateInstanceCheck(upstream *dtstruct.InstanceKey, downstream *dtstruct.InstanceKey) {
	_, err := db.ExecSQL(`
		update
			ham_database_instance
		set 
			last_attempted_check_timestamp = now(), last_checked_timestamp = now(), last_seen_timestamp = now(), upstream_host = ?, upstream_port = ?
		where
			hostname = ? and
			port = ? and
			db_type = ?
	`,
		upstream.Hostname, upstream.Port, downstream.Hostname, downstream.Port, downstream.DBType,
	)
	if err != nil {
		log.Error("insert or update instance check status to database, error:%s", err)
	}
}
