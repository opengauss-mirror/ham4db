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

package instance

import (
	"context"
	"errors"
	"fmt"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/cache"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"github.com/rcrowley/go-metrics"
	"github.com/sjmudd/stopwatch"
	"regexp"
	"sort"
	"strings"

	"gitee.com/opengauss/ham4db/go/config"
)

// Constant strings for Group Replication information
// See https://dev.mysql.com/doc/refman/8.0/en/replication-group-members-table.html for additional information.

var readInstanceCounter = metrics.NewCounter()
var writeInstanceCounter = metrics.NewCounter()

func init() {
	metrics.Register(constant.MetricInstanceRead, readInstanceCounter)
	metrics.Register(constant.MetricInstanceWrite, writeInstanceCounter)
}

// GetInfoFromInstance collect information from the instance and write the result synchronously to the backend.
func GetInfoFromInstance(ctx context.Context, instanceKey *dtstruct.InstanceKey, agent string, latency *stopwatch.NamedStopwatch) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).GetInfoFromInstance(ctx, instanceKey, false, false, latency, agent)
}

// GetInfoFromInstance collect information from the instance and write the result synchronously to the backend.
func CheckInstance(ctx context.Context, instanceKey *dtstruct.InstanceKey, agent string, latency *stopwatch.NamedStopwatch) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).GetInfoFromInstance(ctx, instanceKey, true, false, latency, agent)
}

// GetInfoSync get node sync info.
func GetInfoSync(instanceKey *dtstruct.InstanceKey, agent string) (interface{}, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).GetSyncInfo(instanceKey, false, agent)
}

// WriteInstance write all instances to backend db
func WriteInstance(instanceList []dtstruct.InstanceAdaptor, instanceWasActuallyFound bool, updateLastSeen bool, lastError error) error {

	// check last error, if not nil, do nothing
	if lastError != nil {
		return lastError
	}

	//  filter instance, ignore instance has be forgotten
	var writeList []dtstruct.InstanceAdaptor
	for _, instance := range instanceList {
		if base.IsInstanceForgotten(&instance.GetInstance().Key) && !instance.GetInstance().IsSeed() {
			continue
		}
		writeList = append(writeList, instance)
	}
	if len(writeList) == 0 {
		return nil
	}

	// write instance left to backend db
	return dtstruct.GetHamHandler(instanceList[0].GetInstance().Key.DBType).WriteToBackendDB(context.TODO(), writeList, instanceWasActuallyFound, updateLastSeen)
}

// ReadInstance reads an instance from the backend database
func ReadInstance(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, bool, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).ReadFromBackendDB(instanceKey)
}

// ReadClusterInstance is a generic function to read instances for cluster
func ReadClusterInstance(dbt string, clusterName string) ([]dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(dbt).ReadClusterInstances(clusterName)
}

// ReadInstanceByCondition is a generic function to read instance from the backend database
func ReadInstanceByCondition(dbt string, query string, condition string, args []interface{}, sort string) ([]dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(dbt).ReadInstanceByCondition(query, condition, args, sort)
}

// ReadInstanceLostInRecovery returns all instances (potentially filtered by cluster) which are currently indicated as downtime due to being lost during a topology recovery.
func ReadInstanceLostInRecovery(dbt string, clusterName string) ([]dtstruct.InstanceAdaptor, error) {
	condition := `
		if(
			ham_database_instance_downtime.downtime_active = 1 and 
			ham_database_instance_downtime.end_timestamp > current_timestamp and 
			ham_database_instance_downtime.reason = ?
			, 1, 0
		)
		and ? in ('', cluster_name)
	`
	return ReadInstanceByCondition(dbt, constant.DefaultQuery, condition, sqlutil.Args(constant.DowntimeReasonLostInRecovery, clusterName), "cluster_name asc, replication_depth asc")
}

// ReadInstanceDownStream read all downstream instance of a given master.
func ReadInstanceDownStream(dbt string, masterKey *dtstruct.InstanceKey) ([]dtstruct.InstanceAdaptor, error) {
	return ReadInstanceByCondition(dbt, constant.DefaultQuery, "upstream_host = ? and upstream_port = ?", sqlutil.Args(masterKey.Hostname, masterKey.Port), "")
}

// ReadInstanceUnSeen reads all instances which were not recently seen.
func ReadInstanceUnSeen(dbt string) ([]dtstruct.InstanceAdaptor, error) {
	return ReadInstanceByCondition(dbt, constant.DefaultQuery, "last_seen_timestamp < last_checked_timestamp", sqlutil.Args(), "")
}

// ReadClusterMasterWriteable returns the/a writeable master of this cluster
// Typically, the cluster name indicates the master of the cluster. However, in circular
// master-master replication one master can assume the name of the cluster, and it is
// not guaranteed that it is the writeable one.
func ReadClusterMasterWriteable(dbt string, clusterName string) ([]dtstruct.InstanceAdaptor, error) {
	return ReadInstanceByCondition(dbt, constant.DefaultQuery, "cluster_name = ? and is_read_only = 0 and (replication_depth = 0 or is_co_master)", sqlutil.Args(clusterName), "replication_depth asc")
}

// ReadClusterMaster returns the master of this cluster.
// - if the cluster has co-masters, the/a writable one is returned
// - if the cluster has a single master, that master is returned whether it is read-only or writable.
func ReadClusterMaster(dbt string, clusterName string) ([]dtstruct.InstanceAdaptor, error) {
	return ReadInstanceByCondition(dbt, constant.DefaultQuery, "cluster_name = ? and (replication_depth = 0 or is_co_master)", sqlutil.Args(clusterName), "is_read_only asc, replication_depth asc")
}

// ReadMasterWriteable returns writeable masters of all clusters, but only one per cluster, in similar logic to ReadClusterMasterWriteable
func ReadMasterWriteable() (instanceList []*dtstruct.Instance, err error) {

	// get master list
	var masterList []*dtstruct.Instance
	if err = db.Query(`
				select hostname, port, db_type, cluster_name 
				from ham_database_instance 
				where is_read_only = 0 and (replication_depth = 0 or is_co_master)
				order by cluster_name asc, replication_depth asc
		`,
		sqlutil.Args(),
		func(m sqlutil.RowMap) error {
			instance := &dtstruct.Instance{}
			instance.Key.Hostname = m.GetString("hostname")
			instance.Key.Port = m.GetInt("port")
			instance.Key.DBType = dtstruct.GetDatabaseType(m.GetString("db_type"))
			instance.ClusterName = m.GetString("cluster_name")
			masterList = append(masterList, instance)
			return nil
		},
	); err != nil {
		return
	}

	// get one record for same cluster, order by replication_depth asc
	visited := make(map[string]bool)
	for _, instance := range masterList {
		if !visited[instance.ClusterName] {
			visited[instance.ClusterName] = true
			instanceList = append(instanceList, instance)
		}
	}
	return
}

// ReadInstanceDowntime returns all instances currently marked as downtime, potentially filtered by cluster
func ReadInstanceDowntime(dbt string, clusterName string) ([]dtstruct.InstanceAdaptor, error) {
	condition := `
		ifnull(
			ham_database_instance_downtime.downtime_active = 1
			and ham_database_instance_downtime.end_timestamp > now()
			, 0
		)
		and ? IN ('', cluster_name)
	`
	return ReadInstanceByCondition(dbt, constant.DefaultQuery, condition, sqlutil.Args(clusterName), "cluster_name asc, replication_depth asc")
}

// ReadInstanceMinimal get minimal instance with minimal base info
func ReadInstanceMinimal() (res []dtstruct.MinimalInstance, err error) {
	err = db.Query(`
		select
			hostname, port, db_type, cluster_id, upstream_host, upstream_port, cluster_name
		from
			ham_database_instance
		`,
		sqlutil.Args(), func(m sqlutil.RowMap) error {
			inst := dtstruct.MinimalInstance{}
			inst.Key = dtstruct.InstanceKey{
				Hostname: m.GetString("hostname"), Port: m.GetInt("port"),
				DBType: m.GetString("db_type"), ClusterId: m.GetString("cluster_id"),
			}
			inst.MasterKey = dtstruct.InstanceKey{
				Hostname: m.GetString("upstream_host"), Port: m.GetInt("upstream_port"),
				DBType: m.GetString("db_type"), ClusterId: m.GetString("cluster_id"),
			}
			inst.ClusterName = m.GetString("cluster_name")

			// only if not in "forget" cache
			if !base.IsInstanceForgotten(&inst.Key) {
				res = append(res, inst)
			}
			return nil
		})
	return
}

// ReadInstanceKeyOutDate reads and returns keys for all instances that are not up to date (i.e.
// pre-configured time has passed since they were last checked)
// But we also check for the case where an attempt at instance checking has been made, that hasn't
// resulted in an actual check! This can happen when TCP/IP connections are hung, in which case the "check"
// never returns. In such case we multiply interval by a factor, so as not to open too many connections on
// the instance.
func ReadInstanceKeyOutDate() (res []dtstruct.InstanceKey, err error) {
	err = db.Query(
		`
		select db_type, hostname, port, cluster_id
		from ham_database_instance
		where
			case 
				when last_attempted_check_timestamp <= last_checked_timestamp then last_checked_timestamp < now() - interval ? second
				else last_checked_timestamp < now() - interval ? second
			end
	`,
		sqlutil.Args(config.Config.InstancePollSeconds, 2*config.Config.InstancePollSeconds),
		func(m sqlutil.RowMap) error {
			instanceKey, instErr := base.NewResolveInstanceKey(m.GetString("db_type"), m.GetString("hostname"), m.GetInt("port"))

			// We don't return an error because we want to keep filling the outdated instances list.
			if instErr != nil {
				log.Errore(instErr)

				// only if not in "forget" cache
			} else if !base.IsInstanceForgotten(instanceKey) {
				instanceKey.ClusterId = m.GetString("cluster_id")
				res = append(res, *instanceKey)
			}
			return nil
		})
	return
}

// ReadInstanceProblem reads all instances with problems
func ReadInstanceProblem(dbt string, clusterName string) ([]dtstruct.InstanceAdaptor, error) {

	// get problem instance
	handler := dtstruct.GetHamHandler(dbt)
	instanceList, err := handler.ReadInstanceByCondition(handler.SQLProblemQuery(), handler.SQLProblemCondition(), handler.SQLProblemArgs(dbt, clusterName, clusterName), "")
	if err != nil {
		return instanceList, err
	}

	// check again
	var reportList []dtstruct.InstanceAdaptor
	for _, instance := range instanceList {
		if !instance.GetInstance().IsDowntimed && !util.RegexpMatchPattern(instance.GetInstance().Key.StringCode(), config.Config.ProblemIgnoreHostnameFilters) {
			reportList = append(reportList, instance)
		}
	}
	return reportList, nil
}

// GetHeuristicClusterPoolInstanceLag returns a heuristic lag for the instances participating in a cluster pool (or all the cluster's pools)
func GetHeuristicClusterPoolInstanceLag(dbt string, clusterName string, pool string) (int64, error) {
	instanceList, err := GetHeuristicClusterPoolInstance(dbt, clusterName, pool)
	if err != nil {
		return 0, err
	}
	return GetInstanceMaxLag(instanceList)
}

// GetHeuristicClusterPoolInstance returns instances of a cluster which are also pooled. If `pool` argument
// is empty, all pools are considered, otherwise, only instances of given pool are considered.
func GetHeuristicClusterPoolInstance(dbt string, clusterName string, pool string) (result []dtstruct.InstanceAdaptor, err error) {

	// get all instance for cluster
	var instanceList []dtstruct.InstanceAdaptor
	if instanceList, err = ReadClusterInstance(dbt, clusterName); err != nil {
		return
	}

	// get instance in pool and put it to instance key map
	pooledInstanceKeys := dtstruct.NewInstanceKeyMap()
	var clusterPoolInstanceList []*dtstruct.ClusterPoolInstance
	if clusterPoolInstanceList, err = base.ReadClusterPoolInstance(clusterName, pool); err != nil {
		return
	}
	for _, cpInst := range clusterPoolInstanceList {
		pooledInstanceKeys.AddKey(dtstruct.InstanceKey{Hostname: cpInst.Hostname, Port: cpInst.Port})
	}

	// if key pooled and last check is valid and not binlog server, put it to result
	for _, instance := range instanceList {
		if !instance.IsReplicaServer() && instance.GetInstance().IsLastCheckValid && pooledInstanceKeys.HasKey(instance.GetInstance().Key) {
			result = append(result, instance)
		}
	}

	return
}

// GetInstanceMaxLag return the maximum lag in a set of instances
func GetInstanceMaxLag(instanceList []dtstruct.InstanceAdaptor) (maxLag int64, err error) {
	if len(instanceList) == 0 {
		return 0, log.Errorf("no instances found")
	}
	for _, inst := range instanceList {
		if inst.GetInstance().ReplicationLagSeconds.Valid && inst.GetInstance().ReplicationLagSeconds.Int64 > maxLag {
			maxLag = inst.GetInstance().ReplicationLagSeconds.Int64
		}
	}
	return
}

// ReviewInstanceUnSeen reviews instance that have not been seen (supposedly dead) and updates some of their data
func ReviewInstanceUnSeen(dbt string) error {

	// get unseen instance
	instanceList, err := ReadInstanceUnSeen(dbt)
	if err != nil {
		return err
	}

	// update cluster name
	operation := 0
	for _, inst := range instanceList {
		instance := inst

		// get master resolved hostname
		var masterHostname string
		if masterHostname, err = base.ResolveHostname(instance.GetInstance().UpstreamKey.Hostname); err != nil {
			continue
		}
		instance.GetInstance().UpstreamKey.Hostname = masterHostname

		// update instance cluster name if needed
		clusterName := instance.GetInstance().ClusterName
		if err = base.ReadInstanceClusterAttributes(instance); err == nil && instance.GetInstance().ClusterName != clusterName {
			_ = base.UpdateInstanceClusterName(instance)
			operation++
		}
	}
	base.AuditOperation(constant.AuditReviewUnSeenInstance, nil, "", fmt.Sprintf("operation instance: %d", operation))
	return err
}

// IsInstanceExistInBackendDB check if instance exist in backend database
func IsInstanceExistInBackendDB(instanceKey *dtstruct.InstanceKey) dtstruct.InstanceAdaptor {
	instance, _, err := ReadInstance(instanceKey)
	if err != nil || instance == nil {
		log.Errorf("no key:%s found %s", instanceKey, err)
		return nil
	}
	return instance
}

// InjectUnseenMaster will review masters of instances that are known to be replicating, yet which are not listed
// in ham_database_instance. Since their replicas are listed as replicating, we can assume that such masters actually do
// exist: we shall therefore inject them with minimal details into the ham_database_instance table.
func InjectUnseenMaster() error {

	// get unseen master key list
	masterKeyList, err := base.ReadUnseenMasterKey()
	if err != nil {
		return err
	}

	// check again
	var instanceList []dtstruct.InstanceAdaptor
	for _, instKey := range masterKeyList {
		masterKey := instKey

		//	ignore if host name in config
		if util.RegexpMatchPattern(masterKey.StringCode(), config.Config.DiscoveryIgnoreMasterHostnameFilters) || util.RegexpMatchPattern(masterKey.StringCode(), config.Config.DiscoveryIgnoreHostnameFilters) {
			continue
		}

		// construct instance and append to instance list
		inst := dtstruct.GetInstanceAdaptor(masterKey.DBType)
		inst.SetInstance(&dtstruct.Instance{Key: masterKey, Version: "Unknown", ClusterName: masterKey.StringCode()})
		instanceList = append(instanceList, inst)
	}

	// update master last seen
	if err = WriteInstance(instanceList, false, true, nil); err != nil {
		return err
	}
	base.AuditOperation(constant.AuditInjectUnSeenMaster, nil, "", fmt.Sprintf("operation instance: %d", len(instanceList)))
	return err
}

// SearchInstance reads all instances qualifying for some search string
func SearchInstance(dbt string, searchString string) ([]dtstruct.InstanceAdaptor, error) {
	searchString = strings.TrimSpace(searchString)
	condition := `
		instr(hostname, ?) > 0 or 
		instr(cluster_name, ?) > 0 or 
		instr(db_version, ?) > 0 or 
		instr(concat(hostname, ':', port), ?) > 0 or 
		concat(port, '') = ?
	`
	return ReadInstanceByCondition(dbt, constant.DefaultQuery, condition, sqlutil.Args(searchString, searchString, searchString, searchString, searchString), `replication_depth asc, downstream_count desc, cluster_name, hostname, port`)
}

// FindInstance reads all instances whose name matches given pattern
func FindInstance(dbt string, regexpPattern string) (result []dtstruct.InstanceAdaptor, err error) {

	//  check regex pattern
	r, err := regexp.Compile(regexpPattern)
	if err != nil {
		return result, err
	}

	// get all instance from database
	condition := `1=1`
	instanceList, err := ReadInstanceByCondition(dbt, "select hostname, port, db_type from ham_database_instance", condition, sqlutil.Args(), "replication_depth asc, downstream_count desc, cluster_name, hostname, port")
	if err != nil {
		return instanceList, err
	}

	// match the regex
	for _, instance := range instanceList {
		if r.MatchString(instance.GetInstance().Key.DisplayString()) {
			result = append(result, instance)
		}
	}
	return result, nil
}

// BulkReadInstance returns a list of all instances from the database
// - I only need the Hostname and Port fields.
// - I must use ReadInstanceByCondition to ensure all column settings are correct.
func BulkReadInstance(dbt string) (instKeyList []*dtstruct.InstanceKey, err error) {

	// get all instance
	var instanceList []dtstruct.InstanceAdaptor
	if instanceList, err = ReadInstanceByCondition(dbt, "select hostname, port, db_type from ham_database_instance", "1=1", nil, ""); err != nil {
		return nil, err
	}

	// update counters if we picked anything up
	if len(instanceList) > 0 {
		readInstanceCounter.Inc(int64(len(instanceList)))
		for _, instance := range instanceList {
			instKeyList = append(instKeyList, &instance.GetInstance().Key)
		}
		// sort on and not the backend (should be redundant)
		sort.Sort(dtstruct.ByNamePort(instKeyList))
	}
	return
}

// InjectSeed intent to be used to inject an instance upon startup, assuming it's not already known.
func InjectSeed(instanceKey *dtstruct.InstanceKey) error {

	// check
	if instanceKey == nil {
		return log.Errore(errors.New("instance key is nil"))
	}

	// construct a minimal instance
	clusterName := instanceKey.StringCode()
	instance := &dtstruct.Instance{Key: *instanceKey, Version: "Unknown", ClusterName: clusterName}
	instance.SetSeed()
	ia := dtstruct.GetInstanceAdaptor(instanceKey.DBType)
	ia.SetInstance(instance)

	// save the instance to database
	err := WriteInstance([]dtstruct.InstanceAdaptor{ia}, false, true, nil)

	// audit this operation
	base.AuditOperation(constant.AuditInjectSeed, instanceKey, clusterName, "injected")
	return err
}

// GetMastersKVPair get a listing of KVPair for clusters master, for all clusters or for a specific cluster.
func GetMastersKVPair(dbt string, clusterName string) (kvPairs []*dtstruct.KVPair, err error) {
	clusterAliasMap := make(map[string]string)

	// get all cluster info from database and map cluster name to cluster alias
	var clusterInfoList []dtstruct.ClusterInfo
	if clusterInfoList, err = base.ReadAllClusterInfo(dbt, clusterName); err != nil {
		return
	}
	for _, clusterInfo := range clusterInfoList {
		clusterAliasMap[clusterInfo.ClusterName] = clusterInfo.ClusterAlias
	}

	// get all writeable master and get master kv pair
	var masterList []*dtstruct.Instance
	if masterList, err = ReadMasterWriteable(); err != nil {
		return
	}
	for _, master := range masterList {
		kvPairs = append(kvPairs, base.GetClusterMasterKVPairs(clusterAliasMap[master.ClusterName], &master.Key)...)
	}

	return
}

// HeuristicallyApplyClusterDomainInstanceAttribute writes down the cluster-domain
// to master-hostname as a general attribute, by reading current topology and **trusting** it to be correct
func HeuristicallyApplyClusterDomainInstanceAttribute(dbt string, clusterName string) (instanceKey *dtstruct.InstanceKey, err error) {

	// get cluster info and check
	clusterInfo, err := base.ReadClusterInfo(dbt, clusterName)
	if err != nil {
		return nil, err
	}
	if clusterInfo.ClusterDomain == "" {
		return nil, fmt.Errorf("cannot find domain name for cluster %+v", clusterName)
	}

	// get all writeable master for cluster and check
	masters, err := ReadClusterMasterWriteable(dbt, clusterName)
	if err != nil {
		return nil, err
	}
	if len(masters) != 1 {
		return nil, fmt.Errorf("found %+v potential master for cluster %+v", len(masters), clusterName)
	}

	instanceKey = &masters[0].GetInstance().Key
	return instanceKey, base.SetGeneralAttribute(clusterInfo.ClusterDomain, instanceKey.StringCode())
}

// ForgetInstance delete instance by forget according to instance type
func ForgetInstance(instanceKey *dtstruct.InstanceKey) error {
	if err := base.ForgetInstance(instanceKey); err != nil {
		return err
	}
	return dtstruct.GetHamHandler(instanceKey.DBType).ForgetInstance(instanceKey)
}

// ForgetClusterInstance removes an instance entry from the backed database.
// It may be auto-rediscovered through topology or requested for discovery by multiple means.
func ForgetClusterInstance(dbt string, clusterName string) (err error) {

	// get cluster instance list and check
	var clusterInstanceList []dtstruct.InstanceAdaptor
	if clusterInstanceList, err = ReadClusterInstance(dbt, clusterName); err != nil {
		return err
	}
	if len(clusterInstanceList) == 0 {
		return nil
	}

	// put instance to cache and audit this operation
	for _, instance := range clusterInstanceList {
		cache.ForgetInstance(instance.GetInstance().Key.StringCode(), true)
		base.AuditOperation(constant.AuditForget, &instance.GetInstance().Key, clusterName, "")
	}

	// delete all instance of this cluster from database
	_, err = db.ExecSQL(`delete from ham_database_instance where cluster_name = ?`, clusterName)
	return err
}
