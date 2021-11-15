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
	"bytes"
	"errors"
	"fmt"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/cache"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"strings"
)

var instanceReadChan = make(chan bool, constant.ConcurrencyBackendDBRead)
var emptyInstanceKey dtstruct.InstanceKey

// UpdateInstanceClusterName update instance cluster name.
func UpdateInstanceClusterName(instance dtstruct.InstanceAdaptor) error {
	return db.ExecDBWrite(
		func() error {
			if _, err := db.ExecSQL(
				"update ham_database_instance set cluster_name=? where hostname = ? and port = ?",
				instance.GetInstance().ClusterName,
				instance.GetInstance().Key.Hostname,
				instance.GetInstance().Key.Port,
			); err != nil {
				return err
			}
			AuditOperation(constant.AuditUpdateClusterName, &instance.GetInstance().Key, instance.GetInstance().ClusterName, fmt.Sprintf("set instance %s cluster name to %s", instance.GetInstance().Key, instance.GetInstance().ClusterName))
			return nil
		},
	)
}

// ReadUnseenMasterKey will read list of masters that have never been seen, and yet whose replicas seem to be replicating.
func ReadUnseenMasterKey() (res []dtstruct.InstanceKey, err error) {
	err = db.Query(`
			select distinct
			    slave_instance.db_type, slave_instance.upstream_host, slave_instance.upstream_port
			from
			    ham_database_instance slave_instance 
				left join ham_hostname_resolve on (slave_instance.upstream_host = ham_hostname_resolve.hostname) 
				left join ham_database_instance master_instance on (
			    	coalesce(ham_hostname_resolve.resolved_hostname, slave_instance.upstream_host) = master_instance.hostname
			    	and slave_instance.upstream_port = master_instance.port
				)
			where
			    master_instance.last_checked_timestamp is null and 
				slave_instance.upstream_host != '' and 
				slave_instance.upstream_host != '_' and 
				slave_instance.upstream_port > 0 and 
				slave_instance.replication_state = 1
			`,
		nil,
		func(m sqlutil.RowMap) error {
			instanceKey, _ := NewResolveInstanceKey(m.GetString("db_type"), m.GetString("upstream_host"), m.GetInt("upstream_port"))
			// we ignore the error. It can be expected that we are unable to resolve the hostname.
			// Maybe that's how we got here in the first place!
			res = append(res, *instanceKey)
			return nil
		},
	)
	return
}

// ReadClusterInfo reads some info about a given cluster
func ReadClusterInfo(dbt string, clusterName string) (*dtstruct.ClusterInfo, error) {
	clusterList, err := ReadAllClusterInfo(dbt, clusterName)
	if err != nil {
		return &dtstruct.ClusterInfo{}, err
	}
	if len(clusterList) != 1 {
		return &dtstruct.ClusterInfo{}, fmt.Errorf("no cluster info found for %s", clusterName)
	}
	return &(clusterList[0]), nil
}

// ReadAllClusterInfo reads names of all known clusters and some aggregated info
func ReadAllClusterInfo(dbt string, clusterName string) ([]dtstruct.ClusterInfo, error) {
	var clusters []dtstruct.ClusterInfo

	// construct where and query clause
	whereClause := "where 1=1"
	args := sqlutil.Args()
	if dbt != "" {
		whereClause += " and db_type = ?"
		args = append(args, dbt)
	}
	if clusterName != "" {
		whereClause += ` and cluster_name = ?`
		args = append(args, clusterName)
	}
	query := fmt.Sprintf(`
			select
				db_type,
				cluster_name,
				count(*) as count_instances,
				ifnull(min(alias), cluster_name) as alias,
				ifnull(min(domain_name), '') as domain_name
			from
				ham_database_instance
				left join ham_cluster_alias using (cluster_name)
				left join ham_cluster_domain_name using (cluster_name)
			%s
			group by db_type, cluster_name
		`,
		whereClause,
	)

	// query and construct cluster info
	err := db.Query(query, args, func(m sqlutil.RowMap) error {
		clusterInfo := dtstruct.ClusterInfo{
			DatabaseType:   m.GetString("db_type"),
			ClusterName:    m.GetString("cluster_name"),
			CountInstances: m.GetUint("count_instances"),
			ClusterAlias:   m.GetString("alias"),
			ClusterDomain:  m.GetString("domain_name"),
		}
		clusterInfo.ApplyClusterAlias()
		clusterInfo.ReadRecoveryInfo()
		clusters = append(clusters, clusterInfo)
		return nil
	})

	return clusters, err
}

// IsInstanceForgotten check if instance is forgotten
func IsInstanceForgotten(instanceKey *dtstruct.InstanceKey) bool {
	return cache.IsExistInstanceForget(instanceKey.StringCode())
}

// ForgetInstance removes an instance entry from the backed database.
// It may be auto-rediscovered through topology or requested for discovery by multiple means.
func ForgetInstance(instanceKey *dtstruct.InstanceKey) error {

	// check if instance key is nil
	if instanceKey == nil {
		return log.Errorf("cannot forget nil instance")
	}

	// put instance to forget cache and delete from database
	cache.ForgetInstance(instanceKey.StringCode(), true)
	sqlResult, err := db.ExecSQL(`delete from ham_database_instance where hostname = ? and port = ? and cluster_id = ? and db_type = ?`,
		instanceKey.Hostname,
		instanceKey.Port,
		instanceKey.ClusterId,
		instanceKey.DBType,
	)
	if err != nil {
		return err
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return log.Errorf("instance %+v not found", *instanceKey)
	}

	// get cluster name of instance and write to audit
	clusterName, _ := GetClusterName(instanceKey)
	AuditOperation("forget", instanceKey, clusterName, "")
	return nil
}

// GetInstanceKey
func GetInstanceKey(dbt string, host string, port string) (dtstruct.InstanceKey, error) {
	return GetInstanceKeyInternal(dbt, host, port, true)
}

// GetInstanceKeyInternal
func GetInstanceKeyInternal(dbt string, host string, port string, resolve bool) (dtstruct.InstanceKey, error) {
	var instanceKey *dtstruct.InstanceKey
	var err error
	if resolve {
		instanceKey, err = NewResolveInstanceKeyStrings(dbt, host, port)
	} else {
		instanceKey, err = NewRawInstanceKeyStrings(dbt, host, port)
	}
	if err != nil {
		return emptyInstanceKey, err
	}
	instanceKey, err = FigureInstanceKey(instanceKey, nil)
	if err != nil {
		return emptyInstanceKey, err
	}
	if instanceKey == nil {
		return emptyInstanceKey, fmt.Errorf("Unexpected nil instanceKey in getInstanceKeyInternal(%+v, %+v, %+v)", host, port, resolve)
	}
	return *instanceKey, nil
}

//////////////////////////////////////////TODO

func MkInsertOdku(table string, columns []string, values []string, nrRows int, insertIgnore bool) (string, error) {
	if len(columns) == 0 {
		return "", errors.New("Column list cannot be empty")
	}
	if nrRows < 1 {
		return "", errors.New("nrRows must be a positive number")
	}
	if len(columns) != len(values) {
		return "", errors.New("number of values must be equal to number of columns")
	}

	var q bytes.Buffer
	var ignore string = ""
	if insertIgnore {
		ignore = "ignore"
	}
	var valRow string = fmt.Sprintf("(%s)", strings.Join(values, ", "))
	var val bytes.Buffer
	val.WriteString(valRow)
	for i := 1; i < nrRows; i++ {
		val.WriteString(",\n                ") // indent VALUES, see below
		val.WriteString(valRow)
	}

	var col string = strings.Join(columns, ", ")
	var odku bytes.Buffer
	odku.WriteString(fmt.Sprintf("%s=VALUES(%s)", columns[0], columns[0]))
	for _, c := range columns[1:] {
		odku.WriteString(", ")
		odku.WriteString(fmt.Sprintf("%s=VALUES(%s)", c, c))
	}

	q.WriteString(fmt.Sprintf(`INSERT %s INTO %s
                (%s)
        VALUES
                %s
        ON DUPLICATE KEY UPDATE
                %s
        `,
		ignore, table, col, val.String(), odku.String()))

	return q.String(), nil
}

// getClusterName will make a best effort to deduce a cluster name using either a given alias
// or an instanceKey. First attempt is at alias, and if that doesn't work, we try instanceKey.
func GetClusterNameWithAlias(clusterAlias string, instanceKey *dtstruct.InstanceKey, thisInstanceKey *dtstruct.InstanceKey) (clusterName string) {
	clusterName, _ = FigureClusterName(clusterAlias, instanceKey, thisInstanceKey)
	return clusterName
}

// FigureClusterName will make a best effort to deduce a cluster name using either a given alias
// or an instanceKey. First attempt is at alias, and if that doesn't work, we try instanceKey.
// - clusterHint may be an empty string
func FigureClusterName(clusterHint string, instanceKey *dtstruct.InstanceKey, thisInstanceKey *dtstruct.InstanceKey) (clusterName string, err error) {
	// Look for exact matches, first.
	dbt := dtstruct.GetDatabaseType(instanceKey.DBType)
	if clusterHint != "" {
		// Exact cluster name match:
		if clusterInfo, err := ReadClusterInfo(dbt, clusterHint); err == nil && clusterInfo != nil {
			return clusterInfo.ClusterName, nil
		}
		// Exact cluster alias match:
		if clustersInfo, err := ReadAllClusterInfo(dbt, ""); err == nil {
			for _, clusterInfo := range clustersInfo {
				if clusterInfo.ClusterAlias == clusterHint {
					return clusterInfo.ClusterName, nil
				}
			}
		}
	}

	clusterByInstanceKey := func(instanceKey *dtstruct.InstanceKey) (hasResult bool, clusterName string, err error) {
		if instanceKey == nil {
			return false, "", nil
		}
		clusterName, err = FindInstanceClusterName(instanceKey)
		if err != nil {
			return true, clusterName, log.Errore(err)
		}
		if clusterName == "" {
			return true, clusterName, log.Errorf("Unable to determine cluster name for %+v, empty cluster name. clusterHint=%+v", instanceKey, clusterHint)
		}
		return true, clusterName, nil
	}
	// exact instance key:
	if hasResult, clusterName, err := clusterByInstanceKey(instanceKey); hasResult {
		return clusterName, err
	}
	// fuzzy instance key:
	if hasResult, clusterName, err := clusterByInstanceKey(ReadFuzzyInstanceKeyIfPossible(instanceKey)); hasResult {
		return clusterName, err
	}
	//  Let's see about _this_ instance
	if hasResult, clusterName, err := clusterByInstanceKey(thisInstanceKey); hasResult {
		return clusterName, err
	}
	return clusterName, log.Errorf("Unable to determine cluster name. clusterHint=%+v", clusterHint)
}

// FindFuzzyInstances return instances whose names are like the one given (host & port substrings)
// For example, the given `mydb-3:3306` might find `myhosts-mydb301-production.mycompany.com:3306`
func FindInstanceClusterName(instanceKey *dtstruct.InstanceKey) (string, error) {
	readFunc := func() (string, error) {
		var clusterName string
		sql := fmt.Sprintf(`
			%s
		where
			hostname = ? and 
			port = ? and
			db_type = ?
			`, constant.DefaultQueryClusterName)

		err := db.Query(sql, sqlutil.Args(instanceKey.Hostname, instanceKey.Port, instanceKey.DBType), func(m sqlutil.RowMap) error {
			clusterName = m.GetString("cluster_name")
			return nil
		})
		if err != nil {
			return "", log.Errore(err)
		}
		return clusterName, err
	}
	instanceReadChan <- true
	clusterName, err := readFunc()
	<-instanceReadChan
	return clusterName, err
}

// FindFuzzyInstances return instances whose names are like the one given (host & port substrings)
// For example, the given `mydb-3:3306` might find `myhosts-mydb301-production.mycompany.com:3306`
func FindFuzzyInstances(fuzzyInstanceKey *dtstruct.InstanceKey) ([]*dtstruct.InstanceKey, error) {
	readFunc := func() ([]*dtstruct.InstanceKey, error) {
		var instanceKeys []*dtstruct.InstanceKey
		sql := fmt.Sprintf(`
			%s
		where
			hostname like concat('%%', ?, '%%') and 
			port = ?
		order by
			replication_depth asc, downstream_count desc, cluster_name, hostname, port
			`, constant.DefaultQueryFuzzy)

		err := db.Query(sql, sqlutil.Args(fuzzyInstanceKey.Hostname, fuzzyInstanceKey.Port), func(m sqlutil.RowMap) error {
			instanceKey := &dtstruct.InstanceKey{
				DBType:    m.GetString("db_type"),
				Hostname:  m.GetString("hostname"),
				Port:      m.GetInt("port"),
				ClusterId: m.GetString("cluster_id"),
			}
			instanceKeys = append(instanceKeys, instanceKey)
			return nil
		})
		if err != nil {
			return instanceKeys, log.Errore(err)
		}
		return instanceKeys, err
	}
	instanceReadChan <- true
	instances, err := readFunc()
	<-instanceReadChan
	return instances, err
}

// FigureInstanceKey tries to figure out a key
func FigureInstanceKey(instanceKey *dtstruct.InstanceKey, thisInstanceKey *dtstruct.InstanceKey) (*dtstruct.InstanceKey, error) {
	if figuredKey := ReadFuzzyInstanceKeyIfPossible(instanceKey); figuredKey != nil {
		return figuredKey, nil
	}
	figuredKey := thisInstanceKey
	if figuredKey == nil {
		return nil, log.Errorf("Cannot deduce instance %+v", instanceKey)
	}
	return figuredKey, nil
}

// ReadFuzzyInstanceKeyIfPossible accepts a fuzzy instance key and hopes to return a single, fully qualified,
// known instance key, or else the original given key
func ReadFuzzyInstanceKeyIfPossible(fuzzyInstanceKey *dtstruct.InstanceKey) *dtstruct.InstanceKey {
	if instanceKey := ReadFuzzyInstanceKey(fuzzyInstanceKey); instanceKey != nil {
		return instanceKey
	}
	return fuzzyInstanceKey
}

// ReadFuzzyInstanceKey accepts a fuzzy instance key and expects to return a single, fully qualified,
// known instance key.
func ReadFuzzyInstanceKey(fuzzyInstanceKey *dtstruct.InstanceKey) *dtstruct.InstanceKey {
	if fuzzyInstanceKey == nil {
		return nil
	}
	if fuzzyInstanceKey.IsIPv4() {
		// avoid fuzziness. When looking for 10.0.0.1 we don't want to match 10.0.0.15!
		return nil
	}
	if fuzzyInstanceKey.Hostname != "" {
		// Fuzzy instance search
		if fuzzyInstances, _ := FindFuzzyInstances(fuzzyInstanceKey); len(fuzzyInstances) == 1 {
			return fuzzyInstances[0]
		}
	}
	return nil
}

// SnapshotTopologies records topology graph for all existing topologies
func SnapshotTopologies() error {
	writeFunc := func() error {
		_, err := db.ExecSQL(`
        	insert ignore into
        		ham_database_instance_topology_history (snapshot_unix_timestamp,
        			hostname, port, master_host, master_port, cluster_name, version)
        	select
        		UNIX_TIMESTAMP(NOW()),
        		hostname, port, upstream_host, upstream_port, cluster_name, version
			from
				ham_database_instance
				`,
		)
		if err != nil {
			return log.Errore(err)
		}

		return nil
	}
	return db.ExecDBWrite(writeFunc)
}

// ReadHistoryClusterInstances reads (thin) instances from history
func ReadHistoryClusterInstances(clusterName string, historyTimestampPattern string) ([](*dtstruct.Instance), error) {
	instances := [](*dtstruct.Instance){}

	query := `
		select
			*
		from
			ham_database_instance_topology_history
		where
			snapshot_unix_timestamp rlike ?
			and cluster_name = ?
		order by
			hostname, port`

	err := db.Query(query, sqlutil.Args(historyTimestampPattern, clusterName), func(m sqlutil.RowMap) error {
		instance := dtstruct.NewInstance()

		instance.Key.Hostname = m.GetString("hostname")
		instance.Key.Port = m.GetInt("port")
		instance.UpstreamKey.Hostname = m.GetString("upstream_host")
		instance.UpstreamKey.Port = m.GetInt("upstream_port")
		instance.ClusterName = m.GetString("cluster_name")

		instances = append(instances, instance)
		return nil
	})
	if err != nil {
		return instances, log.Errore(err)
	}
	return instances, err
}

// ForgetLongUnseenInstances will remove entries of all instacnes that have long since been last seen.
func ForgetLongUnseenInstances() error {
	sqlResult, err := db.ExecSQL(`
			delete
				from ham_database_instance
			where
				last_seen_timestamp < NOW() - interval ? hour`,
		config.Config.UnseenInstanceForgetHours,
	)
	if err != nil {
		return log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return log.Errore(err)
	}
	AuditOperation("forget-unseen", nil, "", fmt.Sprintf("Forgotten instances: %d", rows))
	return err
}

// ReadAllInstanceKeys
func ReadAllInstanceKeys() ([]dtstruct.InstanceKey, error) {
	var res []dtstruct.InstanceKey
	query := `
		select
			db_type, hostname, port, cluster_id
		from
			ham_database_instance
			`
	err := db.Query(query, sqlutil.Args(), func(m sqlutil.RowMap) error {
		instanceKey, merr := NewResolveInstanceKey(m.GetString("db_type"), m.GetString("hostname"), m.GetInt("port"))
		if merr != nil {
			return merr
		}

		instanceKey.ClusterId = m.GetString("cluster_id")
		if !IsInstanceForgotten(instanceKey) {
			// only if not in "forget" cache
			res = append(res, *instanceKey)
		}
		return nil
	})
	return res, log.Errore(err)
}

// ReadClusters reads names of all known clusters
func ReadClusters(dbt string) (clusterNames []string, err error) {
	clusters, err := ReadAllClusterInfo(dbt, "")
	if err != nil {
		return clusterNames, err
	}
	for _, clusterInfo := range clusters {
		clusterNames = append(clusterNames, clusterInfo.ClusterName)
	}
	return clusterNames, nil
}

// ReadUnknownMasterHostnameResolves will figure out the resolved hostnames of master-hosts which cannot be found.
// It uses the hostname_resolve_history table to heuristically guess the correct hostname (based on "this was the
// last time we saw this hostname and it resolves into THAT")
func ReadUnknownMasterHostnameResolves() (map[string]string, error) {
	res := make(map[string]string)
	err := db.Query(`
			SELECT DISTINCT
			    slave_instance.upstream_host, ham_hostname_resolve_history.resolved_hostname
			FROM
			    ham_database_instance slave_instance
			LEFT JOIN ham_hostname_resolve ON (slave_instance.upstream_host = ham_hostname_resolve.hostname)
			LEFT JOIN ham_database_instance master_instance ON (
			    COALESCE(ham_hostname_resolve.resolved_hostname, slave_instance.upstream_host) = master_instance.hostname
			    and slave_instance.upstream_port = master_instance.port
			) LEFT JOIN ham_hostname_resolve_history ON (slave_instance.upstream_host = ham_hostname_resolve_history.hostname)
			WHERE
			    master_instance.last_checked_timestamp IS NULL
			    and slave_instance.upstream_host != ''
			    and slave_instance.upstream_host != '_'
			    and slave_instance.upstream_port > 0
			`, nil, func(m sqlutil.RowMap) error {
		res[m.GetString("upstream_host")] = m.GetString("resolved_hostname")
		return nil
	})
	if err != nil {
		return res, log.Errore(err)
	}

	return res, nil
}

// ReplaceClusterName replaces all occurances of oldClusterName with newClusterName
// It is called after a master failover
func ReplaceClusterName(oldClusterName string, newClusterName string) error {
	if oldClusterName == "" {
		return log.Errorf("replaceClusterName: skipping empty oldClusterName")
	}
	if newClusterName == "" {
		return log.Errorf("replaceClusterName: skipping empty newClusterName")
	}
	writeFunc := func() error {
		_, err := db.ExecSQL(`
			update
				ham_database_instance
			set
				cluster_name=?
			where
				cluster_name=?
				`, newClusterName, oldClusterName,
		)
		if err != nil {
			return log.Errore(err)
		}
		AuditOperation("replace-cluster-name", nil, newClusterName, fmt.Sprintf("replaxced %s with %s", oldClusterName, newClusterName))
		return nil
	}
	return db.ExecDBWrite(writeFunc)
}

// WithCurrentTime reads and returns the current timestamp as string. This is an unfortunate workaround
//	// to support both MySQL and SQLite in all possible timezones. SQLite only speaks UTC where MySQL has
//	// timezone support. By reading the time as string we get the database's de-facto notion of the time,
//	// which we can then feed back to it.
func WithCurrentTime(cdi *dtstruct.CandidateDatabaseInstance) *dtstruct.CandidateDatabaseInstance {
	_ = db.Query(`select now() as time_now`, nil, func(m sqlutil.RowMap) error {
		cdi.LastSuggestedString = m.GetString("time_now")
		return nil
	})
	return cdi
}
