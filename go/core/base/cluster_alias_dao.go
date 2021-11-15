/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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
	"fmt"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
)

// WriteClusterAliasManualOverride will write (and override) a single cluster name mapping
func WriteClusterAliasManualOverride(clusterName string, alias string) error {
	return db.ExecDBWrite(
		func() error {
			_, err := db.ExecSQL(`
				replace into
					ham_cluster_alias_override (cluster_name, alias)
				values
					(?, ?)
			`, clusterName, alias,
			)
			return err
		},
	)
}

// ReadUnambiguousSuggestedClusterAlias reads potential master hostname:port who have suggested cluster aliases,
// where no one else shares said suggested cluster alias. Such hostname:port are likely true owners
// of the alias.
func ReadUnambiguousSuggestedClusterAlias() (result map[string]dtstruct.InstanceKey, err error) {
	result = map[string]dtstruct.InstanceKey{}
	query := `
		select cluster_alias, min(hostname) as hostname, min(port) as port
		from ham_database_instance
		where cluster_alias != '' and replication_depth = 0
		group by cluster_alias having count(*) = 1
	`
	err = db.Query(query, sqlutil.Args(), func(m sqlutil.RowMap) error {
		result[m.GetString("cluster_alias")] = dtstruct.InstanceKey{Hostname: m.GetString("hostname"), Port: m.GetInt("port")}
		return nil
	})
	return result, err
}

// GetClusterMasterKVPair construct master kv pair using prefix in config
func GetClusterMasterKVPair(clusterAlias string, masterKey *dtstruct.InstanceKey) *dtstruct.KVPair {
	if clusterAlias == "" {
		return nil
	}
	if masterKey == nil {
		return nil
	}
	return dtstruct.NewKVPair(fmt.Sprintf("%s%s", config.Config.KVClusterMasterPrefix, clusterAlias), masterKey.StringCode())
}

//======================TODO

// ReadClusterNameByAlias
func ReadClusterNameByAlias(alias string) (clusterName string, err error) {
	query := `
		select
			cluster_name
		from
			ham_cluster_alias
		where
			alias = ?
			or cluster_name = ?
		`
	err = db.Query(query, sqlutil.Args(alias, alias), func(m sqlutil.RowMap) error {
		clusterName = m.GetString("cluster_name")
		return nil
	})
	if err != nil {
		return "", err
	}
	if clusterName == "" {
		err = fmt.Errorf("No cluster found for alias %s", alias)
	}
	return clusterName, err
}

// DeduceClusterName attempts to resolve a cluster name given a name or alias.
// If unsuccessful to match by alias, the function returns the same given string
func DeduceClusterName(nameOrAlias string) (clusterName string, err error) {
	if nameOrAlias == "" {
		return "", fmt.Errorf("empty cluster name")
	}
	if name, err := ReadClusterNameByAlias(nameOrAlias); err == nil {
		return name, nil
	}
	return nameOrAlias, nil
}

// ReadAliasByClusterName returns the cluster alias for the given cluster name,
// or the cluster name itself if not explicit alias found
func ReadAliasByClusterName(clusterName string) (alias string, err error) {
	alias = clusterName // default return value
	query := `
		select
			alias
		from
			ham_cluster_alias
		where
			cluster_name = ?
		`
	err = db.Query(query, sqlutil.Args(clusterName), func(m sqlutil.RowMap) error {
		alias = m.GetString("alias")
		return nil
	})
	return clusterName, err
}

// WriteClusterAlias will write (and override) a single cluster name mapping
func WriteClusterAlias(clusterName string, alias string) error {
	writeFunc := func() error {
		_, err := db.ExecSQL(`
			replace into
					ham_cluster_alias (cluster_name, alias, last_register_timestamp)
				values
					(?, ?, now())
			`,
			clusterName, alias)
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}

// UpdateClusterAliases writes down the cluster_alias table based on information
// gained from ham_database_instance
func UpdateClusterAliases() error {
	writeFunc := func() error {
		_, err := db.ExecSQL(`
			replace into
					ham_cluster_alias (alias, cluster_name, last_register_timestamp)
				select
				    cluster_alias,
						cluster_name,
						now()
					from
				    ham_database_instance
				    left join ham_database_instance_downtime using (hostname, port)
				  where
				    cluster_alias!=''
						/* exclude newly demoted, downtimed masters */
						and ifnull(
								ham_database_instance_downtime.downtime_active = 1
								and ham_database_instance_downtime.end_timestamp > now()
								and ham_database_instance_downtime.reason = ?
							, 0) = 0
					order by
						ifnull(last_checked_timestamp <= last_seen_timestamp, 0) asc,
						is_read_only desc,
						downstream_count asc
			`, constant.DowntimeReasonLostInRecovery)
		return log.Errore(err)
	}
	if err := db.ExecDBWrite(writeFunc); err != nil {
		return err
	}
	writeFunc = func() error {
		// Handling the case where no cluster alias exists: we write a dummy alias in the form of the real cluster name.
		_, err := db.ExecSQL(`
			replace into
					ham_cluster_alias (alias, cluster_name, last_register_timestamp)
				select
						cluster_name as alias, cluster_name, now()
				  from
				    ham_database_instance
				  group by
				    cluster_name
					having
						sum(cluster_alias = '') = count(*)
			`)
		return log.Errore(err)
	}
	if err := db.ExecDBWrite(writeFunc); err != nil {
		return err
	}
	return nil
}

// ForgetLongUnseenClusterAliases will remove entries of cluster_aliases that have long since been last seen.
// This function is compatible with ForgetLongUnseenInstances
func ForgetLongUnseenClusterAliases() error {
	sqlResult, err := db.ExecSQL(`
			delete
				from ham_cluster_alias
			where
			last_register_timestamp < NOW() - interval ? hour`,
		config.Config.UnseenInstanceForgetHours,
	)
	if err != nil {
		return log.Errore(err)
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return log.Errore(err)
	}
	AuditOperation("forget-clustr-aliases", nil, "", fmt.Sprintf("Forgotten aliases: %d", rows))
	return err
}

// GetClusterMasterKVPairs returns all KV pairs associated with a master. This includes the
// full identity of the master as well as a breakdown by hostname, port, ipv4, ipv6
func GetClusterMasterKVPairs(clusterAlias string, masterKey *dtstruct.InstanceKey) (kvPairs []*dtstruct.KVPair) {
	masterKVPair := GetClusterMasterKVPair(clusterAlias, masterKey)
	if masterKVPair == nil {
		return kvPairs
	}
	kvPairs = append(kvPairs, masterKVPair)

	addPair := func(keySuffix, value string) {
		key := fmt.Sprintf("%s/%s", masterKVPair.Key, keySuffix)
		kvPairs = append(kvPairs, dtstruct.NewKVPair(key, value))
	}

	addPair("hostname", masterKey.Hostname)
	addPair("port", fmt.Sprintf("%d", masterKey.Port))
	if ipv4, ipv6, err := ReadHostnameIPs(masterKey.Hostname); err == nil {
		addPair("ipv4", ipv4)
		addPair("ipv6", ipv6)
	}
	return kvPairs
}
