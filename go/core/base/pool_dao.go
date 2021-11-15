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
	"fmt"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"strings"
	"time"
)

// ReadAllClusterPoolInstance returns all clusters-pools-instances associations
func ReadAllClusterPoolInstance() ([]*dtstruct.ClusterPoolInstance, error) {
	return ReadClusterPoolInstance("", "")
}

// ReadClusterPoolInstancesMap returns association of pools-to-instances for a given cluster
// and potentially for a given pool.
func ReadClusterPoolInstancesMap(clusterName string, pool string) (*dtstruct.PoolInstancesMap, error) {
	poolInstMap := make(dtstruct.PoolInstancesMap)

	// get instance in cluster and pool
	poolInstList, err := ReadClusterPoolInstance(clusterName, pool)
	if err != nil {
		return &poolInstMap, err
	}

	// construct instance key and append it to pool
	for _, clusterPoolInstance := range poolInstList {
		if _, ok := poolInstMap[clusterPoolInstance.Pool]; !ok {
			poolInstMap[clusterPoolInstance.Pool] = []*dtstruct.InstanceKey{}
		}
		poolInstMap[clusterPoolInstance.Pool] = append(poolInstMap[clusterPoolInstance.Pool], &dtstruct.InstanceKey{Hostname: clusterPoolInstance.Hostname, Port: clusterPoolInstance.Port})
	}

	return &poolInstMap, nil
}

// ReadClusterPoolInstance reads cluster-pool-instance associations for given cluster and pool
func ReadClusterPoolInstance(clusterName string, pool string) (result []*dtstruct.ClusterPoolInstance, err error) {
	args := sqlutil.Args()
	whereClause := ``
	if clusterName != "" {
		whereClause = " where ham_database_instance.cluster_name = ? and ? in ('', pool)"
		args = append(args, clusterName, pool)
	}
	query := fmt.Sprintf(`
		select
			cluster_name,
			ifnull(alias, cluster_name) as alias,
			ham_database_instance_pool.*
		from
			ham_database_instance
			join ham_database_instance_pool using (hostname, port)
			left join ham_cluster_alias using (cluster_name)
		%s
		`, whereClause)
	err = db.Query(query, args, func(m sqlutil.RowMap) error {
		result = append(result, &dtstruct.ClusterPoolInstance{
			ClusterName:  m.GetString("cluster_name"),
			ClusterAlias: m.GetString("alias"),
			Pool:         m.GetString("pool"),
			Hostname:     m.GetString("hostname"),
			Port:         m.GetInt("port"),
		})
		return nil
	})
	return
}

// ApplyPoolInstance
func ApplyPoolInstance(submission *dtstruct.PoolInstancesSubmission) error {

	// already expired, no need to persist
	if submission.CreatedAt.Add(time.Duration(config.Config.InstancePoolExpiryMinutes) * time.Minute).Before(time.Now()) {
		return nil
	}

	// get instance key from submission's delimited instance
	var instanceKeyList []*dtstruct.InstanceKey
	if submission.DelimitedInstances != "" {
		for _, instanceString := range strings.Split(submission.DelimitedInstances, ",") {
			instanceString = strings.TrimSpace(instanceString)
			instanceKey, err := ParseResolveInstanceKey(submission.DatabaseType, instanceString)
			if err != nil {
				return err
			}
			if config.Config.SupportFuzzyPoolHostnames {
				instanceKey = ReadFuzzyInstanceKeyIfPossible(instanceKey)
			}
			instanceKeyList = append(instanceKeyList, instanceKey)
		}
	}

	// write to database
	return WritePoolInstance(submission.Pool, instanceKeyList)
}

// WritePoolInstance will write (and override) a single cluster name mapping
func WritePoolInstance(pool string, instKeyList []*dtstruct.InstanceKey) error {
	return db.ExecDBWrite(func() (err error) {
		multiSQL := &dtstruct.MultiSQL{}

		// delete pool
		multiSQL.Query = append(multiSQL.Query, "delete from ham_database_instance_pool where pool = ?")
		multiSQL.Args = append(multiSQL.Args, sqlutil.Args(pool))

		// insert all instance to pool
		for _, instanceKey := range instKeyList {
			multiSQL.Query = append(multiSQL.Query, "insert into ham_database_instance_pool (hostname, port, pool, register_timestamp) values (?, ?, ?, current_timestamp)")
			multiSQL.Args = append(multiSQL.Args, sqlutil.Args(instanceKey.Hostname, instanceKey.Port, pool))
		}

		// exec it
		return db.ExecMultiSQL(multiSQL)
	})
}

// ExpirePoolInstance clean up the database_instance_pool table from expired items
func ExpirePoolInstance() (err error) {
	_, err = db.ExecSQL(`delete from ham_database_instance_pool where register_timestamp < now() - interval ? minute `, config.Config.InstancePoolExpiryMinutes)
	return
}
