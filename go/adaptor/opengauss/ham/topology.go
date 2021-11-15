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
	"database/sql"
	"fmt"
	oconstant "gitee.com/opengauss/ham4db/go/adaptor/opengauss/common/constant"
	aodtstruct "gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/dtstruct"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"time"
)

// OpenTopology get db client to remote opengauss instance
func OpenTopology(host string, port int, args ...interface{}) (db *sql.DB, err error) {
	uri := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
		host,
		port,
		config.Config.MysqlTopologyUser,
		config.Config.MysqlTopologyPassword,
	)
	if db, _, err = sqlutil.GetGenericDB(oconstant.DBTOpenGauss, uri); err != nil {
		return nil, err
	}
	if config.Config.ConnectionLifetimeSeconds > 0 {
		db.SetConnMaxLifetime(time.Duration(config.Config.ConnectionLifetimeSeconds) * time.Second)
	}
	db.SetMaxOpenConns(config.TopologyMaxPoolConnections)
	db.SetMaxIdleConns(config.TopologyMaxPoolConnections)
	return db, err
}

// RelocateBelow change upstream and downstream role by switch over
func RelocateBelow(downstreamInstKey, upstreamInstKey *dtstruct.InstanceKey) (interface{}, error) {
	return NodeOperation(context.TODO(), downstreamInstKey, aodtstruct.ActionType_SWITCHOVER)
}

// Topology show opengauss topology
func Topology(request *dtstruct.Request, historyTimestampPattern string, tabulated bool, printTags bool) (result interface{}, err error) {

	// instance map group by role
	instanceMap := make(map[string][]interface{})

	// get data from database
	if err = db.Query(`
		select cluster_id, db_type, hostname, port, db_role
		from 
			ham_database_instance 
		where
			cluster_id = ? or (cluster_id like ? or cluster_name like ?)
		order by 
			db_role
	`,
		sqlutil.Args(request.Hint, request.Hint, request.Hint), func(rowMap sqlutil.RowMap) error {
			role := rowMap.GetString("db_role")
			instanceMap[role] = append(instanceMap[role], dtstruct.InstanceKey{
				ClusterId: rowMap.GetString("cluster_id"),
				DBType:    rowMap.GetString("db_type"),
				Hostname:  rowMap.GetString("hostname"),
				Port:      rowMap.GetInt("port"),
			})
			return nil
		}); err != nil {
		return nil, err
	}
	return instanceMap, nil
}
