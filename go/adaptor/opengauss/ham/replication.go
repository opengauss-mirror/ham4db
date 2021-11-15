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
	oconstant "gitee.com/opengauss/ham4db/go/adaptor/opengauss/common/constant"
	aodtstruct "gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/dtstruct"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
)

// StartReplica
func StartReplica(ctx context.Context, instanceKey *dtstruct.InstanceKey) (interface{}, error) {

	// get role from backend database
	var role string
	if err := db.Query(`
				select 
					db_role
				from 
					ham_database_instance 
				where
					hostname = ? and port = ? and db_type = ?
		`,
		sqlutil.Args(instanceKey.Hostname, instanceKey.Port, instanceKey.DBType),
		func(rowMap sqlutil.RowMap) error {
			role = rowMap.GetString("db_role")
			return nil
		},
	); err != nil {
		return nil, err
	}

	// get action type according to role
	var actionType aodtstruct.ActionType
	switch role {
	case oconstant.DBStandby:
		actionType = aodtstruct.ActionType_START_BY_STANDBY
	case oconstant.DBCascade:
		actionType = aodtstruct.ActionType_START_BY_CASCADE
	case oconstant.DBPrimary:
		actionType = aodtstruct.ActionType_START_BY_PRIMARY
	case "":
		return nil, log.Errorf("role is empty for instance:%v", instanceKey)
	default:
		return nil, log.Errorf("role:%s is not supported for instance:%v", role, instanceKey)
	}

	// exec node operation, start and build
	if _, err := NodeOperation(ctx, instanceKey, actionType); err != nil {
		return nil, err
	}

	return NodeOperation(ctx, instanceKey, aodtstruct.ActionType_BUILD)
}

// RestartReplication restart replication
func RestartReplication(ctx context.Context, instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return NodeOperation(ctx, instanceKey, aodtstruct.ActionType_RESTART)
}

// StopReplication stop replication
func StopReplication(ctx context.Context, instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return NodeOperation(ctx, instanceKey, aodtstruct.ActionType_STOP)
}
