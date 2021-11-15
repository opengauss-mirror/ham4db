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
	aodtstruct "gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/dtstruct"
	"gitee.com/opengauss/ham4db/go/core/agent"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"google.golang.org/grpc"
)

// NodeOperation connect to agent, and send action request to agent
func NodeOperation(ctx context.Context, instanceKey *dtstruct.InstanceKey, operationType aodtstruct.ActionType) (detail interface{}, err error) {

	// audit this operation
	base.AuditOperation(aodtstruct.ActionType_name[int32(operationType)], instanceKey, "", instanceKey.String())

	// connect to agent
	var conn *grpc.ClientConn
	if conn, err = agent.ConnectToAgent(instanceKey); err != nil {
		return instanceKey, err
	}

	// operation node
	return aodtstruct.NewOpengaussAgentClient(conn).ManageAction(ctx, &aodtstruct.ManageActionRequest{ActionType: operationType})
}
