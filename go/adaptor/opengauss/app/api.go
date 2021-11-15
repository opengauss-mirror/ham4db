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
package app

import (
	"context"
	"fmt"
	oconstant "gitee.com/opengauss/ham4db/go/adaptor/opengauss/common/constant"
	"gitee.com/opengauss/ham4db/go/adaptor/opengauss/ham"
	aodtstruct "gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/dtstruct"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/render"
	"net/http"
	"time"
)

// RegisterAPIRequest register api request
func RegisterAPIRequest() map[string][]interface{} {
	return map[string][]interface{}{

		// build
		oconstant.DBTOpenGauss + "/build/:clusterId/:host/:port/:role": {http.MethodPut, NodeBuild},

		// node start or stop
		oconstant.DBTOpenGauss + "/start/:clusterId/:host/:port/:role": {http.MethodPut, NodeStart},
		oconstant.DBTOpenGauss + "/stop/:clusterId/:host/:port":        {http.MethodPut, NodeStop},

		// recovery
		oconstant.DBTOpenGauss + "/switchover/:clusterId/:host/:port": {http.MethodPut, SwitchOver},
		oconstant.DBTOpenGauss + "/failover/:clusterId/:host/:port":   {http.MethodPut, FailOver},
	}
}

// RegisterWebRequest register web request
func RegisterWebRequest() map[string][]interface{} {
	return map[string][]interface{}{}
}

// NodeBuild build node using specified role
func NodeBuild(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	Operation(params, r, req, user, true, func(ctx context.Context, instanceKey *dtstruct.InstanceKey) (interface{}, error) {
		switch params["role"] {
		case oconstant.OpenGaussRolePrimary:
			return ham.NodeOperation(ctx, instanceKey, aodtstruct.ActionType_BUILD)
		case oconstant.OpenGaussRoleStandby:
			return ham.NodeOperation(ctx, instanceKey, aodtstruct.ActionType_BUILD)
		case oconstant.OpenGaussRoleCascade:
			return ham.NodeOperation(ctx, instanceKey, aodtstruct.ActionType_BUILD)
		default:
			return instanceKey, log.Errorf("role:%s not supported", params["role"])
		}
	})
}

// NodeStart start node using specified role
func NodeStart(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	Operation(params, r, req, user, true, func(ctx context.Context, instanceKey *dtstruct.InstanceKey) (interface{}, error) {
		switch params["role"] {
		case oconstant.OpenGaussRolePrimary:
			return ham.NodeOperation(ctx, instanceKey, aodtstruct.ActionType_START_BY_PRIMARY)
		case oconstant.OpenGaussRoleStandby:
			return ham.NodeOperation(ctx, instanceKey, aodtstruct.ActionType_START_BY_STANDBY)
		case oconstant.OpenGaussRoleCascade:
			return ham.NodeOperation(ctx, instanceKey, aodtstruct.ActionType_START_BY_CASCADE)
		default:
			return instanceKey, log.Errorf("role:%s not supported", params["role"])
		}
	})
}

// NodeStop stop node
func NodeStop(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	Operation(params, r, req, user, false, func(ctx context.Context, instanceKey *dtstruct.InstanceKey) (interface{}, error) {
		return ham.NodeOperation(ctx, instanceKey, aodtstruct.ActionType_STOP)
	})
}

// SwitchOver do switch over on node
func SwitchOver(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	Operation(params, r, req, user, false, func(ctx context.Context, instanceKey *dtstruct.InstanceKey) (interface{}, error) {
		return ham.NodeOperation(ctx, instanceKey, aodtstruct.ActionType_SWITCHOVER)
	})
}

// FailOver do failover on node
func FailOver(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	Operation(params, r, req, user, false, func(ctx context.Context, instanceKey *dtstruct.InstanceKey) (interface{}, error) {
		return ham.NodeOperation(ctx, instanceKey, aodtstruct.ActionType_FAILOVER)
	})
}

// Operation do operation on specified instance
func Operation(params martini.Params, r render.Render, req *http.Request, user auth.User, roleCheck bool, operation func(context.Context, *dtstruct.InstanceKey) (interface{}, error)) {
	ctx, cancel := context.WithTimeout(context.TODO(), oconstant.DefaultTimeout*time.Second)
	defer cancel()

	// set database type
	params["type"] = oconstant.DBTOpenGauss

	// check and generate instance key
	instanceKey, err := base.AuthCheckAndGetInstanceKeyWithClusterId(params, r, req, user, true)
	if instanceKey == nil {
		return
	}

	// check role
	if roleCheck && params["role"] == "" {
		base.Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, constant.HttpRespMsgMissRole, params))
		return
	}

	// do operation
	optRslt, err := operation(ctx, instanceKey)
	if err != nil {
		base.Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, err.Error(), ""))
		return
	}

	// response it
	base.Respond(r, dtstruct.NewApiResponse(dtstruct.OK, fmt.Sprintf("operation on:%+v", instanceKey), optRslt))
}
