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
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	orcraft "gitee.com/opengauss/ham4db/go/core/consensus/raft"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/core/security/token"
	"gitee.com/opengauss/ham4db/go/core/system/osp"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	gutil "gitee.com/opengauss/ham4db/go/util"
	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/render"
	"net/http"
	"strconv"
	"strings"
)

func Respond(r render.Render, apiResponse *dtstruct.APIResponse) {
	r.JSON(apiResponse.Code.HttpStatus(), apiResponse)
}

// IsAuthorizedForAction checks req to see whether authenticated user has write-privileges.
// This depends on configured authentication method.
func IsAuthorizedForAction(req *http.Request, user auth.User) bool {
	if config.Config.ReadOnly {
		return false
	}

	if orcraft.IsRaftEnabled() && !orcraft.IsLeader() {
		// A raft member that is not a leader is unauthorized.
		return false
	}

	switch strings.ToLower(config.Config.AuthenticationMethod) {
	case "basic":
		{
			// The mere fact we're here means the user has passed authentication
			return true
		}
	case "multi":
		{
			if string(user) == "readonly" {
				// read only
				return false
			}
			// passed authentication ==> writeable
			return true
		}
	case "proxy":
		{
			authUser := GetProxyAuthUser(req)
			for _, configPowerAuthUser := range config.Config.PowerAuthUsers {
				if configPowerAuthUser == "*" || configPowerAuthUser == authUser {
					return true
				}
			}
			// check the user's group is one of those listed here
			if len(config.Config.PowerAuthGroups) > 0 && osp.UserInGroups(authUser, config.Config.PowerAuthGroups) {
				return true
			}
			return false
		}
	case "token":
		{
			cookie, err := req.Cookie("access-token")
			if err != nil {
				return false
			}

			publicToken := strings.Split(cookie.Value, ":")[0]
			secretToken := strings.Split(cookie.Value, ":")[1]
			result, _ := token.IsTokenValid(publicToken, secretToken)
			return result
		}
	case "oauth":
		{
			return false
		}
	default:
		{
			// Default: no authentication method
			return true
		}
	}
}

// AuthCheck check user authorization
func AuthCheck(r render.Render, req *http.Request, user auth.User) bool {

	// user authorized check
	if !IsAuthorizedForAction(req, user) {
		Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, constant.HttpRespMsgUnAuth, nil))
		return false
	}
	return true
}

// AuthCheckAndGetInstanceKey check user authorization and generate instance key using params
func AuthCheckAndGetInstanceKey(params martini.Params, r render.Render, req *http.Request, user auth.User, resolve bool) (*dtstruct.InstanceKey, error) {

	// user authorized check
	if !AuthCheck(r, req, user) {
		return nil, nil
	}

	// get instance key using params
	instanceKey, err := GetInstanceKeyInternal(params["type"], params["host"], params["port"], resolve)
	if err != nil {
		Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, err.Error(), nil))
		return nil, log.Errore(err)
	}

	// set cluster id by param and reset if not exist in param
	if params["clusterId"] != "" {
		instanceKey.ClusterId = params["clusterId"]
	} else {
		instanceKey.ClusterId = gutil.RandomHash32()
	}
	return &instanceKey, nil
}

// AuthCheckAndGetInstanceKeyWithClusterId check user authorization and cluster id, generate instance key using params
func AuthCheckAndGetInstanceKeyWithClusterId(params martini.Params, r render.Render, req *http.Request, user auth.User, resolve bool) (*dtstruct.InstanceKey, error) {

	// user authorized check
	if !AuthCheck(r, req, user) {
		return nil, nil
	}

	// check cluster id
	if params["clusterId"] == "" {
		Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, constant.HttpRespMsgMissClusterId, nil))
		return nil, nil
	}

	// get instance key using params, set cluster id and return
	instanceKey, err := GetInstanceKeyInternal(params["type"], params["host"], params["port"], resolve)
	if err != nil {
		Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, err.Error(), nil))
		return nil, log.Errore(err)
	}
	instanceKey.ClusterId = params["clusterId"]
	return &instanceKey, nil
}

// AuthCheckAndGetInstanceKeyWithClusterId check user authorization and cluster id, generate instance key using params
func AuthCheckAndGetRequest(params martini.Params, r render.Render, req *http.Request, user auth.User) (request *dtstruct.Request, err error) {

	// user authorized check
	if !AuthCheck(r, req, user) {
		return nil, nil
	}

	// check port
	var portInt int
	if params["port"] != "" {
		if portInt, err = strconv.Atoi(params["port"]); err != nil {
			Respond(r, dtstruct.NewApiResponse(dtstruct.ERROR, err.Error(), nil))
			return nil, fmt.Errorf("invalid port: %s", params["port"])
		}
	}
	return &dtstruct.Request{
		InstanceKey: &dtstruct.InstanceKey{DBType: params["type"], ClusterId: params["clusterId"], Hostname: params["host"], Port: portInt},
		Hint:        params["hint"],
	}, nil
}

func GetProxyAuthUser(req *http.Request) string {
	for _, user := range req.Header[config.Config.AuthUserHeader] {
		return user
	}
	return ""
}
