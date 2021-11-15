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
package v2d0d1

import (
	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/common/constant"
	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/core"
	aodtstruct "gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/dtstruct"
	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/util"
	"gitee.com/opengauss/ham4db/go/core/log"
	"strings"
)

// CollectInfo collect node info
func CollectInfo(req *aodtstruct.DatabaseInfoRequest, nodeInfoHookList []aodtstruct.HookFunction) (*aodtstruct.DatabaseInfoResponse, error) {
	if nodeInfo, err := core.GetNodeInfo(req, nodeInfoHookList); err == nil {
		return &aodtstruct.DatabaseInfoResponse{NodeInfo: nodeInfo}, nil
	} else {
		return nil, err
	}
}

// Action do action on opengauss instance, TODO check if cmd has been executed successfully
func Action(actType aodtstruct.ActionType, dataPathHook aodtstruct.HookFunction, cmdHook aodtstruct.HookFunction) (*aodtstruct.ManageActionResponse, error) {

	// get data dir path
	dataPath := core.GetDataPath(dataPathHook)
	if dataPath == "" {
		return nil, log.Errorf("cannot get data path")
	}

	var err error
	switch actType {
	case aodtstruct.ActionType_STOP:
		_, err = core.ExecCmd(cmdHook, "gs_ctl", "stop", "-D", dataPath)
	case aodtstruct.ActionType_START:
		_, err = core.ExecCmd(cmdHook, "gs_ctl", "start", "-D", dataPath)
	case aodtstruct.ActionType_RESTART:
		_, err = core.ExecCmd(cmdHook, "gs_ctl", "restart", "-D", dataPath)
	case aodtstruct.ActionType_FAILOVER:
		_, err = core.ExecCmd(cmdHook, "gs_ctl", "failover", "-D", dataPath)
	case aodtstruct.ActionType_SWITCHOVER:
		_, err = core.ExecCmd(cmdHook, "gs_ctl", "switchover", "-D", dataPath)
	case aodtstruct.ActionType_START_BY_PRIMARY:
		_, err = core.ExecCmd(cmdHook, "gs_ctl", "start", "-D", dataPath, "-M", "primary")
	case aodtstruct.ActionType_START_BY_STANDBY:
		_, err = core.ExecCmd(cmdHook, "gs_ctl", "start", "-D", dataPath, "-M", "standby")
	case aodtstruct.ActionType_START_BY_CASCADE:
		_, err = core.ExecCmd(cmdHook, "gs_ctl", "start", "-D", dataPath, "-M", "cascade_standby")
	case aodtstruct.ActionType_START_BY_PENDING:
		_, err = core.ExecCmd(cmdHook, "gs_ctl", "start", "-D", dataPath, "-M", "pending")
	case aodtstruct.ActionType_START_CLUSTER:
		_, err = core.ExecCmd(cmdHook, "gs_om", "-t", "start")
	case aodtstruct.ActionType_BUILD:
		_, err = core.ExecCmd(cmdHook, "gs_ctl", "build", "-D", dataPath, "-b", "auto")
	case aodtstruct.ActionType_BUILD_BY_PRIMARY:
		_, err = core.ExecCmd(cmdHook, "gs_ctl", "build", "-D", dataPath, "-b", "auto", "-M", "primary")
	case aodtstruct.ActionType_BUILD_BY_STANDBY:
		_, err = core.ExecCmd(cmdHook, "gs_ctl", "build", "-D", dataPath, "-b", "auto", "-M", "standby")
	case aodtstruct.ActionType_BUILD_BY_CASCADE:
		_, err = core.ExecCmd(cmdHook, "gs_ctl", "build", "-D", dataPath, "-b", "auto", "-M", "cascade_standby")
	default:
		err = log.Errorf("action:%s not supported now", actType)
	}
	return &aodtstruct.ManageActionResponse{}, err
}

// UpdateSyncConfig update sync node config
func UpdateSyncConfig(syncConf string, dataPathHook aodtstruct.HookFunction) (*aodtstruct.UpdateSyncConfigResponse, error) {
	stdOut, err := core.ExecCmd(nil, "gs_guc", "reload", "-D", core.GetDataPath(dataPathHook), "-c", "synchronous_standby_names='"+syncConf+"'")
	if err != nil {
		return nil, err
	}
	return &aodtstruct.UpdateSyncConfigResponse{SyncStandby: strings.Join(stdOut, "\n")}, err
}

// ReplicationConfirm confirm if replication between instance in request and local is normal
func ReplicationConfirm(req *aodtstruct.ReplicationConfirmRequest, hookFs []aodtstruct.HookFunction) (*aodtstruct.ReplicationConfirmResponse, error) {

	// check hook functions
	if hookFs == nil || len(hookFs) != 3 {
		hookFs = []aodtstruct.HookFunction{nil, nil, nil}
	}

	// get upstream and downstream info
	command := "gs_ctl query -D " + core.GetDataPath(hookFs[0])
	result, err := core.ExecCmd(hookFs[1], "sh", "-c", command)
	if err != nil {
		return nil, err
	}

	// collect sender and receiver info from all channel
	replDirect := constant.ReplDirectDownStream
	if req.Upstream {
		replDirect = constant.ReplDirectUpstream
	}

	// get all instance hostname/ip -> port map
	portAndNodeNameMap, err := core.GetAllPortAndNodeName(hookFs[2])
	if err != nil {
		return nil, err
	}

	// check if replication between failed instance and local instance is exist
	for _, line := range result {
		if strings.Contains(line, "channel") && strings.Contains(line, replDirect) {
			if streamBaseInfo, errU := util.ParseChannel(line, replDirect, portAndNodeNameMap); errU == nil {
				if streamBaseInfo.NodeName == req.ConfirmNodename {
					return &aodtstruct.ReplicationConfirmResponse{Normal: true}, nil
				}
			} else {
				log.Error("parse channel:%s, error:%s", line, errU)
			}
		}
	}
	return &aodtstruct.ReplicationConfirmResponse{Normal: false}, nil
}
