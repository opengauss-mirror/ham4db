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
package core

import (
	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/common/constant"
	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/dtstruct"
	"strings"

	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/util"
	"gitee.com/opengauss/ham4db/go/core/log"
	"github.com/go-cmd/cmd"
)

var baseInfo *dtstruct.BaseInfo

func init() {
	baseInfo = &dtstruct.BaseInfo{}
}

// GetNodeInfo get node info
func GetNodeInfo(req *dtstruct.DatabaseInfoRequest, hookFs []dtstruct.HookFunction) (*dtstruct.NodeInfo, error) {

	// check hook functions
	if hookFs == nil || len(hookFs) != 5 {
		hookFs = []dtstruct.HookFunction{nil, nil, nil, nil, nil}
	}

	// init node info
	nodeInfo := &dtstruct.NodeInfo{
		BaseInfo: &dtstruct.BaseInfo{}, SyncInfo: &dtstruct.SyncInfo{}, Upstream: &dtstruct.BaseInfo{}, DownstreamList: []*dtstruct.BaseInfo{},
	}

	// get base info
	bi, err := GetBaseInfo(hookFs[:3], !req.FromCache)
	if err != nil {
		return nil, err
	}
	nodeInfo.BaseInfo = bi

	// get upstream and downstream info
	command := "gs_ctl query -D " + nodeInfo.BaseInfo.DataPath
	result, err := ExecCmd(hookFs[3], "sh", "-c", command)
	if err != nil {
		return nil, err
	}

	// locate receiver info and collect all channel
	var syncChannel []string
	recIdx := -1
	for i, line := range result {
		if strings.Contains(line, "Receiver info") {
			recIdx = i
			continue
		}
		if strings.Contains(line, "channel") {
			syncChannel = append(syncChannel, line)
		}
	}

	// get sync info
	infoMap, err := util.OutputToMap(result[recIdx+1:], []int{-1}, constant.OutputTypeCmd)
	if err != nil {
		return nil, err
	}

	// refactor some key
	if _, ok := infoMap["SyncPercent"]; ok {
		infoMap["SyncPercent"] = strings.Trim(infoMap["SyncPercent"], "%")
	}

	// update node sync info
	if err = util.ResetValue(nodeInfo.SyncInfo, infoMap, true); err != nil {
		return nil, err
	}

	// get all instance hostname/ip -> port map
	portAndNodeNameMap, err := GetAllPortAndNodeName(hookFs[4])
	if err != nil {
		return nil, err
	}

	// get upstream and downstream
	for _, stream := range syncChannel {

		// upstream
		if strings.Contains(stream, "<--") {
			if upstreamBaseInfo, errU := util.ParseChannel(stream, "<--", portAndNodeNameMap); errU == nil {
				nodeInfo.Upstream = upstreamBaseInfo
			} else {
				return nil, errU
			}
			continue
		}
		// downstream
		if strings.Contains(stream, "-->") {
			if downstreamBaseInfo, errU := util.ParseChannel(stream, "-->", portAndNodeNameMap); errU == nil {
				nodeInfo.DownstreamList = append(nodeInfo.DownstreamList, downstreamBaseInfo)
			} else {
				return nil, errU
			}
		}
	}
	return nodeInfo, nil
}

// GetBaseInfo init or refresh opengauss base info
func GetBaseInfo(hookFs []dtstruct.HookFunction, refresh bool) (*dtstruct.BaseInfo, error) {

	// force fresh or first init
	if refresh || baseInfo.GaussVersion == "" {

		// check hook functions
		if hookFs == nil || len(hookFs) != 3 {
			hookFs = []dtstruct.HookFunction{nil, nil, nil}
		}

		// get env variable
		envMap, err := GetEnvVar(hookFs[0])
		if err != nil {
			return baseInfo, err
		}

		// get local node info
		infoMap, err := GetStatusInfo(hookFs[1])
		if err != nil {
			return baseInfo, err
		}

		// get port
		portMap, err := GetPort(hookFs[2], infoMap["NodeName"])
		if err != nil {
			return baseInfo, err
		}

		// combine node/env/port kvs
		func(mapList ...map[string]string) {
			for _, kvs := range mapList {
				for k, v := range kvs {
					infoMap[k] = v
				}
			}
		}(envMap, portMap)

		// refresh base info, TODO ignoreMiss is true because when instance is down, there will be miss key HA_state and static_connection
		if err = util.ResetValue(baseInfo, infoMap, true); err != nil {
			return baseInfo, err
		}
	}
	return baseInfo, nil
}

// GetEnvVar get opengauss cluster name from env, demo of output for command:
/*
GAUSS_VERSION=2.0.1
GS_CLUSTER_NAME=sangfor-opengauss-cluster
*/
func GetEnvVar(hookF dtstruct.HookFunction) (map[string]string, error) {

	// get cluster name from env
	result, err := ExecCmd(hookF, "sh", "-c", constant.CmdEnv)
	if err != nil {
		return nil, err
	}

	// output to map
	infoMap, err := util.OutputToMap(result, []int{2}, constant.OutputTypeEnv)
	if err != nil {
		return nil, err
	}
	return infoMap, nil
}

// GetStatusInfo use gs_om tool to get local node info
// demo of output for command: gs_om -t status -h `hostname`
/*
-----------------------------------------------------------------------

cluster_state             : Normal
redistributing            : No

-----------------------------------------------------------------------

node                      : 1
node_name                 : c-node-1
instance_id               : 6001
node_ip                   : 10.10.10.10
data_path                 : /opt/sangfor/db/install/data/dn
type                      : Datanode
instance_state            : Normal
az_name                   : AZ1
static_connections        : 1
HA_state                  : Normal
instance_role             : Primary

-----------------------------------------------------------------------
*/
func GetStatusInfo(hookF dtstruct.HookFunction) (map[string]string, error) {

	// get local node info
	result, err := ExecCmd(hookF, "sh", "-c", constant.CmdGSOMStatusLocal)
	if err != nil {
		return nil, log.Errorf("exec gs_om status, error:%s", err)
	}

	// output to map, 13 means cluster is normal and 11 means cluster is stopped
	infoMap, err := util.OutputToMap(result, []int{13, 11, 22, 14, 12, 23}, constant.OutputTypeCmd)
	if err != nil {
		return nil, err
	}

	// update role: cascade standby to cascade
	if infoMap["InstanceRole"] == "cascade standby" {
		infoMap["InstanceRole"] = "cascade"
	}
	return infoMap, nil
}

// GetPort get instance listen port, demo of output for command:
/*
nodeName:c-node-1
datanodePort :15400
nodeName:c-node-2
datanodePort :15400
*/
func GetPort(hookF dtstruct.HookFunction, nodeName string) (map[string]string, error) {

	// get server port
	result, err := ExecCmd(hookF, "sh", "-c", constant.CmdGSOMView)
	if err != nil {
		return nil, err
	}

	// find port for specified node name
	idx := -1
	nmLine := "nodeName:" + nodeName
	for i, line := range result {
		if strings.Contains(line, nmLine) {
			idx = i
			break
		}
	}

	// check and transfer to map
	if idx > -1 && idx < len(result)-1 {

		// check if next line is include datanode port
		if !strings.Contains(result[idx+1], "datanodePort") {
			return nil, log.Errorf("need datanodePort but found:%s ", result[idx+1])
		}

		// output to map
		infoMap, errT := util.OutputToMap(result[idx:idx+2], []int{2}, constant.OutputTypeCmd)
		if errT != nil {
			return nil, errT
		}
		return infoMap, nil
	}

	return nil, log.Errorf("miss server port for:%s from:%s", nodeName, result)
}

// GetAllPortAndNodeName get port for all instance
/*
nodeName:c-node-1
datanodePort :15400
datanodeListenIP 1:10.10.10.10
nodeName:c-node-2
datanodeListenIP 1:10.10.10.11
datanodePort :15400
*/
func GetAllPortAndNodeName(hookF dtstruct.HookFunction) (map[string]string, error) {

	// get server port
	result, err := ExecCmd(hookF, "sh", "-c", constant.CmdGSOMViewALL)
	if err != nil {
		return nil, err
	}

	// find first line start with nodeName and last line start with datanodePort
	nmIdx, dpIdx, num := -1, -1, 0
	for i, line := range result {
		if strings.HasPrefix(line, "nodeName:") && nmIdx == -1 {
			nmIdx = i
		}
		if strings.HasPrefix(line, "datanodePort :") {
			dpIdx = i
			num++
		}
	}

	// check and transfer to map
	if nmIdx > -1 && dpIdx > -1 && nmIdx < len(result)-1 && (dpIdx-nmIdx+1)%num == 0 {
		infoMap := make(map[string]string)
		result = result[nmIdx : dpIdx+1]
		partLnNum := (dpIdx - nmIdx + 1) / num
		for i := len(result) - 1; i > 0; i = i - partLnNum {

			// get port
			var port string
			if idx := strings.Index(result[i], ":"); idx > -1 {
				port = strings.TrimSpace(result[i][idx+1:])
			} else {
				continue
			}

			// get nodename
			var nodename string
			if idx := strings.Index(result[i-partLnNum+1], ":"); idx > -1 {
				nodename = strings.TrimSpace(result[i-partLnNum+1][idx+1:])
			} else {
				continue
			}

			// map for nodename and port
			for j := 1; j <= partLnNum-1; j++ {
				if tIdx := strings.Index(result[i-j], ":"); tIdx > -1 {
					if j == partLnNum-1 {
						infoMap[strings.TrimSpace(result[i-j][tIdx+1:])] = port
					} else {
						infoMap[strings.TrimSpace(result[i-j][tIdx+1:])] = nodename + ":" + port
					}
				}
			}
		}
		if len(infoMap) != 0 {
			return infoMap, nil
		}
	}

	return nil, log.Errorf("miss server port from:%s", result)
}

// GetVersion get opengauss version
func GetVersion(hookF dtstruct.HookFunction, version *string) string {
	if *version == "" {
		infoMap, err := GetEnvVar(hookF)
		if err != nil {
			return ""
		}
		*version = infoMap["GaussVersion"]
	}
	return *version
}

// GetDataPath get opengauss data path
func GetDataPath(hookF dtstruct.HookFunction) string {
	if baseInfo.DataPath == "" {
		if infoMap, err := GetStatusInfo(hookF); err == nil {
			return infoMap["DataPath"]
		}
		return ""
	}
	return baseInfo.DataPath
}

// ExecCmd exec command with args
func ExecCmd(hookF dtstruct.HookFunction, command string, args ...string) ([]string, error) {

	// exec command and wait to stop
	c := cmd.NewCmd(command, args...)
	<-c.Start()

	// command exec failed and has hook function, so exec hook
	if hookF != nil && (c.Status().Error != nil || len(c.Status().Stderr) != 0) {
		return hookF(command, args...)
	}

	// get go error
	if c.Status().Error != nil {
		return nil, log.Errore(c.Status().Error)
	}

	// get command error
	if len(c.Status().Stderr) != 0 {
		return nil, log.Errorf("exec:%s with args:%v failed, error:%s", command, args, strings.Join(c.Status().Stderr, "\n"))
	}

	return c.Status().Stdout, nil
}
