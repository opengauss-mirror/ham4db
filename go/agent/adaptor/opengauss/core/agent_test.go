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
	"gitee.com/opengauss/ham4db/go/test/config"
	"gitee.com/opengauss/ham4db/go/util/tests"
	"os"
	"testing"
	"time"
)

var statusInfo = []string{
	"-----------------------------------------------------------------------",
	"",
	"cluster_state             : Unavailable",
	"redistributing            : No",
	"",
	"-----------------------------------------------------------------------",
	"",
	"node                      : 2",
	"node_name                 : opengauss-test-standby-1",
	"instance_id               : 6002",
	"node_ip                   : 10.10.10.11",
	"data_path                 : /opt/sangfor/db/install/data/dn",
	"type                      : Datanode",
	"instance_state            : Normal",
	"az_name                   : AZ1",
	"instance_role             : Standby",
	"HA_state                  : Streaming",
	"sender_sent_location      : 0/FE00758",
	"sender_write_location     : 0/FE00758",
	"sender_flush_location     : 0/FE00758",
	"sender_replay_location    : 0/FE00758",
	"receiver_received_location: 0/FE00758",
	"receiver_write_location   : 0/FE00758",
	"receiver_flush_location   : 0/FE00758",
	"receiver_replay_location  : 0/FE00758",
	"sync_percent              : 100%",
	"sync_state                : Quorum",
}

var portInfo = []string{
	"nodeName:opengauss-test-primary",
	"datanodePort :15400",
	"nodeName:opengauss-test-standby-1",
	"datanodePort :15401",
}

var portAllInfo = []string{
	"nodeName:opengauss-test-primary",
	"datanodeListenIP 1:10.10.10.10",
	"datanodePort :15400",
	"nodeName:opengauss-test-standby-1",
	"datanodeListenIP 1:10.10.10.11",
	"datanodePort :15401",
	"nodeName:opengauss-test-standby-2",
	"datanodeListenIP 1:10.10.10.12",
	"datanodePort :15401",
	"nodeName:opengauss-test-cascade-1",
	"datanodeListenIP 1:10.10.10.13",
	"datanodePort :15401",
}

var topoInfo = [][]string{
	{"[2021-09-03 05:51:41.962][5178][][gs_ctl]: gs_ctl query ,datadir is /opt/sangfor/db/install/data/dn ",
		"HA state:           ",
		"local_role                     : Standby",
		"static_connections             : 4",
		"db_state                       : Normal",
		"detail_information             : Normal",
		"Senders info:       ",
		"sender_pid                     : 10127",
		"local_role                     : Standby",
		"peer_role                      : Cascade Standby",
		"peer_state                     : Normal",
		"state                          : Streaming",
		"sender_sent_location           : 0/100A3178",
		"sender_write_location          : 0/100A3178",
		"sender_flush_location          : 0/100A3178",
		"sender_replay_location         : 0/100A3178",
		"receiver_received_location     : 0/100A3178",
		"receiver_write_location        : 0/100A3178",
		"receiver_flush_location        : 0/100A3178",
		"receiver_replay_location       : 0/100A3178",
		"sync_percent                   : 100%",
		"sync_state                     : Async",
		"sync_priority                  : 0",
		"sync_most_available            : Off",
		"channel                        : 10.10.10.11:15401-->10.10.10.13:52414",
		"Receiver info:      ",
		"receiver_pid                   : 9445",
		"local_role                     : Standby",
		"peer_role                      : Primary",
		"peer_state                     : Normal",
		"state                          : Normal",
		"sender_sent_location           : 0/100A3178",
		"sender_write_location          : 0/100A3178",
		"sender_flush_location          : 0/100A3178",
		"sender_replay_location         : 0/100A3178",
		"receiver_received_location     : 0/100A3178",
		"receiver_write_location        : 0/100A3178",
		"receiver_flush_location        : 0/100A3178",
		"receiver_replay_location       : 0/100A3178",
		"sync_percent                   : 100%",
		"channel                        : 10.10.10.11:36580<--10.10.10.10:15401",
	}, {
		"[2021-09-03 06:33:51.581][1325][][gs_ctl]: gs_ctl query ,datadir is /opt/sangfor/db/install/data/dn ",
		"HA state:           ",
		"local_role                     : Primary",
		"static_connections             : 4",
		"db_state                       : Normal",
		"detail_information             : Normal",
		"Senders info:       ",
		"sender_pid                     : 1566",
		"local_role                     : Primary",
		"peer_role                      : Standby",
		"peer_state                     : Normal",
		"state                          : Streaming",
		"sender_sent_location           : 0/100A5F80",
		"sender_write_location          : 0/100A5F80",
		"sender_flush_location          : 0/100A5F80",
		"sender_replay_location         : 0/100A5F80",
		"receiver_received_location     : 0/100A5F80",
		"receiver_write_location        : 0/100A5F80",
		"receiver_flush_location        : 0/100A5F80",
		"receiver_replay_location       : 0/100A5F80",
		"sync_percent                   : 100%",
		"sync_state                     : Quorum",
		"sync_priority                  : 1",
		"sync_most_available            : Off",
		"channel                        : 10.10.10.10:15401-->10.10.10.11:36580",
		"sender_pid                     : 1724",
		"local_role                     : Primary",
		"peer_role                      : Standby",
		"peer_state                     : Normal",
		"state                          : Streaming",
		"sender_sent_location           : 0/100A5F80",
		"sender_write_location          : 0/100A5F80",
		"sender_flush_location          : 0/100A5F80",
		"sender_replay_location         : 0/100A5F80",
		"receiver_received_location     : 0/100A5F80",
		"receiver_write_location        : 0/100A5F80",
		"receiver_flush_location        : 0/100A5F80",
		"receiver_replay_location       : 0/100A5F80",
		"sync_percent                   : 100%",
		"sync_state                     : Quorum",
		"sync_priority                  : 1",
		"sync_most_available            : Off",
		"channel                        : 10.10.10.10:15401-->10.10.10.12:49180",
		"Receiver info:      ",
		"No information ",
	}, {
		"[2021-09-03 06:35:07.338][23913][][gs_ctl]: gs_ctl query ,datadir is /opt/sangfor/db/install/data/dn ",
		"HA state:           ",
		"local_role                     : Cascade Standby",
		"static_connections             : 4",
		"db_state                       : Normal",
		"detail_information             : Normal",
		"Senders info:       ",
		"No information ",
		"Receiver info:      ",
		"receiver_pid                   : 2219",
		"local_role                     : Cascade Standby",
		"peer_role                      : Standby",
		"peer_state                     : Normal",
		"state                          : Normal",
		"sender_sent_location           : 0/100A61C8",
		"sender_write_location          : 0/100A61C8",
		"sender_flush_location          : 0/100A61C8",
		"sender_replay_location         : 0/100A6130",
		"receiver_received_location     : 0/100A61C8",
		"receiver_write_location        : 0/100A61C8",
		"receiver_flush_location        : 0/100A61C8",
		"receiver_replay_location       : 0/100A61C8",
		"sync_percent                   : 100%",
		"channel                        : 10.10.10.13:52414<--10.10.10.11:15401",
	},
}

func init() {
	config.TestConfigLog()
}

func TestGetNodeInfo(t *testing.T) {
	baseInfo = &dtstruct.BaseInfo{}

	// failed case 1
	ni, err := GetNodeInfo(&dtstruct.DatabaseInfoRequest{
		FromCache: false,
	}, nil)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(ni == nil)

	// failed case 2: failed when get upstream and downstream
	ni, err = GetNodeInfo(&dtstruct.DatabaseInfoRequest{
		FromCache: false,
	}, []dtstruct.HookFunction{
		nil,
		func(command string, args ...string) ([]string, error) {
			return statusInfo, nil
		},
		func(command string, args ...string) ([]string, error) {
			return portInfo, nil
		},
		nil,
		nil,
	})
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(ni == nil)

	// set env
	_ = os.Setenv("GS_CLUSTER_NAME", "opengauss-cluster")
	_ = os.Setenv("GAUSS_VERSION", "2.0.1")

	// normal case 1: standby with primary and cascade
	ni, err = GetNodeInfo(&dtstruct.DatabaseInfoRequest{
		FromCache: false,
	}, []dtstruct.HookFunction{
		nil,
		func(command string, args ...string) ([]string, error) {
			return statusInfo, nil
		},
		func(command string, args ...string) ([]string, error) {
			return portInfo, nil
		},
		func(command string, args ...string) ([]string, error) {
			return topoInfo[0], nil
		},
		func(command string, args ...string) ([]string, error) {
			return portAllInfo, nil
		},
	})
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(ni.Upstream.NodeIp == "10.10.10.10")
	tests.S(t).ExpectTrue(len(ni.DownstreamList) == 1)
	tests.S(t).ExpectTrue(ni.DownstreamList[0].NodeIp == "10.10.10.13")

	// normal case 2: primary
	ni, err = GetNodeInfo(&dtstruct.DatabaseInfoRequest{
		FromCache: false,
	}, []dtstruct.HookFunction{
		nil,
		func(command string, args ...string) ([]string, error) {
			return statusInfo, nil
		},
		func(command string, args ...string) ([]string, error) {
			return portInfo, nil
		},
		func(command string, args ...string) ([]string, error) {
			return topoInfo[1], nil
		},
		func(command string, args ...string) ([]string, error) {
			return portAllInfo, nil
		},
	})
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(ni.Upstream.NodeIp == "")
	tests.S(t).ExpectTrue(len(ni.DownstreamList) == 2)
	tests.S(t).ExpectTrue(ni.DownstreamList[0].NodeIp == "10.10.10.11")
	tests.S(t).ExpectTrue(ni.DownstreamList[1].NodeIp == "10.10.10.12")

	// normal case 3: cascade
	ni, err = GetNodeInfo(&dtstruct.DatabaseInfoRequest{
		FromCache: false,
	}, []dtstruct.HookFunction{
		nil,
		func(command string, args ...string) ([]string, error) {
			return statusInfo, nil
		},
		func(command string, args ...string) ([]string, error) {
			return portInfo, nil
		},
		func(command string, args ...string) ([]string, error) {
			return topoInfo[2], nil
		},
		func(command string, args ...string) ([]string, error) {
			return portAllInfo, nil
		},
	})
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(ni.Upstream.NodeIp == "10.10.10.11")
	tests.S(t).ExpectTrue(len(ni.DownstreamList) == 0)
}

func TestGetBaseInfo(t *testing.T) {
	baseInfo = &dtstruct.BaseInfo{}

	// failed case 1: without hook, command failed
	bi, err := GetBaseInfo(nil, false)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(bi.DataPath == "")

	// set env
	_ = os.Setenv("GS_CLUSTER_NAME", "opengauss-cluster")
	_ = os.Setenv("GAUSS_VERSION", "2.0.1")

	// failed case 2: failed status info
	bi, err = GetBaseInfo([]dtstruct.HookFunction{
		nil, nil, nil,
	}, false)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(bi.DataPath == "")

	// failed case 3: failed port
	bi, err = GetBaseInfo([]dtstruct.HookFunction{
		nil,
		func(command string, args ...string) ([]string, error) {
			return statusInfo, nil
		},
		nil,
	}, false)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(bi.DataPath == "")

	// normal case 1
	bi, err = GetBaseInfo([]dtstruct.HookFunction{
		nil,
		func(command string, args ...string) ([]string, error) {
			return statusInfo, nil
		},
		func(command string, args ...string) ([]string, error) {
			return portInfo, nil
		},
	}, false)
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(bi.DataPath == "/opt/sangfor/db/install/data/dn")
}

func TestGetEnvVar(t *testing.T) {

	// clear test
	baseInfo = &dtstruct.BaseInfo{}
	_ = os.Unsetenv("GS_CLUSTER_NAME")
	_ = os.Unsetenv("GAUSS_VERSION")

	// failed case 1: no env variables
	infoMap, err := GetEnvVar(nil)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(infoMap == nil)

	// failed case 2: has no enough env variables
	_ = os.Setenv("GS_CLUSTER_NAME", "opengauss-cluster")
	infoMap, err = GetEnvVar(nil)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(infoMap == nil)

	// normal case 1: with 2 env variables
	_ = os.Setenv("GAUSS_VERSION", "2.0.1")
	infoMap, err = GetEnvVar(nil)
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(infoMap["GsClusterName"] == "opengauss-cluster")
	tests.S(t).ExpectTrue(infoMap["GaussVersion"] == "2.0.1")
}

func TestGetStatusInfo(t *testing.T) {
	baseInfo = &dtstruct.BaseInfo{}

	// failed case 1
	infoMap, err := GetStatusInfo(nil)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(infoMap == nil)

	// failed case 2
	infoMap, err = GetStatusInfo(func(command string, args ...string) ([]string, error) {
		return statusInfo[0:10], nil
	})
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(infoMap == nil)

	// normal case 1
	infoMap, err = GetStatusInfo(func(command string, args ...string) ([]string, error) {
		return statusInfo, nil
	})
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(infoMap["InstanceId"] == "6002")
}

func TestGetPort(t *testing.T) {
	baseInfo = &dtstruct.BaseInfo{}

	// test node
	nodeName := "--"

	// failed case 1
	infoMap, err := GetPort(nil, nodeName)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(infoMap == nil)

	// failed case 2: with hook, but no info for node name
	infoMap, err = GetPort(func(command string, args ...string) ([]string, error) {
		return portInfo, nil
	}, nodeName)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(infoMap == nil)

	// failed case 3: no enough output info
	nodeName = "opengauss-test-primary"
	infoMap, err = GetPort(func(command string, args ...string) ([]string, error) {
		return portInfo[0:1], nil
	}, nodeName)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(infoMap == nil)

	// failed case 4: no port info
	infoMap, err = GetPort(func(command string, args ...string) ([]string, error) {
		return []string{portInfo[0], portInfo[2], portInfo[3]}, nil
	}, nodeName)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(infoMap == nil)

	// normal case 1
	infoMap, err = GetPort(func(command string, args ...string) ([]string, error) {
		return portInfo, nil
	}, nodeName)
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(infoMap["DatanodePort"] == "15400")
}

func TestGetAllPort(t *testing.T) {
	baseInfo = &dtstruct.BaseInfo{}

	// failed case 1
	infoMap, err := GetAllPortAndNodeName(nil)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(infoMap == nil)

	// normal case 1: with hook and right info
	infoMap, err = GetAllPortAndNodeName(func(command string, args ...string) ([]string, error) {
		return portAllInfo, nil
	})
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(infoMap["opengauss-test-primary"] == "15400")
	tests.S(t).ExpectTrue(infoMap["opengauss-test-standby-1"] == "15401")
	tests.S(t).ExpectTrue(infoMap["10.10.10.10"] == "opengauss-test-primary:15400")
	tests.S(t).ExpectTrue(infoMap["10.10.10.11"] == "opengauss-test-standby-1:15401")

	// failed case 3: no enough output info
	infoMap, err = GetAllPortAndNodeName(func(command string, args ...string) ([]string, error) {
		return portAllInfo[0:1], nil
	})
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(infoMap == nil)

	// failed case 4: no port info
	infoMap, err = GetAllPortAndNodeName(func(command string, args ...string) ([]string, error) {
		return []string{portAllInfo[0], portAllInfo[3], portAllInfo[4]}, nil
	})
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(infoMap == nil)
}

func TestGetVersion(t *testing.T) {

	// clear test
	baseInfo = &dtstruct.BaseInfo{}
	_ = os.Unsetenv("GS_CLUSTER_NAME")
	_ = os.Unsetenv("GAUSS_VERSION")

	// test version
	version := constant.Version2d0d1

	// normal case 1: already has version
	tests.S(t).ExpectTrue(GetVersion(nil, &version) == constant.Version2d0d1)

	// failed case 1: no version and no env variables
	version = ""
	tests.S(t).ExpectTrue(GetVersion(nil, &version) == "")

	// normal case 2: no version but has env variables
	_ = os.Setenv("GS_CLUSTER_NAME", "opengauss-cluster")
	_ = os.Setenv("GAUSS_VERSION", "2.0.1")
	tests.S(t).ExpectTrue(GetVersion(nil, &version) == "2.0.1")
}

func TestGetDataPath(t *testing.T) {
	baseInfo = &dtstruct.BaseInfo{}

	// test path
	dpath := "/opt"

	// normal case 1:
	baseInfo.DataPath = dpath
	tests.S(t).ExpectTrue(GetDataPath(nil) == dpath)

	// failed case 1:
	baseInfo.DataPath = ""
	tests.S(t).ExpectTrue(GetDataPath(nil) == "")

	// normal case 2: use hook function
	tests.S(t).ExpectNotNil(GetDataPath(
		func(command string, args ...string) ([]string, error) {
			return statusInfo, nil
		}) == "/opt/sangfor/db/install/data/dn",
	)
}

func TestExecCmd(t *testing.T) {
	baseInfo = &dtstruct.BaseInfo{}

	// normal case 1:
	result, err := ExecCmd(nil, "date", "+%s")
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(len(result) == 1)

	// normal case 2: if got error, use hook function
	result, err = ExecCmd(
		func(command string, args ...string) ([]string, error) {
			return []string{"", ""}, nil
		},
		"su - "+time.Now().String(), "-c", "ls",
	)
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(len(result) == 2)

	// failed case 1: command failed
	result, err = ExecCmd(nil, "ls", "/"+time.Now().String())
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(result == nil)

	// failed case 2: go error
	result, err = ExecCmd(nil, "su - "+time.Now().String(), "-c", "ls")
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(result == nil)
}
