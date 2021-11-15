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
package handler

import (
	"context"
	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/common/constant"
	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/core"
	aodtstruct "gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/dtstruct"
	adtstruct "gitee.com/opengauss/ham4db/go/agent/dtstruct"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/test/config"
	"gitee.com/opengauss/ham4db/go/util/tests"
	"os"
	"syscall"
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
	"node_ip                   : 10.10.10.10",
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
	"datanodePort :15400",
}

func init() {
	config.TestConfigLog()
}

func TestOpenGaussServer_CollectInfo(t *testing.T) {

	// test server
	ogs := OpenGaussServer{}

	// set version
	ogs.version = constant.Version2d0d1

	// failed case 1: no running database instance, so cannot get info
	resp, err := ogs.CollectInfo(context.TODO(), &aodtstruct.DatabaseInfoRequest{})
	tests.S(t).ExpectTrue(resp == nil)
	tests.S(t).ExpectNotNil(err)

	// reset version
	ogs.version = "xx.xx.xx"

	// failed case 2: not supported version
	resp, err = ogs.CollectInfo(context.TODO(), &aodtstruct.DatabaseInfoRequest{})
	tests.S(t).ExpectTrue(resp == nil)
	tests.S(t).ExpectNotNil(err)
}

func TestOpenGaussServer_ManageAction(t *testing.T) {

	// test server
	ogs := OpenGaussServer{}

	// set version
	ogs.version = constant.Version2d0d1

	// failed case 1: no running database instance, so cannot get info
	resp, err := ogs.ManageAction(context.TODO(), &aodtstruct.ManageActionRequest{})
	tests.S(t).ExpectTrue(resp == nil)
	tests.S(t).ExpectNotNil(err)

	// reset version
	ogs.version = "xx.xx.xx"

	// failed case 2: not supported version
	resp, err = ogs.ManageAction(context.TODO(), &aodtstruct.ManageActionRequest{})
	tests.S(t).ExpectTrue(resp == nil)
	tests.S(t).ExpectNotNil(err)
}

func TestOpenGaussServer_UpdateSyncConfig(t *testing.T) {

	// test server
	ogs := OpenGaussServer{}

	// set version
	ogs.version = constant.Version2d0d1

	// failed case 1: no running database instance, so cannot get info
	resp, err := ogs.UpdateSyncConfig(context.TODO(), &aodtstruct.UpdateSyncConfigRequest{})
	tests.S(t).ExpectTrue(resp == nil)
	tests.S(t).ExpectNotNil(err)

	// reset version
	ogs.version = "xx.xx.xx"

	// failed case 2: not supported version
	resp, err = ogs.UpdateSyncConfig(context.TODO(), &aodtstruct.UpdateSyncConfigRequest{})
	tests.S(t).ExpectTrue(resp == nil)
	tests.S(t).ExpectNotNil(err)
}

func TestOpenGaussServer_RunTask(t *testing.T) {

	// test server
	ogs := OpenGaussServer{}

	// simulate cli command args
	args := &adtstruct.Args{RefreshInterval: 1}

	// create worker pool
	wp := dtstruct.NewWorkerPool()

	// run fresh task
	ogs.RunTask(wp, args)

	// normal case 1: worker is created successfully
	tests.S(t).ExpectTrue(wp.GetWorker(constant.WorkerNameAgentRefreshBaseInfo) != nil)

	// failed case 1: can't get basic info because failed command
	bi, err := core.GetBaseInfo(nil, false)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(bi.DataPath == "")

	// set env
	_ = os.Setenv("GS_CLUSTER_NAME", "opengauss-cluster")
	_ = os.Setenv("GAUSS_VERSION", "2.0.1")

	// config hook functions
	ogs.hookTask = []aodtstruct.HookFunction{
		nil,
		func(command string, args ...string) ([]string, error) {
			return statusInfo, nil
		},
		func(command string, args ...string) ([]string, error) {
			return portInfo, nil
		},
	}

	// wait until refresh
	time.Sleep(2 * time.Second)

	// normal case 2
	bi, err = core.GetBaseInfo(nil, false)
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(bi.DataPath == "/opt/sangfor/db/install/data/dn")

	// exit
	wp.Exit(syscall.SIGTERM)
	wp.WaitStop()
}
