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
package util

import (
	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/common/constant"
	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/dtstruct"
	"gitee.com/opengauss/ham4db/go/test/config"
	"gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
)

func init() {
	config.TestConfigLog()
}

func TestUpdateHostAndPort(t *testing.T) {

	// test config
	ip := "10.10.10.10"
	hostname := "host-1"
	port := "10000"

	// map hold kvs
	kvs := make(map[string]string)
	kvs["hostname"] = ip

	// update hostname
	UpdateHostAndPort(kvs, ip, hostname, port)

	tests.S(t).ExpectTrue(kvs["hostname"] == hostname)
	tests.S(t).ExpectTrue(kvs["port"] == port)
}

func TestOutputToMap(t *testing.T) {

	// normal case 1: command output
	var op1 []string
	op1 = append(op1, "-------------")
	op1 = append(op1, "")
	op1 = append(op1, "node :  1")
	op1 = append(op1, "node_ip :  10.10.10.10")
	kvs, err := OutputToMap(op1, []int{2}, constant.OutputTypeCmd)
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(kvs["Node"] == "1")
	tests.S(t).ExpectTrue(kvs["NodeIp"] == "10.10.10.10")

	// normal case 2: env output
	op1 = op1[:0]
	op1 = append(op1, "XXX=123")
	op1 = append(op1, "XXX_YYY=xyz")
	kvs, err = OutputToMap(op1, []int{2}, constant.OutputTypeEnv)
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(kvs["Xxx"] == "123")
	tests.S(t).ExpectTrue(kvs["XxxYyy"] == "xyz")

	// failed case 1: no valid output
	op1 = op1[:0]
	op1 = append(op1, "-------------")
	kvs, err = OutputToMap(op1, []int{2}, constant.OutputTypeCmd)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(kvs == nil)

	// failed case 2: number of valid output is not equal expect
	op1 = op1[:0]
	op1 = append(op1, "-------------")
	op1 = append(op1, "node_ip :  10.10.10.10")
	kvs, err = OutputToMap(op1, []int{2, 3}, constant.OutputTypeCmd)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(kvs == nil)

}

func TestKeyToField(t *testing.T) {

	// normal case 1
	tests.S(t).ExpectTrue(KeyToField("instance_id") == "InstanceId")
	tests.S(t).ExpectTrue(KeyToField("inStance_id") == "InstanceId")
	tests.S(t).ExpectTrue(KeyToField("inStance") == "InStance")

	// failed case 1: not match regex
	tests.S(t).ExpectTrue(KeyToField("inStance9") == "inStance9")
	tests.S(t).ExpectTrue(KeyToField("instance-id") == "instance-id")
}

func TestResetValue(t *testing.T) {

	bi := &dtstruct.BaseInfo{}

	// normal case 1
	tests.S(t).ExpectTrue(ResetValue(bi, map[string]string{}, true) == nil)

	// normal case 2
	tests.S(t).ExpectTrue(ResetValue(bi, map[string]string{"NodeName": "XXX", "DatanodePort": "10000"}, true) == nil)
	tests.S(t).ExpectTrue(bi.NodeName == "XXX")
	tests.S(t).ExpectTrue(bi.DatanodePort == 10000)

	// normal case 3
	bi2 := &dtstruct.BaseInfo{}
	tests.S(t).ExpectTrue(ResetValue(bi2, map[string]string{"NodeName": "XXX", "DatanodePort": "10000"}, true) == nil)
	tests.S(t).ExpectTrue(bi2.NodeName == "XXX")
	tests.S(t).ExpectTrue(bi2.DatanodePort == 10000)

	// failed case 1
	tests.S(t).ExpectTrue(ResetValue(bi, map[string]string{}, false) != nil)
}

func TestParseChannel(t *testing.T) {

	// normal case 1: sender channel
	stream := "channel                        : 10.116.43.188:15401-->10.116.43.202:59988"
	baseInfo, err := ParseChannel(stream, "-->", map[string]string{"10.116.43.202": "xxxx:15400"})
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(baseInfo.NodeIp == "10.116.43.202")
	tests.S(t).ExpectTrue(baseInfo.DatanodePort == 15400)
	tests.S(t).ExpectTrue(baseInfo.NodeName == "xxxx")

	// normal case 2: receiver channel
	stream = "channel                        : 10.116.43.199:51726<--10.116.43.200:15401"
	baseInfo, err = ParseChannel(stream, "<--", map[string]string{"10.116.43.200": "xxxx:15400"})
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectEquals(baseInfo.NodeIp, "10.116.43.200")
	tests.S(t).ExpectEquals(baseInfo.NodeName, "xxxx")

	// failed case 1: wrong format
	stream = "channel                        : 10.116.43.199:51726<--10.116.43.200:15401"
	baseInfo, err = ParseChannel(stream, "-->", map[string]string{})
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(baseInfo == nil)

	// failed case 1: wrong ip/port
	stream = "channel                        : 10.116.43.199:51726<--10.116.43.200"
	baseInfo, err = ParseChannel(stream, "<--", map[string]string{})
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(baseInfo == nil)
}
