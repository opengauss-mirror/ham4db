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
package dtstruct

import (
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/common/constant"
	cdtstruct "gitee.com/opengauss/ham4db/go/dtstruct"
	test "gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
)

var (
	i710k = cdtstruct.InstanceKey{Hostname: "i710", Port: 3306, DBType: constant.DBTMysql}
	i720k = cdtstruct.InstanceKey{Hostname: "i720", Port: 3306, DBType: constant.DBTMysql}
	i730k = cdtstruct.InstanceKey{Hostname: "i730", Port: 3306, DBType: constant.DBTMysql}
)
var instance1 = MysqlInstance{Instance: &cdtstruct.Instance{Key: i710k}}
var instance2 = MysqlInstance{Instance: &cdtstruct.Instance{Key: i720k}}
var instance3 = MysqlInstance{Instance: &cdtstruct.Instance{Key: i730k}}

func newInstance(instanceKey cdtstruct.InstanceKey, InstanceId string, logFile string, logPos int64) MysqlInstance {
	instance := MysqlInstance{Instance: &cdtstruct.Instance{Key: instanceKey}}
	handler := &MysqlInstance{}
	handler.InstanceId = InstanceId
	handler.ExecBinlogCoordinates = cdtstruct.LogCoordinates{LogFile: logFile, LogPos: logPos}
	return instance
}

func TestIsSmallerMajorVersion(t *testing.T) {
	i55 := MysqlInstance{Instance: &cdtstruct.Instance{Version: "5.5"}}
	i5517 := MysqlInstance{Instance: &cdtstruct.Instance{Version: "5.5.17"}}
	i56 := MysqlInstance{Instance: &cdtstruct.Instance{Version: "5.6"}}

	test.S(t).ExpectFalse(i55.IsSmallerMajorVersion(i5517.Instance))
	test.S(t).ExpectFalse(i56.IsSmallerMajorVersion(i5517.Instance))
	test.S(t).ExpectTrue(i55.IsSmallerMajorVersion(i56.Instance))
}

func TestIsVersion(t *testing.T) {
	i51 := MysqlInstance{Instance: &cdtstruct.Instance{Version: "5.1.19"}}
	i55 := MysqlInstance{Instance: &cdtstruct.Instance{Version: "5.5.17-debug"}}
	i56 := MysqlInstance{Instance: &cdtstruct.Instance{Version: "5.6.20"}}
	i57 := MysqlInstance{Instance: &cdtstruct.Instance{Version: "5.7.8-log"}}

	test.S(t).ExpectTrue(i51.IsMySQL51())
	test.S(t).ExpectTrue(i55.IsMySQL55())
	test.S(t).ExpectTrue(i56.IsMySQL56())
	test.S(t).ExpectFalse(i55.IsMySQL56())
	test.S(t).ExpectTrue(i57.IsMySQL57())
	test.S(t).ExpectFalse(i56.IsMySQL57())
}

func TestIsDescendant(t *testing.T) {
	{
		i57 := MysqlInstance{Instance: &cdtstruct.Instance{Key: i710k, Version: "5.7"}}
		i56 := MysqlInstance{Instance: &cdtstruct.Instance{Key: i720k, Version: "5.6"}}
		isDescendant := i57.IsDescendantOf(&i56)
		test.S(t).ExpectEquals(isDescendant, false)
	}
	{
		i57 := MysqlInstance{Instance: &cdtstruct.Instance{Key: i710k, Version: "5.7"}, AncestryUUID: "00020192-1111-1111-1111-111111111111"}
		i56 := MysqlInstance{Instance: &cdtstruct.Instance{Key: i720k, Version: "5.6", InstanceId: ""}}
		isDescendant := i57.IsDescendantOf(&i56)
		test.S(t).ExpectEquals(isDescendant, false)
	}
	{
		i57 := MysqlInstance{Instance: &cdtstruct.Instance{Key: i710k, Version: "5.7"}, AncestryUUID: ""}
		i56 := MysqlInstance{Instance: &cdtstruct.Instance{Key: i720k, Version: "5.6", InstanceId: "00020192-1111-1111-1111-111111111111"}}
		isDescendant := i57.IsDescendantOf(&i56)
		test.S(t).ExpectEquals(isDescendant, false)
	}
	{
		i57 := MysqlInstance{Instance: &cdtstruct.Instance{Key: i710k, Version: "5.7"}, AncestryUUID: "00020193-2222-2222-2222-222222222222"}
		i56 := MysqlInstance{Instance: &cdtstruct.Instance{Key: i720k, Version: "5.6", InstanceId: "00020192-1111-1111-1111-111111111111"}}
		isDescendant := i57.IsDescendantOf(&i56)
		test.S(t).ExpectEquals(isDescendant, false)
	}
	{
		i57 := MysqlInstance{Instance: &cdtstruct.Instance{Key: i710k, Version: "5.7"}, AncestryUUID: "00020193-2222-2222-2222-222222222222,00020193-3333-3333-3333-222222222222"}
		i56 := MysqlInstance{Instance: &cdtstruct.Instance{Key: i720k, Version: "5.6", InstanceId: "00020192-1111-1111-1111-111111111111"}}
		isDescendant := i57.IsDescendantOf(&i56)
		test.S(t).ExpectEquals(isDescendant, false)
	}
	{
		i57 := MysqlInstance{Instance: &cdtstruct.Instance{Key: i710k, Version: "5.7"}, AncestryUUID: "00020193-2222-2222-2222-222222222222,00020192-1111-1111-1111-111111111111"}
		i56 := MysqlInstance{Instance: &cdtstruct.Instance{Key: i720k, Version: "5.6", InstanceId: "00020192-1111-1111-1111-111111111111"}}
		isDescendant := i57.IsDescendantOf(&i56)
		test.S(t).ExpectEquals(isDescendant, true)
	}
}

func TestNextGTID(t *testing.T) {
	{
		i := MysqlInstance{ExecutedGtidSet: "4f6d62ed-df65-11e3-b395-60672090eb04:1,b9b4712a-df64-11e3-b391-60672090eb04:1-6"}
		nextGTID, err := i.NextGTID()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(nextGTID, "b9b4712a-df64-11e3-b391-60672090eb04:7")
	}
	{
		i := MysqlInstance{ExecutedGtidSet: "b9b4712a-df64-11e3-b391-60672090eb04:1-6"}
		nextGTID, err := i.NextGTID()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(nextGTID, "b9b4712a-df64-11e3-b391-60672090eb04:7")
	}
	{
		i := MysqlInstance{ExecutedGtidSet: "b9b4712a-df64-11e3-b391-60672090eb04:6"}
		nextGTID, err := i.NextGTID()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(nextGTID, "b9b4712a-df64-11e3-b391-60672090eb04:7")
	}
}

func TestHumanReadableDescription(t *testing.T) {
	i57 := MysqlInstance{Instance: &cdtstruct.Instance{Version: "5.7.8-log"}}
	{
		desc := i57.HumanReadableDescription()
		test.S(t).ExpectEquals(desc, "[unknown,invalid,5.7.8-log,rw,nobinlog]")
	}
	{
		i57.UsingPseudoGTID = true
		i57.LogBinEnabled = true
		i57.Binlog_format = "ROW"
		i57.LogReplicationUpdatesEnabled = true
		desc := i57.HumanReadableDescription()
		test.S(t).ExpectEquals(desc, "[unknown,invalid,5.7.8-log,rw,ROW,>>,P-GTID]")
	}
}

func TestTabulatedDescription(t *testing.T) {
	i57 := MysqlInstance{Instance: &cdtstruct.Instance{Version: "5.7.8-log"}}
	{
		desc := i57.TabulatedDescription("|")
		test.S(t).ExpectEquals(desc, "unknown|invalid|5.7.8-log|rw|nobinlog|")
	}
	{
		i57.UsingPseudoGTID = true
		i57.LogBinEnabled = true
		i57.Binlog_format = "ROW"
		i57.LogReplicationUpdatesEnabled = true
		desc := i57.TabulatedDescription("|")
		test.S(t).ExpectEquals(desc, "unknown|invalid|5.7.8-log|rw|ROW|>>,P-GTID")
	}
}

func TestReplicationThreads(t *testing.T) {
	{
		test.S(t).ExpectFalse(instance1.ReplicaRunning())
	}
	{
		test.S(t).ExpectTrue(instance1.ReplicationThreadsExist())
	}
	{
		test.S(t).ExpectTrue(instance1.ReplicationThreadsStopped())
	}
	{
		i := MysqlInstance{Instance: &cdtstruct.Instance{Key: i710k}, ReplicationIOThreadState: ReplicationThreadStateNoThread, ReplicationSQLThreadState: ReplicationThreadStateNoThread}
		test.S(t).ExpectFalse(i.ReplicationThreadsExist())
	}
}
