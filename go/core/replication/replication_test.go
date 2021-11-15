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
package replication

import (
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	dbtest "gitee.com/opengauss/ham4db/go/test"
	tdtstruct "gitee.com/opengauss/ham4db/go/test/testdb/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
	"time"
)

func init() {
	dbtest.DBTestInit()
}

// TODO should be fully test in the implement of database adaptor
func TestReplication(t *testing.T) {

	// key for test
	keyList := []*dtstruct.InstanceKey{
		{Hostname: "abc.local", Port: 0, DBType: constant.TestDB}, // test db
		{Hostname: "abc.local", Port: 0, DBType: "abc"},           // db not exist
	}

	// normal case 1: start replication
	adaptor, err := StartReplication(keyList[0])
	tests.S(t).ExpectNotNil(adaptor)
	tests.S(t).ExpectNil(err)

	// failed case 1: start replication on not exist db
	adaptor, err = StartReplication(keyList[1])
	tests.S(t).ExpectNotNil(adaptor)
	if adaptor != nil {
		tests.S(t).ExpectTrue(adaptor.(*tdtstruct.TestDBInstance).GetDatabaseType() == constant.TestDB)
	}
	tests.S(t).ExpectNil(err)

	// normal case 2: restart replication
	adaptor, err = RestartReplication(keyList[0])
	tests.S(t).ExpectNotNil(adaptor)
	tests.S(t).ExpectNil(err)

	// normal case 3:
	adaptor, err = StopReplication(keyList[0])
	tests.S(t).ExpectNotNil(adaptor)
	tests.S(t).ExpectNil(err)

	// normal case 4:
	adaptor, err = StopReplicationNicely(keyList[0], 1*time.Second)
	tests.S(t).ExpectNotNil(adaptor)
	tests.S(t).ExpectNil(err)

	// normal case 5:
	adaptor, err = ResetReplication(keyList[0])
	tests.S(t).ExpectNotNil(adaptor)
	tests.S(t).ExpectNil(err)

	// normal case 6:
	err = DelayReplication(keyList[0], 1)
	tests.S(t).ExpectNil(err)

	// normal case 7:
	var replicate bool
	replicate, err = CanReplicateFrom(&tdtstruct.TestDBInstance{Instance: &dtstruct.Instance{Key: *keyList[0]}}, &tdtstruct.TestDBInstance{Instance: &dtstruct.Instance{Key: *keyList[1]}})
	tests.S(t).ExpectFalse(replicate)
	tests.S(t).ExpectNil(err)

	// normal case 8:
	adaptor, err = SetReadOnly(keyList[0], true)
	tests.S(t).ExpectNotNil(adaptor)
	tests.S(t).ExpectNil(err)

	// normal case 9:
	adaptor, err = SetSemiSyncOnDownstream(keyList[0], true)
	tests.S(t).ExpectNotNil(adaptor)
	tests.S(t).ExpectNil(err)

	// normal case 10:
	adaptor, err = SetSemiSyncOnUpstream(keyList[0], true)
	tests.S(t).ExpectNotNil(adaptor)
	tests.S(t).ExpectNil(err)

	// normal case 11:
	var repAnalysis []dtstruct.ReplicationAnalysis
	repAnalysis, err = GetReplicationAnalysis(constant.TestDB, "", "", &dtstruct.ReplicationAnalysisHints{})
	tests.S(t).ExpectTrue(len(repAnalysis) == 0)
	tests.S(t).ExpectNil(err)
}
