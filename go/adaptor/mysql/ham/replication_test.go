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
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/common/constant"
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/dtstruct"
	cdtstruct "gitee.com/opengauss/ham4db/go/dtstruct"
	test "gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
)

var (
	i710k = cdtstruct.InstanceKey{Hostname: "i710", Port: 3306, DBType: constant.DBTMysql}
	i720k = cdtstruct.InstanceKey{Hostname: "i720", Port: 3306, DBType: constant.DBTMysql}
)

func TestCanReplicateFrom(t *testing.T) {
	i55 := dtstruct.MysqlInstance{Instance: &cdtstruct.Instance{Key: i710k, Version: "5.5"}}
	i56 := dtstruct.MysqlInstance{Instance: &cdtstruct.Instance{Key: i720k, Version: "5.6"}}

	var canReplicate bool
	canReplicate, _ = CanReplicateFrom(&i56, &i55)
	test.S(t).ExpectEquals(canReplicate, false) //binlog not yet enabled

	i55.LogBinEnabled = true
	i55.LogReplicationUpdatesEnabled = true
	i56.LogBinEnabled = true
	i56.LogReplicationUpdatesEnabled = true

	canReplicate, _ = CanReplicateFrom(&i56, &i55)
	test.S(t).ExpectEquals(canReplicate, false) //InstanceId not set
	i55.InstanceId = "55"
	i56.InstanceId = "56"

	canReplicate, err := CanReplicateFrom(&i56, &i55)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectTrue(canReplicate)
	canReplicate, _ = CanReplicateFrom(&i55, &i56)
	test.S(t).ExpectFalse(canReplicate)

	iStatement := dtstruct.MysqlInstance{Instance: &cdtstruct.Instance{Key: i710k, InstanceId: "1", Version: "5.5"}, Binlog_format: "STATEMENT", LogBinEnabled: true, LogReplicationUpdatesEnabled: true}
	iRow := dtstruct.MysqlInstance{Instance: &cdtstruct.Instance{Key: i720k, InstanceId: "2", Version: "5.5"}, Binlog_format: "ROW", LogBinEnabled: true, LogReplicationUpdatesEnabled: true}
	canReplicate, err = CanReplicateFrom(&iRow, &iStatement)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectTrue(canReplicate)
	canReplicate, _ = CanReplicateFrom(&iStatement, &iRow)
	test.S(t).ExpectFalse(canReplicate)
}
