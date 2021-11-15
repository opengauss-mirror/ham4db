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
	dtstruct2 "gitee.com/opengauss/ham4db/go/adaptor/mysql/dtstruct"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	test "gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
)

var key1 = dtstruct.InstanceKey{Hostname: "host1", Port: 3306}
var key2 = dtstruct.InstanceKey{Hostname: "host2", Port: 3306}
var key3 = dtstruct.InstanceKey{Hostname: "host3", Port: 3306}

func TestIsSmallerBinlogFormat(t *testing.T) {
	iStatement := &dtstruct2.MysqlInstance{Instance: &dtstruct.Instance{Key: key1}, Binlog_format: "STATEMENT"}
	iRow := &dtstruct2.MysqlInstance{Instance: &dtstruct.Instance{Key: key2}, Binlog_format: "ROW"}
	iMixed := &dtstruct2.MysqlInstance{Instance: &dtstruct.Instance{Key: key3}, Binlog_format: "MIXED"}
	test.S(t).ExpectTrue(IsSmallerBinlogFormat(iStatement.Binlog_format, iRow.Binlog_format))
	test.S(t).ExpectFalse(IsSmallerBinlogFormat(iStatement.Binlog_format, iStatement.Binlog_format))
	test.S(t).ExpectFalse(IsSmallerBinlogFormat(iRow.Binlog_format, iStatement.Binlog_format))

	test.S(t).ExpectTrue(IsSmallerBinlogFormat(iStatement.Binlog_format, iMixed.Binlog_format))
	test.S(t).ExpectTrue(IsSmallerBinlogFormat(iMixed.Binlog_format, iRow.Binlog_format))
	test.S(t).ExpectFalse(IsSmallerBinlogFormat(iMixed.Binlog_format, iStatement.Binlog_format))
	test.S(t).ExpectFalse(IsSmallerBinlogFormat(iRow.Binlog_format, iMixed.Binlog_format))
}
