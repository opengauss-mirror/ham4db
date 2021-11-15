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
	"errors"
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/common/constant"
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/dtstruct"
	cdtstruct "gitee.com/opengauss/ham4db/go/dtstruct"
	test "gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
)

var (
	i710k = cdtstruct.InstanceKey{Hostname: "i710", Port: 3306, DBType: constant.DBTMysql}
	i720k = cdtstruct.InstanceKey{Hostname: "i720", Port: 3306, DBType: constant.DBTMysql}
	i730k = cdtstruct.InstanceKey{Hostname: "i730", Port: 3306, DBType: constant.DBTMysql}
)
var instance1 = dtstruct.MysqlInstance{Instance: &cdtstruct.Instance{Key: i710k}}
var instance2 = dtstruct.MysqlInstance{Instance: &cdtstruct.Instance{Key: i720k}}
var instance3 = dtstruct.MysqlInstance{Instance: &cdtstruct.Instance{Key: i730k}}

func TestUnrecoverableError(t *testing.T) {
	test.S(t).ExpectTrue(UnrecoverableError(errors.New(constant.Error1045AccessDenied)))
}

func TestRemoveInstance(t *testing.T) {
	{
		instances := []*dtstruct.MysqlInstance{&instance1, &instance2}
		test.S(t).ExpectEquals(len(instances), 2)
		instances = RemoveNilInstances(instances)
		test.S(t).ExpectEquals(len(instances), 2)
	}
	{
		instances := []*dtstruct.MysqlInstance{&instance1, nil, &instance2}
		test.S(t).ExpectEquals(len(instances), 3)
		instances = RemoveNilInstances(instances)
		test.S(t).ExpectEquals(len(instances), 2)
	}
	{
		instances := []*dtstruct.MysqlInstance{&instance1, &instance2}
		test.S(t).ExpectEquals(len(instances), 2)
		instances = RemoveInstance(instances, &i710k)
		test.S(t).ExpectEquals(len(instances), 1)
		instances = RemoveInstance(instances, &i710k)
		test.S(t).ExpectEquals(len(instances), 1)
		instances = RemoveInstance(instances, &i720k)
		test.S(t).ExpectEquals(len(instances), 0)
		instances = RemoveInstance(instances, &i720k)
		test.S(t).ExpectEquals(len(instances), 0)
	}
}
