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
package base

import (
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util"
	test "gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
)

func init() {
	config.Config.HostnameResolveMethod = "none"
	config.MarkConfigurationLoaded()
	log.SetLevel(log.ERROR)
}

var clusterId = util.RandomHash32()
var key1 = dtstruct.InstanceKey{ClusterId: clusterId, Hostname: "host1", Port: 3306, DBType: constant.TestDB}
var key2 = dtstruct.InstanceKey{ClusterId: clusterId, Hostname: "host2", Port: 3306, DBType: constant.TestDB}
var key3 = dtstruct.InstanceKey{ClusterId: clusterId, Hostname: "host3", Port: 3306, DBType: constant.TestDB}

func TestInstanceKeyDetach(t *testing.T) {
	test.S(t).ExpectFalse(key1.IsDetached())
	detached1 := key1.DetachedKey()
	test.S(t).ExpectTrue(detached1.IsDetached())
	detached2 := key1.DetachedKey()
	test.S(t).ExpectTrue(detached2.IsDetached())
	test.S(t).ExpectTrue(detached1.Equals(detached2))

	reattached1 := detached1.ReattachedKey()
	test.S(t).ExpectFalse(reattached1.IsDetached())
	test.S(t).ExpectTrue(reattached1.Equals(&key1))
	reattached2 := reattached1.ReattachedKey()
	test.S(t).ExpectFalse(reattached2.IsDetached())
	test.S(t).ExpectTrue(reattached1.Equals(reattached2))
}

func TestNewResolveInstanceKey(t *testing.T) {
	{
		i, err := NewResolveInstanceKey(constant.TestDB, "127.0.0.1", 3308)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(i.Hostname, "127.0.0.1")
		test.S(t).ExpectEquals(i.Port, 3308)
	}
	{
		_, err := NewResolveInstanceKey(constant.TestDB, "", 3309)
		test.S(t).ExpectNotNil(err)
	}
	{
		i, err := NewResolveInstanceKey(constant.TestDB, "127.0.0.1", 0)
		test.S(t).ExpectNil(err)
		test.S(t).ExpectFalse(i.IsValid())
	}
}

func TestParseResolveInstanceKey(t *testing.T) {
	{
		key, err := ParseResolveInstanceKey(constant.TestDB, "myhost:1234")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "myhost")
		test.S(t).ExpectEquals(key.Port, 1234)
	}
	{
		key, err := ParseResolveInstanceKey(constant.TestDB, "myhost")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "myhost")
		test.S(t).ExpectEquals(key.Port, 0)
	}
	{
		key, err := ParseResolveInstanceKey(constant.TestDB, "10.0.0.3:3307")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "10.0.0.3")
		test.S(t).ExpectEquals(key.Port, 3307)
	}
	{
		key, err := ParseResolveInstanceKey(constant.TestDB, "10.0.0.3")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "10.0.0.3")
		test.S(t).ExpectEquals(key.Port, 0)
	}
	{
		key, err := ParseResolveInstanceKey(constant.TestDB, "[2001:db8:1f70::999:de8:7648:6e8]:3308")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "2001:db8:1f70::999:de8:7648:6e8")
		test.S(t).ExpectEquals(key.Port, 3308)
	}
	{
		key, err := ParseResolveInstanceKey(constant.TestDB, "::1")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "::1")
		test.S(t).ExpectEquals(key.Port, 0)
	}
	{
		key, err := ParseResolveInstanceKey(constant.TestDB, "0:0:0:0:0:0:0:0")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(key.Hostname, "0:0:0:0:0:0:0:0")
		test.S(t).ExpectEquals(key.Port, 0)
	}
	{
		_, err := ParseResolveInstanceKey(constant.TestDB, "[2001:xxxx:1f70::999:de8:7648:6e8]:3308")
		test.S(t).ExpectNotNil(err)
	}
	{
		_, err := ParseResolveInstanceKey(constant.TestDB, "10.0.0.4:")
		test.S(t).ExpectNotNil(err)
	}
	{
		_, err := ParseResolveInstanceKey(constant.TestDB, "10.0.0.4:5.6.7")
		test.S(t).ExpectNotNil(err)
	}
}

func TestNewResolveInstanceKeyStrings(t *testing.T) {
	{
		i, err := NewResolveInstanceKeyStrings(constant.TestDB, "127.0.0.1", "3306")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(i.Hostname, "127.0.0.1")
		test.S(t).ExpectEquals(i.Port, 3306)
	}
	{
		_, err := NewResolveInstanceKeyStrings(constant.TestDB, "127.0.0.1", "")
		test.S(t).ExpectNotNil(err)
	}
	{
		_, err := NewResolveInstanceKeyStrings(constant.TestDB, "127.0.0.1", "3306x")
		test.S(t).ExpectNotNil(err)
	}
}

func TestInstanceKeyValid(t *testing.T) {
	test.S(t).ExpectTrue(key1.IsValid())
	i, err := ParseResolveInstanceKey(constant.TestDB, "_:3306")
	test.S(t).ExpectNil(err)
	test.S(t).ExpectFalse(i.IsValid())
	i, err = ParseResolveInstanceKey(constant.TestDB, "//myhost:3306")
	test.S(t).ExpectNil(err)
	test.S(t).ExpectFalse(i.IsValid())
}
func TestIsIPv4(t *testing.T) {
	test.S(t).ExpectFalse(key1.IsIPv4())
	{
		k, _ := ParseRawInstanceKey(constant.TestDB, "mysql-server-1:3306")
		test.S(t).ExpectFalse(k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey(constant.TestDB, "mysql-server-1")
		test.S(t).ExpectFalse(k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey(constant.TestDB, "my.sql.server.1")
		test.S(t).ExpectFalse(k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey(constant.TestDB, "mysql-server-1:3306")
		test.S(t).ExpectFalse(k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey(constant.TestDB, "127.0.0:3306")
		test.S(t).ExpectFalse(k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey(constant.TestDB, "127::0::0::1:3306")
		test.S(t).ExpectFalse(k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey(constant.TestDB, "127.0.0.1:3306")
		test.S(t).ExpectTrue(k.IsIPv4())
	}
	{
		k, _ := ParseRawInstanceKey(constant.TestDB, "127.0.0.1")
		test.S(t).ExpectTrue(k.IsIPv4())
	}
}
