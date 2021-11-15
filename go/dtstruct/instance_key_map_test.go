/*
   Copyright 2014 Outbrain Inc.

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
	"gitee.com/opengauss/ham4db/go/util"
	"math/rand"
	"testing"

	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/log"
	test "gitee.com/opengauss/ham4db/go/util/tests"
)

func init() {
	config.Config.HostnameResolveMethod = "none"
	config.MarkConfigurationLoaded()
	log.SetLevel(log.ERROR)
}

var clusterId = util.RandomHash32()
var key1 = InstanceKey{ClusterId: clusterId, DBType: constant.DBTMysql, Hostname: "host1", Port: 3306}
var key2 = InstanceKey{ClusterId: clusterId, DBType: constant.DBTMysql, Hostname: "host2", Port: 3306}
var key3 = InstanceKey{ClusterId: clusterId, DBType: constant.DBTMysql, Hostname: "host3", Port: 3306}

func TestGetInstanceKeys(t *testing.T) {
	for range rand.Perm(10) { // Just running many iterations to cover multiple possible map iteration ordering. Perm() is just used as an array generator here.
		m := *NewInstanceKeyMap()
		m.AddKey(key1)
		m.AddKey(key2)
		keys := m.GetInstanceKeys()
		test.S(t).ExpectEquals(keys[0], key1)
		test.S(t).ExpectEquals(keys[1], key2)
	}
	for range rand.Perm(10) { // Just running many iterations to cover multiple possible map iteration ordering. Perm() is just used as an array generator here.
		m := *NewInstanceKeyMap()
		m.AddKey(key2)
		m.AddKey(key1)
		keys := m.GetInstanceKeys()
		test.S(t).ExpectEquals(keys[0], key1)
		test.S(t).ExpectEquals(keys[1], key2)
	}
}

func TestInstanceKeyMapToJSON(t *testing.T) {
	m := *NewInstanceKeyMap()
	m.AddKey(key1)
	m.AddKey(key2)
	json, err := m.ToJSON()
	test.S(t).ExpectNil(err)
	ok := (json == `[{"DBType":"mysql","Hostname":"host1","Port":3306,"ClusterId":"`+clusterId+`"},{"DBType":"mysql","Hostname":"host2","Port":3306,"ClusterId":"`+clusterId+`"}]`) || (json == `[{"DBType":"mysql","Hostname":"host2","Port":3306,"ClusterId":"`+clusterId+`"},{"DBType":"mysql","Hostname":"host1","Port":3306,"ClusterId":"`+clusterId+`"}]`)
	test.S(t).ExpectTrue(ok)
}

func TestInstanceKeyMapReadJSON(t *testing.T) {
	json := `[{"DBType":"mysql","Hostname":"host1","Port":3306,"ClusterId":"` + clusterId + `"},{"DBType":"mysql","Hostname":"host2","Port":3306,"ClusterId":"` + clusterId + `"}]`
	m := *NewInstanceKeyMap()
	m.ReadJson(json)
	test.S(t).ExpectEquals(len(m), 2)
	test.S(t).ExpectTrue(m[key1])
	test.S(t).ExpectTrue(m[key2])
}

func TestEmptyInstanceKeyMapToCommaDelimitedList(t *testing.T) {
	m := *NewInstanceKeyMap()
	res := m.ToCommaDelimitedList()

	test.S(t).ExpectEquals(res, "")
}

func TestInstanceKeyMapToCommaDelimitedList(t *testing.T) {
	m := *NewInstanceKeyMap()
	m.AddKey(key1)
	m.AddKey(key2)
	res := m.ToCommaDelimitedList()

	ok := (res == key1.StringCode()+","+key2.StringCode()) || res == (key2.StringCode()+","+key1.StringCode())
	test.S(t).ExpectTrue(ok)
}

func TestIntersect(t *testing.T) {
	{
		m := NewInstanceKeyMap()
		m.AddKey(key1)
		m.AddKey(key2)

		other := NewInstanceKeyMap()
		other.AddKey(key3)
		other.AddKey(key2)

		intersected := m.Intersect(other)
		test.S(t).ExpectEquals(len(*intersected), 1)
	}
	{
		m := NewInstanceKeyMap()
		m.AddKey(key1)

		other := NewInstanceKeyMap()
		other.AddKey(key3)
		other.AddKey(key2)

		intersected := m.Intersect(other)
		test.S(t).ExpectEquals(len(*intersected), 0)
	}
	{
		m := NewInstanceKeyMap()
		m.AddKey(key1)
		m.AddKey(key2)

		other := NewInstanceKeyMap()
		other.AddKey(key1)
		other.AddKey(key3)
		other.AddKey(key2)

		intersected := m.Intersect(other)
		test.S(t).ExpectEquals(len(*intersected), 2)
	}

}
