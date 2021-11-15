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
package agent

import (
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	dbtest "gitee.com/opengauss/ham4db/go/test"
	"gitee.com/opengauss/ham4db/go/util"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	test "gitee.com/opengauss/ham4db/go/util/tests"
	"strconv"
	"testing"
	"time"
)

var (
	ip       = "0.0.0.0"
	hostname = "test"
	port     = 11111
	interval = "10"
)

func init() {
	dbtest.DBTestInit()
}

// HealthCheck update or insert new agent to backend db and return the agent record in backend db
func TestAgentHealthCheck(t *testing.T) {

	// normal case 1: insert new agent to table ham_agent
	if err := HealthCheck(ip, hostname, strconv.Itoa(port), interval); err != nil {
		t.Errorf("%s", err)
	}
	count := 0
	if err := db.Query("select * from ham_agent", nil, func(rowMap sqlutil.RowMap) error {
		test.S(t).ExpectEquals(rowMap.GetString("agt_ip"), "0.0.0.0")
		test.S(t).ExpectEquals(rowMap.GetString("agt_hostname"), "test")
		test.S(t).ExpectEquals(rowMap.GetInt("agt_port"), 11111)
		test.S(t).ExpectEquals(rowMap.GetInt("check_interval"), 10)
		if t1, err := time.Parse(constant.DateFormatTimeZone, rowMap.GetString("last_updated_timestamp")); err != nil {
			t.Errorf("%s", err)
		} else {
			// should be finish in 10s
			test.S(t).ExpectTrue(time.Since(t1).Milliseconds() > 0 && time.Since(t1).Milliseconds() < 10000)
		}
		count++
		return nil
	}); err != nil {
		t.Errorf("%s", err)
	}
	test.S(t).ExpectEquals(count, 1)

	// normal case 2: update exist agent
	if err := HealthCheck(ip, hostname, strconv.Itoa(port), "20"); err != nil {
		t.Errorf("%s", err)
	}
	if err := db.Query("select * from ham_agent", nil, func(rowMap sqlutil.RowMap) error {
		test.S(t).ExpectEquals(rowMap.GetInt("check_interval"), 20)
		return nil
	}); err != nil {
		t.Errorf("%s", err)
	}

	// failed case 1: illegal agent info
	testCase := [][]string{
		{"", "", "", ""},               // illegal all
		{"1111", "1111", "888", "10"},  // illegal port
		{"1111", "1111", "8888", "-1"}, // illegal interval
	}
	for _, cased := range testCase {
		if err := HealthCheck(cased[0], cased[1], cased[2], cased[3]); err == nil {
			t.Fatal("should have error here")
		}
		if err := db.Query("select count(*) as cnt from ham_agent", nil, func(rowMap sqlutil.RowMap) error {
			test.S(t).ExpectEquals(rowMap.GetInt("cnt"), 1)
			return nil
		}); err != nil {
			t.Errorf("%s", err)
		}
	}
}

func TestConnectToAgent(t *testing.T) {

	// get instance key
	instanceKey := &dtstruct.InstanceKey{Hostname: hostname, Port: port, ClusterId: util.RandomHash32()}

	// failed case 1: agent not exist
	conn, err := ConnectToAgent(instanceKey)
	test.S(t).ExpectTrue(conn == nil)
	test.S(t).ExpectTrue(err != nil)
}
