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
package config

import (
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/util"
	test "gitee.com/opengauss/ham4db/go/util/tests"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func init() {
	Config.HostnameResolveMethod = "none"
	log.DisableOutput(true)
	log.SetFatalFunc(func() {})
}

// newConfFile create tmp file and write config to it
func newConfFile(content string) *os.File {
	tf, err := ioutil.TempFile("", constant.OSTempFilePatten)
	if err != nil {
		log.Fatalf("get error when create tmp file, error:%s", err)
	}
	if _, err = tf.WriteString(content); err != nil {
		log.Fatalf("get error when write to tmp file, error:%s", err)
	}
	if err = tf.Close(); err != nil {
		log.Fatalf("get error when close tmp file, error:%s", err)
	}
	return tf
}

func TestRead(t *testing.T) {
	var err error

	// normal case 1: read from config file
	cfs := newConfFile("{\"ListenAddress\": \"0.0.0.0:3123\"}")
	defer os.Remove(cfs.Name())
	conf, err := read(cfs.Name())
	test.S(t).ExpectTrue(conf != nil)
	test.S(t).ExpectNil(err)

	// failed case 1: failed file path
	flist := []string{
		"",
		strings.Join([]string{"", strconv.Itoa(time.Now().Second()), util.RandomHash()}, "/"),
	}
	for _, f := range flist {
		conf, err = read(f)
		test.S(t).ExpectTrue(err != nil)
	}

	// failed case 2: invalid json
	cff1 := newConfFile("{BackendDB: mysql_v5.7}")
	defer os.Remove(cff1.Name())
	conf, err = read(cff1.Name())
	test.S(t).ExpectNotNil(err)

	// failed case 3: adjust failed
	failedPath := strings.Join([]string{"", strconv.Itoa(time.Now().Second()), util.RandomHash()}, "/")
	cff2 := newConfFile("{\"MySQLCredentialsConfigFile\": \"" + failedPath + "\", \"MySQLTopologyCredentialsConfigFile\":\"" + failedPath + "\"}")
	defer os.Remove(cff2.Name())
	conf, err = read(cff2.Name())
	test.S(t).ExpectNotNil(err)

	// read config file created before
	Read(cfs.Name(), cff1.Name(), cff2.Name())
	test.S(t).ExpectEquals(Config.ListenAddress, "0.0.0.0:3123")
	test.S(t).ExpectEquals(Config.BackendDB, constant.DefaultBackendDB)
	test.S(t).ExpectEquals(Config.MySQLCredentialsConfigFile, failedPath)
}

func TestDBTypeCheck(t *testing.T) {

	// reload extra config file
	cf1 := newConfFile("{\"BackendDB\": \"sqlite3\",\"SQLite3DataFile\": \":memory:\"}")
	defer os.Remove(cf1.Name())
	_ = Reload(cf1.Name())
	test.S(t).ExpectTrue(Config.IsSQLite())

	// reload
	cf2 := newConfFile("{\"BackendDB\": \"mysql\"}")
	defer os.Remove(cf2.Name())
	_ = ForceRead(cf2.Name())
	test.S(t).ExpectTrue(Config.IsMySQL())
	test.S(t).ExpectEquals(Config.ListenAddress, "0.0.0.0:3123")
}

func TestReplicationLagQuery(t *testing.T) {
	{
		c := newConfiguration()
		c.SlaveLagQuery = "select 3"
		c.ReplicationLagQuery = "select 4"
		err := c.postReadAdjustment()
		test.S(t).ExpectNotNil(err)
	}
	{
		c := newConfiguration()
		c.SlaveLagQuery = "select 3"
		c.ReplicationLagQuery = "select 3"
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
	}
	{
		c := newConfiguration()
		c.SlaveLagQuery = "select 3"
		c.ReplicationLagQuery = ""
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.ReplicationLagQuery, "select 3")
	}
}

func TestPostponeReplicaRecoveryOnLagMinutes(t *testing.T) {
	{
		c := newConfiguration()
		c.PostponeSlaveRecoveryOnLagMinutes = 3
		c.PostponeReplicaRecoveryOnLagMinutes = 5
		err := c.postReadAdjustment()
		test.S(t).ExpectNotNil(err)
	}
	{
		c := newConfiguration()
		c.PostponeSlaveRecoveryOnLagMinutes = 3
		c.PostponeReplicaRecoveryOnLagMinutes = 3
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
	}
	{
		c := newConfiguration()
		c.PostponeSlaveRecoveryOnLagMinutes = 3
		c.PostponeReplicaRecoveryOnLagMinutes = 0
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.PostponeReplicaRecoveryOnLagMinutes, uint(3))
	}
}

func TestMasterFailoverDetachReplicaMasterHost(t *testing.T) {
	{
		c := newConfiguration()
		c.MasterFailoverDetachSlaveMasterHost = false
		c.MasterFailoverDetachReplicaMasterHost = false
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectFalse(c.MasterFailoverDetachReplicaMasterHost)
	}
	{
		c := newConfiguration()
		c.MasterFailoverDetachSlaveMasterHost = false
		c.MasterFailoverDetachReplicaMasterHost = true
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(c.MasterFailoverDetachReplicaMasterHost)
	}
	{
		c := newConfiguration()
		c.MasterFailoverDetachSlaveMasterHost = true
		c.MasterFailoverDetachReplicaMasterHost = false
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(c.MasterFailoverDetachReplicaMasterHost)
	}
}

func TestMasterFailoverDetachDetachLostReplicasAfterMasterFailover(t *testing.T) {
	{
		c := newConfiguration()
		c.DetachLostSlavesAfterMasterFailover = false
		c.DetachLostReplicasAfterMasterFailover = false
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectFalse(c.DetachLostReplicasAfterMasterFailover)
	}
	{
		c := newConfiguration()
		c.DetachLostSlavesAfterMasterFailover = false
		c.DetachLostReplicasAfterMasterFailover = true
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(c.DetachLostReplicasAfterMasterFailover)
	}
	{
		c := newConfiguration()
		c.DetachLostSlavesAfterMasterFailover = true
		c.DetachLostReplicasAfterMasterFailover = false
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(c.DetachLostReplicasAfterMasterFailover)
	}
}

func TestRecoveryPeriodBlock(t *testing.T) {
	{
		c := newConfiguration()
		c.RecoveryPeriodBlockSeconds = 0
		c.RecoveryPeriodBlockMinutes = 0
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.RecoveryPeriodBlockSeconds, 0)
	}
	{
		c := newConfiguration()
		c.RecoveryPeriodBlockSeconds = 30
		c.RecoveryPeriodBlockMinutes = 1
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.RecoveryPeriodBlockSeconds, 30)
	}
	{
		c := newConfiguration()
		c.RecoveryPeriodBlockSeconds = 0
		c.RecoveryPeriodBlockMinutes = 2
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.RecoveryPeriodBlockSeconds, 120)
	}
	{
		c := newConfiguration()
		c.RecoveryPeriodBlockSeconds = 15
		c.RecoveryPeriodBlockMinutes = 0
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.RecoveryPeriodBlockSeconds, 15)
	}
}

func TestRaft(t *testing.T) {
	{
		c := newConfiguration()
		c.RaftBind = "1.2.3.4:1008"
		c.RaftDataDir = "/path/to/somewhere"
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.RaftAdvertise, c.RaftBind)
	}
	{
		c := newConfiguration()
		c.RaftEnabled = true
		err := c.postReadAdjustment()
		test.S(t).ExpectNotNil(err)
	}
	{
		c := newConfiguration()
		c.RaftEnabled = true
		c.RaftDataDir = "/path/to/somewhere"
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
	}
	{
		c := newConfiguration()
		c.RaftEnabled = true
		c.RaftDataDir = "/path/to/somewhere"
		c.RaftBind = ""
		err := c.postReadAdjustment()
		test.S(t).ExpectNotNil(err)
	}
}

func TestHttpAdvertise(t *testing.T) {
	{
		c := newConfiguration()
		c.HTTPAdvertise = ""
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
	}
	{
		c := newConfiguration()
		c.HTTPAdvertise = "http://127.0.0.1:1234"
		err := c.postReadAdjustment()
		test.S(t).ExpectNil(err)
	}
	{
		c := newConfiguration()
		c.HTTPAdvertise = "http://127.0.0.1"
		err := c.postReadAdjustment()
		test.S(t).ExpectNotNil(err)
	}
	{
		c := newConfiguration()
		c.HTTPAdvertise = "127.0.0.1:1234"
		err := c.postReadAdjustment()
		test.S(t).ExpectNotNil(err)
	}
	{
		c := newConfiguration()
		c.HTTPAdvertise = "http://127.0.0.1:1234/mypath"
		err := c.postReadAdjustment()
		test.S(t).ExpectNotNil(err)
	}
}
