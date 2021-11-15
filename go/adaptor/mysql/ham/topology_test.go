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
	mdtstruct "gitee.com/opengauss/ham4db/go/adaptor/mysql/dtstruct"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	test "gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
)

var (
	i710Key = dtstruct.InstanceKey{Hostname: "i710", Port: 3306}
	i720Key = dtstruct.InstanceKey{Hostname: "i720", Port: 3306}
	i730Key = dtstruct.InstanceKey{Hostname: "i730", Port: 3306}
	i810Key = dtstruct.InstanceKey{Hostname: "i810", Port: 3306}
	i820Key = dtstruct.InstanceKey{Hostname: "i820", Port: 3306}
	i830Key = dtstruct.InstanceKey{Hostname: "i830", Port: 3306}
)

func init() {
	config.Config.HostnameResolveMethod = "none"
	config.MarkConfigurationLoaded()
	log.SetLevel(log.ERROR)
}

func generateTestInstances() (instances [](*mdtstruct.MysqlInstance), instancesMap map[string](*mdtstruct.MysqlInstance)) {
	i710 := mdtstruct.MysqlInstance{Instance: &dtstruct.Instance{Key: i710Key, InstanceId: "710"}, ExecBinlogCoordinates: dtstruct.LogCoordinates{LogFile: "mysql.000007", LogPos: 10}}
	i720 := mdtstruct.MysqlInstance{Instance: &dtstruct.Instance{Key: i720Key, InstanceId: "720"}, ExecBinlogCoordinates: dtstruct.LogCoordinates{LogFile: "mysql.000007", LogPos: 20}}
	i730 := mdtstruct.MysqlInstance{Instance: &dtstruct.Instance{Key: i730Key, InstanceId: "730"}, ExecBinlogCoordinates: dtstruct.LogCoordinates{LogFile: "mysql.000007", LogPos: 30}}
	i810 := mdtstruct.MysqlInstance{Instance: &dtstruct.Instance{Key: i810Key, InstanceId: "810"}, ExecBinlogCoordinates: dtstruct.LogCoordinates{LogFile: "mysql.000008", LogPos: 10}}
	i820 := mdtstruct.MysqlInstance{Instance: &dtstruct.Instance{Key: i820Key, InstanceId: "820"}, ExecBinlogCoordinates: dtstruct.LogCoordinates{LogFile: "mysql.000008", LogPos: 20}}
	i830 := mdtstruct.MysqlInstance{Instance: &dtstruct.Instance{Key: i830Key, InstanceId: "830"}, ExecBinlogCoordinates: dtstruct.LogCoordinates{LogFile: "mysql.000008", LogPos: 30}}
	instances = [](*mdtstruct.MysqlInstance){&i710, &i720, &i730, &i810, &i820, &i830}
	for _, instance := range instances {
		instance.Version = "5.6.7"
		instance.Binlog_format = "STATEMENT"
	}
	instancesMap = make(map[string](*mdtstruct.MysqlInstance))
	for _, instance := range instances {
		instancesMap[instance.Key.StringCode()] = instance
	}
	return instances, instancesMap
}

func applyGeneralGoodToGoReplicationParams(instances [](*mdtstruct.MysqlInstance)) {
	for _, instance := range instances {
		instance.IsLastCheckValid = true
		instance.LogBinEnabled = true
		instance.LogReplicationUpdatesEnabled = true
	}
}

func TestIsGenerallyValidAsBinlogSource(t *testing.T) {
	instances, _ := generateTestInstances()
	for _, instance := range instances {
		test.S(t).ExpectFalse(isGenerallyValidAsBinlogSource(instance))
	}
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		test.S(t).ExpectTrue(isGenerallyValidAsBinlogSource(instance))
	}
}

func TestIsGenerallyValidAsCandidateReplica(t *testing.T) {
	instances, _ := generateTestInstances()
	for _, instance := range instances {
		test.S(t).ExpectFalse(isGenerallyValidAsCandidateReplica(instance))
	}
	for _, instance := range instances {
		instance.IsLastCheckValid = true
		instance.LogBinEnabled = true
		instance.LogReplicationUpdatesEnabled = false
	}
	for _, instance := range instances {
		test.S(t).ExpectFalse(isGenerallyValidAsCandidateReplica(instance))
	}
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		test.S(t).ExpectTrue(isGenerallyValidAsCandidateReplica(instance))
	}
}

func TestChooseCandidateReplicaNoCandidateReplica(t *testing.T) {
	instances, _ := generateTestInstances()
	for _, instance := range instances {
		instance.IsLastCheckValid = true
		instance.LogBinEnabled = true
		instance.LogReplicationUpdatesEnabled = false
	}
	_, _, _, _, _, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNotNil(err)
}

func TestChooseCandidateReplica(t *testing.T) {
	instances, _ := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i830Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 5)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplica2(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].LogReplicationUpdatesEnabled = false
	instancesMap[i820Key.StringCode()].LogBinEnabled = false
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i810Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 2)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 3)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaSameCoordinatesDifferentVersions(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instances[0].ExecBinlogCoordinates
	}
	instancesMap[i810Key.StringCode()].Version = "5.5.1"
	instancesMap[i720Key.StringCode()].Version = "5.7.8"
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i810Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 5)
	test.S(t).ExpectEquals(len(laterReplicas), 0)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityVersionNoLoss(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Version = "5.5.1"
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i830Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 5)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityVersionLosesOne(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Version = "5.7.8"
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i820Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 1)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 4)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityVersionLosesTwo(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Version = "5.7.8"
	instancesMap[i820Key.StringCode()].Version = "5.7.18"
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i810Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 2)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 3)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityVersionHigherVersionOverrides(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Version = "5.7.8"
	instancesMap[i820Key.StringCode()].Version = "5.7.18"
	instancesMap[i810Key.StringCode()].Version = "5.7.5"
	instancesMap[i730Key.StringCode()].Version = "5.7.30"
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i830Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 3)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 2)
}

func TestChooseCandidateReplicaLosesOneDueToBinlogFormat(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.Binlog_format = "ROW"
	}
	instancesMap[i730Key.StringCode()].Binlog_format = "STATEMENT"

	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i830Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 4)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 1)
}

func TestChooseCandidateReplicaPriorityBinlogFormatNoLoss(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.Binlog_format = "MIXED"
	}
	instancesMap[i830Key.StringCode()].Binlog_format = "STATEMENT"
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i830Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 5)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityBinlogFormatLosesOne(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Binlog_format = "ROW"
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i820Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 1)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 4)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityBinlogFormatLosesTwo(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Binlog_format = "ROW"
	instancesMap[i820Key.StringCode()].Binlog_format = "ROW"
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i810Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 2)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 3)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPriorityBinlogFormatRowOverrides(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].Binlog_format = "ROW"
	instancesMap[i820Key.StringCode()].Binlog_format = "ROW"
	instancesMap[i810Key.StringCode()].Binlog_format = "ROW"
	instancesMap[i730Key.StringCode()].Binlog_format = "ROW"
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i830Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 3)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 2)
}

func TestChooseCandidateReplicaMustNotPromoteRule(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].PromotionRule = dtstruct.MustNotPromoteRule
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i820Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 1)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 4)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPreferNotPromoteRule(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	instancesMap[i830Key.StringCode()].PromotionRule = dtstruct.MustNotPromoteRule
	instancesMap[i820Key.StringCode()].PromotionRule = dtstruct.PreferNotPromoteRule
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i820Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 1)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 4)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPreferNotPromoteRule2(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.PromotionRule = dtstruct.PreferNotPromoteRule
	}
	instancesMap[i830Key.StringCode()].PromotionRule = dtstruct.MustNotPromoteRule
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i820Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 1)
	test.S(t).ExpectEquals(len(equalReplicas), 0)
	test.S(t).ExpectEquals(len(laterReplicas), 4)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPromoteRuleOrdering(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instancesMap[i710Key.StringCode()].ExecBinlogCoordinates
		instance.PromotionRule = dtstruct.NeutralPromoteRule
	}
	instancesMap[i830Key.StringCode()].PromotionRule = dtstruct.PreferPromoteRule
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i830Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 5)
	test.S(t).ExpectEquals(len(laterReplicas), 0)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPromoteRuleOrdering2(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instancesMap[i710Key.StringCode()].ExecBinlogCoordinates
		instance.PromotionRule = dtstruct.PreferPromoteRule
	}
	instancesMap[i820Key.StringCode()].PromotionRule = dtstruct.MustPromoteRule
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i820Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 5)
	test.S(t).ExpectEquals(len(laterReplicas), 0)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}

func TestChooseCandidateReplicaPromoteRuleOrdering3(t *testing.T) {
	instances, instancesMap := generateTestInstances()
	applyGeneralGoodToGoReplicationParams(instances)
	for _, instance := range instances {
		instance.ExecBinlogCoordinates = instancesMap[i710Key.StringCode()].ExecBinlogCoordinates
		instance.PromotionRule = dtstruct.NeutralPromoteRule
	}
	instancesMap[i730Key.StringCode()].PromotionRule = dtstruct.MustPromoteRule
	instancesMap[i810Key.StringCode()].PromotionRule = dtstruct.PreferPromoteRule
	instancesMap[i830Key.StringCode()].PromotionRule = dtstruct.PreferNotPromoteRule
	instances = SortedReplicasDataCenterHint(instances, constant.NoStopReplication, "")
	candidate, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err := chooseCandidateReplica(instances)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(candidate.Key, i730Key)
	test.S(t).ExpectEquals(len(aheadReplicas), 0)
	test.S(t).ExpectEquals(len(equalReplicas), 5)
	test.S(t).ExpectEquals(len(laterReplicas), 0)
	test.S(t).ExpectEquals(len(cannotReplicateReplicas), 0)
}
