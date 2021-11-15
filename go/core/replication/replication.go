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
	"context"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"time"
)

// StartReplication start replication on the given instance.
func StartReplication(instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).StartReplication(context.TODO(), instanceKey)
}

// RestartReplication stop & start replication on the given instance
func RestartReplication(instanceKey *dtstruct.InstanceKey) (detail interface{}, err error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).RestartReplication(instanceKey)
}

// StopReplication stop replication on the given instance
func StopReplication(instanceKey *dtstruct.InstanceKey) (interface{}, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).StopReplication(instanceKey)
}

// StopReplicationNicely stop a replica, wait until all log be consumed, ensure that data is not lost as much as possible
func StopReplicationNicely(instanceKey *dtstruct.InstanceKey, timeout time.Duration) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).StopReplicationNicely(instanceKey, timeout)
}

// ResetReplication will reset a replica's replication
func ResetReplication(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).ResetReplication(instanceKey)
}

// DelayReplication set the replication delay given seconds on the given instance
func DelayReplication(instanceKey *dtstruct.InstanceKey, seconds int) error {
	return dtstruct.GetHamHandler(instanceKey.DBType).DelayReplication(instanceKey, seconds)
}

// CanReplicateFrom check if first instance can practically replicate from other instance
func CanReplicateFrom(first dtstruct.InstanceAdaptor, other dtstruct.InstanceAdaptor) (bool, error) {
	return dtstruct.GetHamHandler(first.GetInstance().Key.DBType).CanReplicateFrom(first, other)
}

// SetReadOnly set or clear the instance's read only setting
func SetReadOnly(instanceKey *dtstruct.InstanceKey, readOnly bool) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).SetReadOnly(instanceKey, readOnly)
}

// SetSemiSyncOnDownstream enable or disable semi sync replication on the downstream instance
func SetSemiSyncOnDownstream(instanceKey *dtstruct.InstanceKey, enable bool) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).SetSemiSyncOnDownstream(instanceKey, enable)
}

// SetSemiSyncOnUpstream enable or disable semi sync replication on the upstream instance
func SetSemiSyncOnUpstream(instanceKey *dtstruct.InstanceKey, enable bool) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).SetSemiSyncOnUpstream(instanceKey, enable)
}

// GetReplicationAnalysis will check for replication problems (dead master; unreachable master; etc)
func GetReplicationAnalysis(dbt string, clusterName, clusterId string, hints *dtstruct.ReplicationAnalysisHints) ([]dtstruct.ReplicationAnalysis, error) {
	return dtstruct.GetHamHandler(dbt).GetReplicationAnalysis(clusterName, clusterId, hints)
}
