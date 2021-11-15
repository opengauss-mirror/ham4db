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
package dtstruct

import (
	"encoding/json"
	"gitee.com/opengauss/ham4db/go/adaptor/opengauss/common/constant"
	"gitee.com/opengauss/ham4db/go/dtstruct"
)

func init() {
	dtstruct.InstanceAdaptorMap[constant.DBTOpenGauss] = &OpenGaussInstance{}
}

// OpenGaussInstance hold opengauss instance basic info and sync info
type OpenGaussInstance struct {
	*dtstruct.Instance
	*SyncInfo
}

func (o *OpenGaussInstance) GetDatabaseType() string {
	return o.Instance.Key.DBType
}

func (o *OpenGaussInstance) GetHandler() dtstruct.InstanceAdaptor {
	return o
}

func (o *OpenGaussInstance) SetInstance(instance *dtstruct.Instance) {
	o.Instance = instance
}

func (o *OpenGaussInstance) HumanReadableDescription() string {
	return o.Instance.Key.String()
}

func (o *OpenGaussInstance) SetHstPrtAndClusterName(hostname string, port int, upstreamHostname string, upstreamPort int, clusterName string) {
	panic("implement me")
}

// ReplicaRunning instance should be replica and with valid last check and with valid upstream key
func (o *OpenGaussInstance) ReplicaRunning() bool {
	return o.IsReplica() && o.IsLastCheckValid && o.UpstreamKey.Hostname != "" && o.UpstreamKey.Port != 0
}

func (o *OpenGaussInstance) SetFlavorName() {
	o.FlavorName = constant.DBTOpenGauss
}

// IsReplicaServer no replica server in opengauss like binlog server in mysql
func (o *OpenGaussInstance) IsReplicaServer() bool {
	return false
}

// Less use replay location and promotion rule and physical location
func (o *OpenGaussInstance) Less(otherInst dtstruct.InstanceAdaptor, dataCenter string) bool {

	// false if nil
	if otherInst == nil {
		return false
	}

	// check xlog location
	other := otherInst.(*OpenGaussInstance)
	if o.ReceiverReplayLocation == other.ReceiverReplayLocation {

		// Prefer candidates:
		if other.PromotionRule.BetterThan(o.PromotionRule) {
			return true
		}

		// Prefer local datacenter:
		if dataCenter != "" && other.DataCenter == dataCenter && o.DataCenter != dataCenter {
			return true
		}

	}
	return o.ReceiverReplayLocation < other.ReceiverReplayLocation
}

func (o *OpenGaussInstance) IsReplica() bool {
	return o.Role == constant.DBStandby || o.Role == constant.DBCascade
}

func (o *OpenGaussInstance) GetInstance() *dtstruct.Instance {
	return o.Instance
}

func (o *OpenGaussInstance) GetHostname() string {
	return o.Key.Hostname
}

func (o *OpenGaussInstance) GetPort() int {
	return o.Key.Port
}

func (o *OpenGaussInstance) SetPromotionRule(rule dtstruct.CandidatePromotionRule) {
	o.PromotionRule = rule
}

func (o *OpenGaussInstance) MarshalJSON() ([]byte, error) {
	return json.Marshal(*o)
}

// NewInstance creates a new, empty instance
func NewInstance() *OpenGaussInstance {
	return &OpenGaussInstance{
		Instance: &dtstruct.Instance{
			Problems:         []string{},
			DownstreamKeyMap: make(map[dtstruct.InstanceKey]bool),
		},
		SyncInfo: &SyncInfo{},
	}
}
