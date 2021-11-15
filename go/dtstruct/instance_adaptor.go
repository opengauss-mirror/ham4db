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
	"gitee.com/opengauss/ham4db/go/config"
	"regexp"
)

// InstanceAdaptor interface that instance need to implement
type InstanceAdaptor interface {

	// get instance database type
	GetDatabaseType() string

	// get instance hostname
	GetHostname() string

	// get instance port
	GetPort() int

	// get handler for this instance
	GetHandler() InstanceAdaptor

	// get instance all replicas
	GetReplicas() InstanceKeyMap

	// get upstream and downstream instance
	GetAssociateInstance() []InstanceKey

	// get instance info
	GetInstance() *Instance

	// get instance description
	HumanReadableDescription() string

	// set promotion rule for candidate instance
	SetPromotionRule(CandidatePromotionRule)

	// set hostname/port/cluster name
	SetHstPrtAndClusterName(hostname string, port int, upstreamHostname string, upstreamPort int, clusterName string)

	// set instance
	SetInstance(*Instance)

	// set instance favor name
	SetFlavorName()

	// check if instance's replication is running
	ReplicaRunning() bool

	// check if instance is a binlog serve
	IsReplicaServer() bool

	// check if instance is replica
	IsReplica() bool

	// for instance sort
	Less(handler InstanceAdaptor, dataCenter string) bool
}

// IsUpstreamOf checks whether an instance is the upstream of another
func IsUpstreamOf(upstream, downstream InstanceAdaptor) bool {

	// instance should not be nil and downstream must be replica
	if upstream == nil || downstream == nil || !downstream.IsReplica() {
		return false
	}

	// instance cannot be upstream of itself
	if upstream.GetInstance().Key.Equals(&downstream.GetInstance().Key) {
		return false
	}

	return upstream.GetInstance().Key.Equals(&downstream.GetInstance().UpstreamKey)
}

// IsSibling checks whether both instances are replicating from same master
func IsSibling(instance0, instance1 InstanceAdaptor) bool {

	// should not be nil and must be replica
	if instance0 == nil || instance1 == nil || !instance0.IsReplica() || !instance1.IsReplica() {
		return false
	}

	// cannot be sibling of itself
	if instance0.GetInstance().Key.Equals(&instance1.GetInstance().Key) {
		return false
	}

	return instance0.GetInstance().UpstreamKey.Equals(&instance1.GetInstance().UpstreamKey)
}

// IsBannedFromBeingCandidateReplica check if replica is banned from being candidate
func IsBannedFromBeingCandidateReplica(replica InstanceAdaptor) bool {

	// banned by promote rule
	if replica.GetInstance().PromotionRule == MustNotPromoteRule {
		return true
	}

	// banned by ignore hostname
	for _, filter := range config.Config.PromotionIgnoreHostnameFilters {
		if matched, _ := regexp.MatchString(filter, replica.GetInstance().Key.Hostname); matched {
			return true
		}
	}
	return false
}

// RemoveInstance will remove an instance from a list of instances
func RemoveInstance(instanceList []InstanceAdaptor, instanceKey *InstanceKey) []InstanceAdaptor {
	if instanceKey == nil {
		return instanceList
	}
	for i := len(instanceList) - 1; i >= 0; i-- {
		if instanceList[i].GetInstance().Key.Equals(instanceKey) {
			instanceList = append(instanceList[:i], instanceList[i+1:]...)
		}
	}
	return instanceList
}
