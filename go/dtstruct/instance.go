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
	"database/sql"
	"strconv"
	"strings"

	"time"
)

const ReasonableDiscoveryLatency = 500 * time.Millisecond

// Instance represents a database instance, including its current configuration & status.
// It presents important replication configuration and detailed replication status.
type Instance struct {
	Uptime                    uint
	Key                       InstanceKey
	InstanceId                string
	InstanceAlias             string
	ClusterId                 string
	Version                   string
	VersionComment            string
	UpstreamKey               InstanceKey
	DownstreamKeyMap          InstanceKeyMap
	Role                      string
	DBState                   string
	IsCoUpstream              bool
	ReadOnly                  bool
	LastSeenTimestamp         string
	IsLastCheckValid          bool
	IsUpToDate                bool
	IsRecentlyChecked         bool
	ClusterName               string
	FlavorName                string
	DataCenter                string
	Region                    string
	Environment               string
	SuggestedClusterAlias     string
	ReplicationState          string
	ReplicationDepth          uint
	HasReplicationFilters     bool
	AllowTLS                  bool
	HasReplicationCredentials bool

	IsDetachedMaster bool

	SlaveLagSeconds       sql.NullInt64 // for API backwards compatibility. Equals `ReplicationLagSeconds`
	ReplicationLagSeconds sql.NullInt64

	ReplicationCredentialsAvailable bool

	SecondsSinceLastSeen sql.NullInt64

	// Careful. IsCandidate and PromotionRule are used together
	// and probably need to be merged. IsCandidate's value may
	// be picked up from daabase_candidate_instance's value when
	// reading an instance from the db.
	IsCandidate          bool
	PromotionRule        CandidatePromotionRule
	IsDowntimed          bool
	DowntimeReason       string
	DowntimeOwner        string
	DowntimeEndTimestamp string
	ElapsedDowntime      time.Duration
	UnresolvedHostname   string

	Problems []string

	LastDiscoveryLatency time.Duration

	Seed bool // Means we force this instance to be written to backend, even if it's invalid, empty or forgotten

	//InstanceAdaptor
}

// NewInstance creates a new, empty instance
func NewInstance() *Instance {
	return &Instance{
		DownstreamKeyMap: make(map[InstanceKey]bool),
		Problems:         []string{},
	}
}

func (this *Instance) GetAssociateInstance() (instanceList []InstanceKey) {
	for key := range this.DownstreamKeyMap {
		instanceList = append(instanceList, key)
	}
	if this.UpstreamKey.IsValid() {
		instanceList = append(instanceList, this.UpstreamKey)
	}
	return
}

//TODO=============================

// MajorVersion returns this instance's major version number (e.g. for 5.5.36 it returns "5.5")
func (this *Instance) MajorVersion() []string {
	return MajorVersion(this.Version)
}

func (this *Instance) GetReplicas() InstanceKeyMap {
	return this.DownstreamKeyMap
}

// MajorVersion returns a MySQL major version number (e.g. given "5.5.36" it returns "5.5")
func MajorVersion(version string) []string {
	tokens := strings.Split(version, ".")
	if len(tokens) < 2 {
		return []string{"0", "0"}
	}
	return tokens[:2]
}

// IsSmallerMajorVersion tests this instance against another and returns true if this instance is of a smaller "major" varsion.
// e.g. 5.5.36 is NOT a smaller major version as comapred to 5.5.36, but IS as compared to 5.6.9
func (this *Instance) IsSmallerMajorVersion(other *Instance) bool {
	return IsSmallerMajorVersion(this.Version, other.Version)
}

// IsSmallerMajorVersion tests two versions against another and returns true if
// the former is a smaller "major" varsion than the latter.
// e.g. 5.5.36 is NOT a smaller major version as comapred to 5.5.40, but IS as compared to 5.6.9
func IsSmallerMajorVersion(version string, otherVersion string) bool {
	thisMajorVersion := MajorVersion(version)
	otherMajorVersion := MajorVersion(otherVersion)
	for i := 0; i < len(thisMajorVersion); i++ {
		thisToken, _ := strconv.Atoi(thisMajorVersion[i])
		otherToken, _ := strconv.Atoi(otherMajorVersion[i])
		if thisToken < otherToken {
			return true
		}
		if thisToken > otherToken {
			return false
		}
	}
	return false
}

// AddDownstreamKey adds a replica to the list of this instance's replicas.
func (this *Instance) AddDownstreamKey(replicaKey *InstanceKey) {
	this.DownstreamKeyMap.AddKey(*replicaKey)
}

// IsSmallerMajorVersionByString checks if this instance has a smaller major version number than given one
func (this *Instance) IsSmallerMajorVersionByString(otherVersion string) bool {
	return IsSmallerMajorVersion(this.Version, otherVersion)
}

// MajorVersion returns this instance's major version number (e.g. for 5.5.36 it returns "5.5")
func (this *Instance) MajorVersionString() string {
	return strings.Join(this.MajorVersion(), ".")
}

func (this *Instance) IsSeed() bool {
	return this.Seed
}

func (this *Instance) SetSeed() {
	this.Seed = true
}

func (this *Instance) GetHostname() string {
	panic("should not be called, use database handler instead")
}
func (this *Instance) GetPort() int { panic("should not be called, use database handler instead") }
func (this *Instance) SetPromotionRule(CandidatePromotionRule) {
	panic("should not be called, use database handler instead")
}
func (this *Instance) SetHstPrtAndClusterName(hostname string, port int, upstreamHostname string, upstreamPort int, clusterName string) {
	panic("should not be called, use database handler instead")
}
func (this *Instance) ReplicaRunning() bool {
	panic("should not be called, use database handler instead")
}
func (this *Instance) IsReplicaServer() bool {
	panic("should not be called, use database handler instead")
}
func (this *Instance) Less(handler InstanceAdaptor, dataCenter string) bool {
	panic("should not be called, use database handler instead")
}
func (this *Instance) IsReplica() bool { panic("should not be called, use database handler instead") }
func (this *Instance) GetInstance() *Instance {
	panic("should not be called, use database handler instead")
}
