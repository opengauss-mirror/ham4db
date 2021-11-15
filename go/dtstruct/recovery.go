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
	"fmt"

	"time"
)

type RecoveryType string

const (
	NotMasterRecovery              RecoveryType = "NotMasterRecovery"
	MasterRecovery                              = "MasterRecovery"
	CoMasterRecovery                            = "CoMasterRecovery"
	IntermediateMasterRecovery                  = "IntermediateMasterRecovery"
	ReplicationGroupMemberRecovery              = "ReplicationGroupMemberRecovery"
)

type RecoveryAcknowledgement struct {
	CreatedAt time.Time
	Owner     string
	Comment   string

	Key           InstanceKey
	ClusterName   string
	Id            int64
	UID           string
	AllRecoveries bool
}

func NewRecoveryAcknowledgement(owner string, comment string) *RecoveryAcknowledgement {
	return &RecoveryAcknowledgement{
		CreatedAt: time.Now(),
		Owner:     owner,
		Comment:   comment,
	}
}

func NewInternalAcknowledgement() *RecoveryAcknowledgement {
	return &RecoveryAcknowledgement{
		CreatedAt: time.Now(),
		Owner:     "ham4db",
		Comment:   "internal",
	}
}

// BlockedTopologyRecovery represents an entry in the blocked_topology_recovery table
type BlockedTopologyRecovery struct {
	FailedInstanceKey    InstanceKey
	ClusterName          string
	Analysis             AnalysisCode
	LastBlockedTimestamp string
	BlockingRecoveryId   int64
}

// TopologyRecovery represents an entry in the topology_recovery table
type TopologyRecovery struct {
	PostponedFunctionsContainer

	Id                        int64
	UID                       string
	AnalysisEntry             ReplicationAnalysis
	SuccessorKey              *InstanceKey
	SuccessorAlias            string
	IsActive                  bool
	IsSuccessful              bool
	LostReplicas              InstanceKeyMap
	ParticipatingInstanceKeys InstanceKeyMap
	AllErrors                 []string
	RecoveryStartTimestamp    string
	RecoveryEndTimestamp      string
	ProcessingNodeHostname    string
	ProcessingNodeToken       string
	Acknowledged              bool
	AcknowledgedAt            string
	AcknowledgedBy            string
	AcknowledgedComment       string
	LastDetectionId           int64
	RelatedRecoveryId         int64
	Type                      RecoveryType
	RecoveryType              RecoveryType
}

func NewTopologyRecovery(replicationAnalysis ReplicationAnalysis) *TopologyRecovery {
	topologyRecovery := &TopologyRecovery{}
	topologyRecovery.UID = fmt.Sprintf("%d:%s", time.Now().UnixNano(), NewToken().Hash)
	topologyRecovery.AnalysisEntry = replicationAnalysis
	topologyRecovery.SuccessorKey = nil
	topologyRecovery.LostReplicas = *NewInstanceKeyMap()
	topologyRecovery.ParticipatingInstanceKeys = *NewInstanceKeyMap()
	topologyRecovery.AllErrors = []string{}
	topologyRecovery.RecoveryType = NotMasterRecovery
	return topologyRecovery
}
func (this *TopologyRecovery) AddError(err error) error {
	if err != nil {
		this.AllErrors = append(this.AllErrors, err.Error())
	}
	return err
}

func (this *TopologyRecovery) AddErrors(errs []error) {
	for _, err := range errs {
		this.AddError(err)
	}
}

type TopologyRecoveryStep struct {
	Id          int64
	RecoveryUID string
	AuditAt     string
	Message     string
}

func NewTopologyRecoveryStep(uid string, message string) *TopologyRecoveryStep {
	return &TopologyRecoveryStep{
		RecoveryUID: uid,
		Message:     message,
	}
}
