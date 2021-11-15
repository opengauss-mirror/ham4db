/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

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

package consensus

import (
	"context"
	"encoding/json"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/consensus/raft"
	"gitee.com/opengauss/ham4db/go/core/discover"

	"gitee.com/opengauss/ham4db/go/core/instance"
	"gitee.com/opengauss/ham4db/go/core/kv"
	"gitee.com/opengauss/ham4db/go/dtstruct"

	"gitee.com/opengauss/ham4db/go/core/log"
)

// AsyncRequest represents an entry in the async_request table
type CommandDBApplier struct {
}

func NewCommandApplier() *CommandDBApplier {
	applier := &CommandDBApplier{}
	return applier
}

func (applier *CommandDBApplier) ApplyCommand(op string, value []byte) interface{} {
	switch op {
	case "heartbeat":
		return nil
	case "async-snapshot":
		return applier.asyncSnapshot(value)
	case "register-node":
		return applier.registerNode(value)
	case "discover":
		return applier.discover(value)
	case "injected-pseudo-gtid":
		return applier.injectedPseudoGTID(value)
	case "forget":
		return applier.forget(value)
	case "forget-cluster":
		return applier.forgetCluster(value)
	case "begin-downtime":
		return applier.beginDowntime(value)
	case "end-downtime":
		return applier.endDowntime(value)
	case "register-candidate":
		return applier.registerCandidate(value)
	case "ack-recovery":
		return applier.ackRecovery(value)
	case "register-hostname-unresolve":
		return applier.registerHostnameUnresolve(value)
	case "submit-pool-instances":
		return applier.submitPoolInstances(value)
	case "register-failure-detection":
		return applier.registerFailureDetection(value)
	case "write-recovery":
		return applier.writeRecovery(value)
	case "write-recovery-step":
		return applier.writeRecoveryStep(value)
	case "resolve-recovery":
		return applier.resolveRecovery(value)
	case "disable-global-recoveries":
		return applier.disableGlobalRecoveries(value)
	case "enable-global-recoveries":
		return applier.enableGlobalRecoveries(value)
	case "put-key-value":
		return applier.putKeyValue(value)
	case "put-instance-tag":
		return applier.putInstanceTag(value)
	case "delete-instance-tag":
		return applier.deleteInstanceTag(value)
	case "leader-uri":
		return applier.leaderURI(value)
	case "request-health-report":
		return applier.healthReport(value)
	case "set-cluster-alias-manual-override":
		return applier.setClusterAliasManualOverride(value)
	}
	return log.Errorf("Unknown command op: %s", op)
}

func (applier *CommandDBApplier) asyncSnapshot(value []byte) interface{} {
	err := orcraft.AsyncSnapshot()
	return err
}

func (applier *CommandDBApplier) registerNode(value []byte) interface{} {
	return nil
}

func (applier *CommandDBApplier) discover(value []byte) interface{} {
	instanceKey := dtstruct.InstanceKey{}
	if err := json.Unmarshal(value, &instanceKey); err != nil {
		return log.Errore(err)
	}
	discover.DiscoverInstance(context.TODO(), instanceKey)
	return nil
}

func (applier *CommandDBApplier) injectedPseudoGTID(value []byte) interface{} {
	var clusterName string
	if err := json.Unmarshal(value, &clusterName); err != nil {
		return log.Errore(err)
	}
	//core.RegisterInjectedPseudoGTID(clusterName)
	return nil
}

func (applier *CommandDBApplier) forget(value []byte) interface{} {
	instanceKey := dtstruct.InstanceKey{}
	if err := json.Unmarshal(value, &instanceKey); err != nil {
		return log.Errore(err)
	}
	err := instance.ForgetInstance(&instanceKey)
	return err
}

// TODO
func (applier *CommandDBApplier) forgetCluster(value []byte) interface{} {
	var clusterName string
	if err := json.Unmarshal(value, &clusterName); err != nil {
		return log.Errore(err)
	}
	err := instance.ForgetClusterInstance("", clusterName)
	return err
}

func (applier *CommandDBApplier) beginDowntime(value []byte) interface{} {
	downtime := dtstruct.Downtime{}
	if err := json.Unmarshal(value, &downtime); err != nil {
		return log.Errore(err)
	}
	err := base.BeginDowntime(&downtime)
	return err
}

func (applier *CommandDBApplier) endDowntime(value []byte) interface{} {
	instanceKey := dtstruct.InstanceKey{}
	if err := json.Unmarshal(value, &instanceKey); err != nil {
		return log.Errore(err)
	}
	_, err := base.EndDowntime(&instanceKey)
	return err
}

func (applier *CommandDBApplier) registerCandidate(value []byte) interface{} {
	candidate := dtstruct.CandidateDatabaseInstance{}
	if err := json.Unmarshal(value, &candidate); err != nil {
		return log.Errore(err)
	}
	err := base.RegisterCandidateInstance(&candidate)
	return err
}

func (applier *CommandDBApplier) ackRecovery(value []byte) interface{} {
	ack := dtstruct.RecoveryAcknowledgement{}
	err := json.Unmarshal(value, &ack)
	if err != nil {
		return log.Errore(err)
	}
	if ack.AllRecoveries {
		_, err = base.AcknowledgeAllRecoveries(ack.Owner, ack.Comment)
	}
	if ack.ClusterName != "" {
		_, err = base.AcknowledgeClusterRecoveries(ack.ClusterName, ack.Owner, ack.Comment)
	}
	if ack.Key.IsValid() {
		_, err = base.AcknowledgeInstanceRecoveries(&ack.Key, ack.Owner, ack.Comment)
	}
	if ack.Id > 0 {
		_, err = base.AcknowledgeRecovery(ack.Id, ack.Owner, ack.Comment)
	}
	if ack.UID != "" {
		_, err = base.AcknowledgeRecoveryByUID(ack.UID, ack.Owner, ack.Comment)
	}
	return err
}

func (applier *CommandDBApplier) registerHostnameUnresolve(value []byte) interface{} {
	registration := dtstruct.HostnameRegistration{}
	if err := json.Unmarshal(value, &registration); err != nil {
		return log.Errore(err)
	}
	err := base.RegisterHostnameUnresolve(&registration)
	return err
}

func (applier *CommandDBApplier) submitPoolInstances(value []byte) interface{} {
	submission := dtstruct.PoolInstancesSubmission{}
	if err := json.Unmarshal(value, &submission); err != nil {
		return log.Errore(err)
	}
	err := base.ApplyPoolInstance(&submission)
	return err
}

func (applier *CommandDBApplier) registerFailureDetection(value []byte) interface{} {
	analysisEntry := dtstruct.ReplicationAnalysis{}
	if err := json.Unmarshal(value, &analysisEntry); err != nil {
		return log.Errore(err)
	}
	_, err := base.AttemptFailureDetectionRegistration(&analysisEntry)
	return err
}

func (applier *CommandDBApplier) writeRecovery(value []byte) interface{} {
	topologyRecovery := dtstruct.TopologyRecovery{}
	if err := json.Unmarshal(value, &topologyRecovery); err != nil {
		return log.Errore(err)
	}
	if _, err := base.WriteTopologyRecovery(&topologyRecovery); err != nil {
		return err
	}
	return nil
}

func (applier *CommandDBApplier) writeRecoveryStep(value []byte) interface{} {
	topologyRecoveryStep := dtstruct.TopologyRecoveryStep{}
	if err := json.Unmarshal(value, &topologyRecoveryStep); err != nil {
		return log.Errore(err)
	}
	err := base.WriteTopologyRecoveryStep(&topologyRecoveryStep)
	return err
}

func (applier *CommandDBApplier) resolveRecovery(value []byte) interface{} {
	topologyRecovery := dtstruct.TopologyRecovery{}
	if err := json.Unmarshal(value, &topologyRecovery); err != nil {
		return log.Errore(err)
	}
	if err := base.WriteResolveRecovery(&topologyRecovery); err != nil {
		return log.Errore(err)
	}
	return nil
}

func (applier *CommandDBApplier) disableGlobalRecoveries(value []byte) interface{} {
	err := base.DisableRecovery()
	return err
}

func (applier *CommandDBApplier) enableGlobalRecoveries(value []byte) interface{} {
	err := base.EnableRecovery()
	return err
}

func (applier *CommandDBApplier) putKeyValue(value []byte) interface{} {
	kvPair := &dtstruct.KVPair{}
	if err := json.Unmarshal(value, kvPair); err != nil {
		return log.Errore(err)
	}
	err := kv.PutKVPairs([]*dtstruct.KVPair{kvPair})
	return err
}

func (applier *CommandDBApplier) putInstanceTag(value []byte) interface{} {
	instanceTag := dtstruct.InstanceTag{}
	if err := json.Unmarshal(value, &instanceTag); err != nil {
		return log.Errore(err)
	}
	err := base.PutInstanceTag(&instanceTag.Key, &instanceTag.T)
	return err
}

func (applier *CommandDBApplier) deleteInstanceTag(value []byte) interface{} {
	instanceTag := dtstruct.InstanceTag{}
	if err := json.Unmarshal(value, &instanceTag); err != nil {
		return log.Errore(err)
	}
	_, err := base.Untag(&instanceTag.Key, &instanceTag.T)
	return err
}

func (applier *CommandDBApplier) leaderURI(value []byte) interface{} {
	var uri string
	if err := json.Unmarshal(value, &uri); err != nil {
		return log.Errore(err)
	}
	orcraft.LeaderURI.Set(uri)
	return nil
}

func (applier *CommandDBApplier) healthReport(value []byte) interface{} {
	var authenticationToken string
	if err := json.Unmarshal(value, &authenticationToken); err != nil {
		return log.Errore(err)
	}
	orcraft.ReportToRaftLeader(authenticationToken)
	return nil
}

func (applier *CommandDBApplier) setClusterAliasManualOverride(value []byte) interface{} {
	var params [2]string
	if err := json.Unmarshal(value, &params); err != nil {
		return log.Errore(err)
	}
	clusterName, alias := params[0], params[1]
	err := base.WriteClusterAliasManualOverride(clusterName, alias)
	return err
}
