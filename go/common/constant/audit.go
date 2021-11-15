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
package constant

const (
	AuditDowntimeEnd          = "downtime-end"
	AuditDowntimeExpire       = "downtime-expire"
	AuditReviewUnSeenInstance = "review-unseen-instance"
	AuditUpdateClusterName    = "update-cluster-name"
	AuditInjectUnSeenMaster   = "inject-unseen-master"
	AuditInjectSeed           = "inject-seed"
	AuditForget               = "forget"
	AuditRegisterCandidate    = "register-candidate"

	AuditReplicaStart   = "start-replica"
	AuditReplicaRestart = "restart-replica"
	AuditReplicaStop    = "stop-replica"

	AuditRecoverDeadMasterWithoutReplica = "recover-dead-master-without-replicas"
)
