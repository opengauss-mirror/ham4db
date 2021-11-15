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
var interestingAnalysis = {
	"DeadMaster" : true,
	"DeadMasterAndReplicas" : true,
	"DeadMasterAndSomeReplicas" : true,
	"DeadMasterWithoutReplicas" : true,
	"UnreachableMasterWithLaggingReplicas": true,
	"UnreachableMaster" : true,
	"LockedSemiSyncMaster" : true,
	"AllMasterReplicasNotReplicating" : true,
	"AllMasterReplicasNotReplicatingOrDead" : true,
	"DeadCoMaster" : true,
	"DeadCoMasterAndSomeReplicas" : true,
	"DeadIntermediateMaster" : true,
	"DeadIntermediateMasterWithSingleReplicaFailingToConnect" : true,
	"DeadIntermediateMasterWithSingleReplica" : true,
	"DeadIntermediateMasterAndSomeReplicas" : true,
	"DeadIntermediateMasterAndReplicas" : true,
	"AllIntermediateMasterReplicasFailingToConnectOrDead" : true,
	"AllIntermediateMasterReplicasNotReplicating" : true,
	"UnreachableIntermediateMasterWithLaggingReplicas": true,
	"UnreachableIntermediateMaster" : true,
	"BinlogServerFailingToConnectToMaster" : true,
};

var errorMapping = {
	"in_maintenance": {
		"badge": "label-info",
		"description": "In maintenance"
	},
	"last_check_invalid": {
		"badge": "label-fatal",
		"description": "Last check invalid"
	},
	"not_recently_checked": {
		"badge": "label-stale",
		"description": "Not recently checked (stale)"
	},
	"not_replicating": {
		"badge": "label-danger",
		"description": "Not replicating"
	},
	"replication_lag": {
		"badge": "label-warning",
		"description": "Replication lag"
	},
	"errant_gtid": {
		"badge": "label-errant",
		"description": "Errant GTID"
	},
	"group_replication_member_not_online": {
		"badge": "label-danger",
		"description": "Replication group member is not ONLINE"
	}
};
