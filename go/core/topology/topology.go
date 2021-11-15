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

package topology

import (
	"gitee.com/opengauss/ham4db/go/dtstruct"
)

//  if we have a database topology like this:
//      C -->  A  <-- B
//		^             ^
//      |			  |
//	    C1			  B1
// A: cluster upstream;
// B: A's downstream, B1's upstream; B1: B's downstream;
// C: A's downstream, C1's upstream; C1: C's downstream;

// Topology returns a string representation of the topology of given cluster.
func Topology(request *dtstruct.Request, historyTimestampPattern string, tabulated bool, printTag bool) (result interface{}, err error) {
	return dtstruct.GetHamHandler(request.DBType).Topology(request, historyTimestampPattern, tabulated, printTag)
}

// MoveUp will attempt moving instance indicated by instanceKey up the topology hierarchy.
// It will perform all safety and sanity checks and will tamper with this instance's replication
// as well as its master.
// e.g. if move up C1, will get new topology like this:
//      C -->  A  <-- B                C -->  A  <-- B
//		^             ^		----->            ^      ^
//      |			  |                       |	     |
//	    C1			  B1	                 C1	    B1
func MoveUp(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).MoveUp(instanceKey)
}

// MoveEquivalent will attempt moving instance indicated by instanceKey below another instance,
// based on known master coordinates equivalence.
// e.g. if move equivalent C1 to B, will get new topology like this:
//      C -->  A  <-- B               C -->  A  <-- B <-- C1
//		^             ^		----->           		^
//      |			  |               	            |
//	    C1			  B1	           	           B1
func MoveEquivalent(instanceKey, anotherKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).MoveEquivalent(instanceKey, anotherKey)
}

// MoveBelow will attempt moving instance indicated by instanceKey below its supposed sibling indicated by siblingKey.
// It will perform all safety and sanity checks and will tamper with this instance's replication
// as well as its sibling.
// e.g. if move below B1 to C1, will get new topology like this:
//      C -->  A  <-- B <-- C1                C -->  A  <-- B <-- C1 <-- B1
//		              ^            ----->
//       			  |
//	     			  B1
func MoveBelow(instanceKey, siblingKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).MoveBelow(instanceKey, siblingKey)
}

// DetachMaster detaches a replica from its master by corrupting the upstream host (in such way that is reversible).
func DetachMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).DetachMaster(instanceKey)
}

// ReattachMaster reattaches a replica back onto its master by undoing a DetachMaster operation.
func ReattachMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).ReattachMaster(instanceKey)
}

// ChangeMasterTo changes the given instance's master according to given input.
func ChangeMasterTo(instanceKey *dtstruct.InstanceKey, masterKey *dtstruct.InstanceKey, masterBinlogCoordinates *dtstruct.LogCoordinates, skipUnresolve bool, gtidHint string) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).ChangeMasterTo(instanceKey, masterKey, masterBinlogCoordinates, skipUnresolve, gtidHint)
}

// MakeCoMaster will attempt to make an instance co-master with its master, by making its master a replica of its own.
// This only works out if the master is not replicating; the master does not have a known master (it may have an unknown master).
func MakeCoMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).MakeCoMaster(instanceKey)
}

// MakeMaster will take an instance, make all its siblings its replicas and make it master
// (stop its replication, make writeable).
func MakeMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).MakeMaster(instanceKey)
}

// TakeMaster will move an instance up the chain and cause its master to become its replica.
// It's almost a role change, just that other replicas of either 'instance' or its master are currently unaffected
// (they continue replicate without change)
// Note that the master must itself be a replica; however the grandparent does not necessarily have to be reachable
// and can in fact be dead.
func TakeMaster(instanceKey *dtstruct.InstanceKey, allowTakingCoMaster bool) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).TakeMaster(instanceKey, allowTakingCoMaster)
}

// MakeLocalMaster promotes a replica above its master, making it replica of its grandparent, while also enslaving its siblings.
// This serves as a convenience method to recover replication when a local master fails; the instance promoted is one of its replicas,
// which is most advanced among its siblings.
func MakeLocalMaster(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).MakeLocalMaster(instanceKey)
}

// MoveUpReplicas will attempt moving up all replicas of a given instance, at the same time.
// However this means all replicas of the given instance, and the instance itself, will all stop replicating together.
func MoveUpReplicas(instanceKey *dtstruct.InstanceKey, pattern string) ([]dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error, []error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).MoveUpReplicas(instanceKey, pattern)
}

// MatchUp will move a replica up the replication chain, so that it becomes sibling of its master.
func MatchUp(instanceKey *dtstruct.InstanceKey, requireInstanceMaintenance bool) (dtstruct.InstanceAdaptor, *dtstruct.LogCoordinates, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).MatchUp(instanceKey, requireInstanceMaintenance)
}

// MatchBelow will attempt moving instance indicated by instanceKey below its the one indicated by otherKey.
// The refactoring is based on matching binlog entries, not on "classic" positions comparisons.
// The "other instance" could be the sibling of the moving instance any of its ancestors. It may actually be
// a cousin of some sort (though unlikely). The only important thing is that the "other instance" is more
// advanced in replication than given instance.
func MatchBelow(instanceKey, otherKey *dtstruct.InstanceKey, requireInstanceMaintenance bool) (dtstruct.InstanceAdaptor, *dtstruct.LogCoordinates, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).MatchBelow(instanceKey, otherKey, requireInstanceMaintenance)
}

// RelocateBelow will try and figure out the best way to move instance indicated by instanceKey below another instance.
func RelocateBelow(instanceKey, otherKey *dtstruct.InstanceKey) (interface{}, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).RelocateBelow(instanceKey, otherKey)
}

// RelocateReplicas will try and figure out the best way to move replicas of an instance indicated by instanceKey below another instance.
func RelocateReplicas(instanceKey, otherKey *dtstruct.InstanceKey, pattern string) (replicas []dtstruct.InstanceAdaptor, other dtstruct.InstanceAdaptor, err error, errs []error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).RelocateReplicas(instanceKey, otherKey, pattern)
}

// MultiMatchReplicas will match all replicas of given master below given instance.
func MultiMatchReplicas(masterKey *dtstruct.InstanceKey, belowKey *dtstruct.InstanceKey, pattern string) ([]dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error, []error) {
	return dtstruct.GetHamHandler(masterKey.DBType).MultiMatchReplicas(masterKey, belowKey, pattern)
}

// MatchUpReplicas will move all replicas of given master up the replication chain, so that they become siblings of their master.
// This should be called when the local master dies.
func MatchUpReplicas(masterKey *dtstruct.InstanceKey, pattern string) ([]dtstruct.InstanceAdaptor, dtstruct.InstanceAdaptor, error, []error) {
	return dtstruct.GetHamHandler(masterKey.DBType).MatchUpReplicas(masterKey, pattern)
}

// MultiMatchBelow will efficiently match multiple replicas below a given instance. It is assumed that all given replicas are siblings.
func MultiMatchBelow(replicas []dtstruct.InstanceAdaptor, belowKey *dtstruct.InstanceKey, postponedFunctionsContainer *dtstruct.PostponedFunctionsContainer) (matchedReplicas []dtstruct.InstanceAdaptor, belowInstance dtstruct.InstanceAdaptor, err error, errs []error) {
	if len(replicas) == 0 {
		return
	}
	return dtstruct.GetHamHandler(replicas[0].GetInstance().Key.DBType).MultiMatchBelow(replicas, belowKey, postponedFunctionsContainer)
}

// RematchReplica will re-match a replica to its master.
func RematchReplica(instanceKey *dtstruct.InstanceKey, requireInstanceMaintenance bool) (dtstruct.InstanceAdaptor, *dtstruct.LogCoordinates, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).RematchReplica(instanceKey, requireInstanceMaintenance)
}

// TakeSiblings is a convenience method for turning siblings of a replica to be its subordinates.
// This operation is a syntactic sugar on top relocate-replicas.
func TakeSiblings(instanceKey *dtstruct.InstanceKey) (instance dtstruct.InstanceAdaptor, takenSiblings int, err error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).TakeSiblings(instanceKey)
}

// Repoint connects a replica to a master using its exact same executing coordinates.
// The given masterKey can be null, in which case the existing master is used.
func Repoint(instanceKey *dtstruct.InstanceKey, masterKey *dtstruct.InstanceKey, gtidHint string) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).Repoint(instanceKey, masterKey, gtidHint)
}

// RegroupReplicas is a "smart" method of promoting one replica over the others ("promoting" it on top of its siblings).
func RegroupReplicas(masterKey *dtstruct.InstanceKey, returnReplicaEvenOnFailureToRegroup bool, onCandidateReplicaChosen func(dtstruct.InstanceAdaptor), postponedFunctionsContainer *dtstruct.PostponedFunctionsContainer) (aheadReplicas []dtstruct.InstanceAdaptor, equalReplicas []dtstruct.InstanceAdaptor, laterReplicas []dtstruct.InstanceAdaptor, cannotReplicateReplicas []dtstruct.InstanceAdaptor, instance dtstruct.InstanceAdaptor, err error) {
	return dtstruct.GetHamHandler(masterKey.DBType).RegroupReplicas(masterKey, returnReplicaEvenOnFailureToRegroup, onCandidateReplicaChosen, postponedFunctionsContainer)
}

// RepointReplicasTo repoints replicas of a given instance (possibly filtered) onto another master.
func RepointReplicasTo(instanceKey *dtstruct.InstanceKey, pattern string, belowKey *dtstruct.InstanceKey) ([]dtstruct.InstanceAdaptor, error, []error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).RepointReplicasTo(instanceKey, pattern, belowKey)
}

// GetCandidateReplica chooses the best replica to promote given a (possibly dead) master.
func GetCandidateReplica(masterKey *dtstruct.InstanceKey, forRematchPurposes bool) (dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, []dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(masterKey.DBType).GetCandidateReplica(masterKey, forRematchPurposes)
}
