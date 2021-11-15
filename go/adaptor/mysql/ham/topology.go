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
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/common/constant"
	mdtstruct "gitee.com/opengauss/ham4db/go/adaptor/mysql/dtstruct"
	mutil "gitee.com/opengauss/ham4db/go/adaptor/mysql/util"
	gconstant "gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/limiter"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/core/security/ssl"
	"gitee.com/opengauss/ham4db/go/util/math"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	gutil "gitee.com/opengauss/ham4db/go/util/text"
	"github.com/go-sql-driver/mysql"
	"github.com/rcrowley/go-metrics"

	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/system/osp"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util"
	"github.com/patrickmn/go-cache"
	goos "os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const maxEmptyBinlogFiles int = 10
const maxEventInfoDisplayLength int = 200

var supportedAutoPseudoGTIDWriters = cache.New(config.CheckAutoPseudoGTIDGrantsIntervalSeconds*time.Second, time.Second)
var instanceBinlogEntryCache = cache.New(time.Duration(10)*time.Minute, time.Minute)
var asciiFillerCharacter = " "
var tabulatorScharacter = "|"
var readInstanceTLSCacheCounter = metrics.NewCounter()
var writeInstanceTLSCacheCounter = metrics.NewCounter()
var writeInstanceTLSCounter = metrics.NewCounter()

// Track if a TLS has already been configured for topology
var topologyTLSConfigured bool = false
var requireTLSCache *cache.Cache = cache.New(time.Duration(config.Config.TLSCacheTTLFactor*config.Config.InstancePollSeconds)*time.Second, time.Second)

func init() {
	metrics.Register("instance_tls.read_cache", readInstanceTLSCacheCounter)
	metrics.Register("instance_tls.write_cache", writeInstanceTLSCacheCounter)
	metrics.Register("instance_tls.write", writeInstanceTLSCounter)
}

// MoveUp will attempt moving instance indicated by instanceKey up the topology hierarchy.
// It will perform all safety and sanity checks and will tamper with this instance's replication
// as well as its master.
func MoveUp(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", instanceKey)
	}
	rinstance, _, _ := ReadFromBackendDB(&instance.Key)
	if canMove, merr := rinstance.CanMove(); !canMove {
		return instance, merr
	}
	master, err := GetInfoFromInstance(&instance.UpstreamKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errorf("Cannot GetInstanceMaster() for %+v. error=%+v", instance.Key, err)
	}

	if !master.IsReplica() {
		return instance, fmt.Errorf("master is not a replica itself: %+v", master.Key)
	}

	if canReplicate, err := CanReplicateFrom(instance, master); canReplicate == false {
		return instance, err
	}
	if master.IsReplicaServer() {
		// Quick solution via binlog servers
		return Repoint(instanceKey, &master.UpstreamKey, constant.GTIDHintDeny)
	}

	log.Infof("Will move %+v up the topology", *instanceKey)

	if maintenanceToken, merr := base.BeginMaintenance(instanceKey, dtstruct.GetMaintenanceOwner(), "move up"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer base.EndMaintenance(maintenanceToken)
	}
	if maintenanceToken, merr := base.BeginMaintenance(&master.Key, dtstruct.GetMaintenanceOwner(), fmt.Sprintf("child %+v moves up", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", master.Key, merr)
		goto Cleanup
	} else {
		defer base.EndMaintenance(maintenanceToken)
	}

	if !rinstance.UsingMariaDBGTID {
		master, err = StopReplication(&master.Key)
		if err != nil {
			goto Cleanup
		}
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	if !rinstance.UsingMariaDBGTID {
		instance, err = StartReplicationUntilMasterCoordinates(instanceKey, &master.SelfBinlogCoordinates)
		if err != nil {
			goto Cleanup
		}
	}

	// We can skip hostname unresolve; we just copy+paste whatever our master thinks of its master.
	instance, err = ChangeMasterTo(instanceKey, &master.UpstreamKey, &master.ExecBinlogCoordinates, true, constant.GTIDHintDeny)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(context.TODO(), instanceKey)
	if !rinstance.UsingMariaDBGTID {
		master, _ = StartReplication(context.TODO(), &master.Key)
	}
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	base.AuditOperation("move-up", instanceKey, instance.ClusterName, fmt.Sprintf("moved up %+v. Previous master: %+v", *instanceKey, master.Key))

	return instance, err
}

// RepointTo repoints list of replicas onto another master.
// Binlog Server is the major use case
func RepointTo(replicas []*mdtstruct.MysqlInstance, belowKey *dtstruct.InstanceKey) ([]*mdtstruct.MysqlInstance, error, []error) {
	res := []*mdtstruct.MysqlInstance{}
	errs := []error{}

	replicas = mutil.RemoveInstance(replicas, belowKey)
	if len(replicas) == 0 {
		// Nothing to do
		return res, nil, errs
	}
	if belowKey == nil {
		return res, log.Errorf("RepointTo received nil belowKey"), errs
	}

	log.Infof("Will repoint %+v replicas below %+v", len(replicas), *belowKey)
	barrier := make(chan *dtstruct.InstanceKey)
	replicaMutex := make(chan bool, 1)
	for _, replica := range replicas {
		replica := replica

		// Parallelize repoints
		go func() {
			defer func() { barrier <- &replica.Key }()
			limiter.ExecuteOnTopology(func() {
				replica, replicaErr := Repoint(&replica.Key, belowKey, constant.GTIDHintNeutral)

				func() {
					// Instantaneous mutex.
					replicaMutex <- true
					defer func() { <-replicaMutex }()
					if replicaErr == nil {
						res = append(res, replica)
					} else {
						errs = append(errs, replicaErr)
					}
				}()
			})
		}()
	}
	for range replicas {
		<-barrier
	}

	if len(errs) == len(replicas) {
		// All returned with error
		return res, log.Errorf("Error on all operations"), errs
	}
	base.AuditOperation("repoint-to", belowKey, "", fmt.Sprintf("repointed %d/%d replicas to %+v", len(res), len(replicas), *belowKey))

	return res, nil, errs
}
func GTIDSubtract(instanceKey *dtstruct.InstanceKey, gtidSet string, gtidSubset string) (gtidSubtract string, err error) {
	db, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return gtidSubtract, err
	}
	err = db.QueryRow("select gtid_subtract(?, ?)", gtidSet, gtidSubset).Scan(&gtidSubtract)
	return gtidSubtract, err
}

// canInjectPseudoGTID checks ham4db's grants to determine whether is has the
// privilege of auto-injecting pseudo-GTID
func canInjectPseudoGTID(instanceKey *dtstruct.InstanceKey) (canInject bool, err error) {
	if canInject, found := supportedAutoPseudoGTIDWriters.Get(instanceKey.StringCode()); found {
		return canInject.(bool), nil
	}
	db, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return canInject, err
	}

	foundAll := false
	foundDropOnAll := false
	foundAllOnSchema := false
	foundDropOnSchema := false

	err = sqlutil.QueryRowsMap(db, `show grants for current_user()`, func(m sqlutil.RowMap) error {
		for _, grantData := range m {
			grant := grantData.String
			if strings.Contains(grant, `GRANT ALL PRIVILEGES ON *.*`) {
				foundAll = true
			}
			if strings.Contains(grant, `DROP`) && strings.Contains(grant, ` ON *.*`) {
				foundDropOnAll = true
			}
			if strings.Contains(grant, fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.*", config.PseudoGTIDSchema)) {
				foundAllOnSchema = true
			}
			if strings.Contains(grant, fmt.Sprintf(`GRANT ALL PRIVILEGES ON "%s".*`, config.PseudoGTIDSchema)) {
				foundAllOnSchema = true
			}
			if strings.Contains(grant, `DROP`) && strings.Contains(grant, fmt.Sprintf(" ON `%s`.*", config.PseudoGTIDSchema)) {
				foundDropOnSchema = true
			}
			if strings.Contains(grant, `DROP`) && strings.Contains(grant, fmt.Sprintf(` ON "%s".*`, config.PseudoGTIDSchema)) {
				foundDropOnSchema = true
			}
		}
		return nil
	})
	if err != nil {
		return canInject, err
	}

	canInject = foundAll || foundDropOnAll || foundAllOnSchema || foundDropOnSchema
	supportedAutoPseudoGTIDWriters.Set(instanceKey.StringCode(), canInject, cache.DefaultExpiration)

	return canInject, nil
}

// injectPseudoGTID injects a Pseudo-GTID statement on a writable instance
func injectPseudoGTID(instance *mdtstruct.MysqlInstance) (hint string, err error) {
	if *dtstruct.RuntimeCLIFlags.Noop {
		return hint, fmt.Errorf("noop: aborting inject-pseudo-gtid operation on %+v; signalling error but nothing went wrong.", instance.Key)
	}

	now := time.Now()
	randomHash := util.RandomHash()[0:16]
	hint = fmt.Sprintf("%.8x:%.8x:%s", now.Unix(), instance.InstanceId, randomHash)
	query := fmt.Sprintf("drop view if exists `%s`.`_asc:%s`", config.PseudoGTIDSchema, hint)
	_, err = ExecSQLOnInstance(&instance.Key, query)
	return hint, log.Errore(err)
}

// RegroupReplicas is a "smart" method of promoting one replica over the others ("promoting" it on top of its siblings)
// This method decides which strategy to use: GTID, Pseudo-GTID, Binlog Servers.
func RegroupReplicas(masterKey *dtstruct.InstanceKey, returnReplicaEvenOnFailureToRegroup bool,
	onCandidateReplicaChosen func(handler dtstruct.InstanceAdaptor),
	postponedFunctionsContainer *dtstruct.PostponedFunctionsContainer) (

	aheadReplicas []*mdtstruct.MysqlInstance,
	equalReplicas []*mdtstruct.MysqlInstance,
	laterReplicas []*mdtstruct.MysqlInstance,
	cannotReplicateReplicas []*mdtstruct.MysqlInstance,
	instance *mdtstruct.MysqlInstance,
	err error,
) {
	//
	var emptyReplicas []*mdtstruct.MysqlInstance

	replicas, err := ReadReplicaInstances(masterKey)
	if err != nil {
		return emptyReplicas, emptyReplicas, emptyReplicas, emptyReplicas, instance, err
	}
	if len(replicas) == 0 {
		return emptyReplicas, emptyReplicas, emptyReplicas, emptyReplicas, instance, err
	}
	if len(replicas) == 1 {
		return emptyReplicas, emptyReplicas, emptyReplicas, emptyReplicas, replicas[0], err
	}
	allGTID := true
	allBinlogServers := true
	allPseudoGTID := true
	for _, replica := range replicas {
		if !replica.UsingGTID() {
			allGTID = false
		}
		if !replica.IsReplicaServer() {
			allBinlogServers = false
		}
		if !replica.UsingPseudoGTID {
			allPseudoGTID = false
		}
	}
	if allGTID {
		log.Debugf("RegroupReplicas: using GTID to regroup replicas of %+v", *masterKey)
		unmovedReplicas, movedReplicas, cannotReplicateReplicas, candidateReplica, err := RegroupReplicasGTID(masterKey, returnReplicaEvenOnFailureToRegroup, onCandidateReplicaChosen, nil, nil)
		return unmovedReplicas, emptyReplicas, movedReplicas, cannotReplicateReplicas, candidateReplica, err
	}
	if allBinlogServers {
		log.Debugf("RegroupReplicas: using binlog servers to regroup replicas of %+v", *masterKey)
		movedReplicas, candidateReplica, err := RegroupReplicasBinlogServers(masterKey, returnReplicaEvenOnFailureToRegroup)
		return emptyReplicas, emptyReplicas, movedReplicas, cannotReplicateReplicas, candidateReplica, err
	}
	if allPseudoGTID {
		log.Debugf("RegroupReplicas: using Pseudo-GTID to regroup replicas of %+v", *masterKey)
		return RegroupReplicasPseudoGTID(masterKey, returnReplicaEvenOnFailureToRegroup, onCandidateReplicaChosen, postponedFunctionsContainer, nil)
	}
	// And, as last resort, we do PseudoGTID & binlog servers
	log.Warningf("RegroupReplicas: unsure what method to invoke for %+v; trying Pseudo-GTID+Binlog Servers", *masterKey)
	return RegroupReplicasPseudoGTIDIncludingSubReplicasOfBinlogServers(masterKey, returnReplicaEvenOnFailureToRegroup, onCandidateReplicaChosen, postponedFunctionsContainer, nil)
}

// RegroupReplicasPseudoGTID will choose a candidate replica of a given instance, and take its siblings using pseudo-gtid
func RegroupReplicasPseudoGTID(
	masterKey *dtstruct.InstanceKey,
	returnReplicaEvenOnFailureToRegroup bool,
	onCandidateReplicaChosen func(handler dtstruct.InstanceAdaptor),
	postponedFunctionsContainer *dtstruct.PostponedFunctionsContainer,
	postponeAllMatchOperations func(dtstruct.InstanceAdaptor, bool) bool,
) (
	aheadReplicas []*mdtstruct.MysqlInstance,
	equalReplicas []*mdtstruct.MysqlInstance,
	laterReplicas []*mdtstruct.MysqlInstance,
	cannotReplicateReplicas []*mdtstruct.MysqlInstance,
	candidateReplica *mdtstruct.MysqlInstance,
	err error,
) {
	candidateReplica, aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, err = GetCandidateReplica(masterKey, true)
	if err != nil {
		if !returnReplicaEvenOnFailureToRegroup {
			candidateReplica = nil
		}
		return aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, candidateReplica, err
	}

	if config.Config.PseudoGTIDPattern == "" {
		return aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, candidateReplica, fmt.Errorf("PseudoGTIDPattern not configured; cannot use Pseudo-GTID")
	}

	if onCandidateReplicaChosen != nil {
		onCandidateReplicaChosen(candidateReplica)
	}

	allMatchingFunc := func() error {
		log.Debugf("RegroupReplicas: working on %d equals replicas", len(equalReplicas))
		barrier := make(chan *dtstruct.InstanceKey)
		for _, replica := range equalReplicas {
			replica := replica
			// This replica has the exact same executing coordinates as the candidate replica. This replica
			// is *extremely* easy to attach below the candidate replica!
			go func() {
				defer func() { barrier <- &candidateReplica.Key }()
				limiter.ExecuteOnTopology(func() {
					ChangeMasterTo(&replica.Key, &candidateReplica.Key, &candidateReplica.SelfBinlogCoordinates, false, constant.GTIDHintDeny)
				})
			}()
		}
		for range equalReplicas {
			<-barrier
		}

		log.Debugf("RegroupReplicas: multi matching %d later replicas", len(laterReplicas))
		// As for the laterReplicas, we'll have to apply pseudo GTID
		laterReplicas, candidateReplica, err, _ = MultiMatchBelow(laterReplicas, &candidateReplica.Key, postponedFunctionsContainer)

		operatedReplicas := append(equalReplicas, candidateReplica)
		operatedReplicas = append(operatedReplicas, laterReplicas...)
		log.Debugf("RegroupReplicas: starting %d replicas", len(operatedReplicas))
		barrier = make(chan *dtstruct.InstanceKey)
		for _, replica := range operatedReplicas {
			replica := replica
			go func() {
				defer func() { barrier <- &candidateReplica.Key }()
				limiter.ExecuteOnTopology(func() {
					StartReplication(context.TODO(), &replica.Key)
				})
			}()
		}
		for range operatedReplicas {
			<-barrier
		}

		clusterName, _ := base.GetClusterName(masterKey)
		base.AuditOperation("regroup-replicas", masterKey, clusterName, fmt.Sprintf("regrouped %+v replicas below %+v", len(operatedReplicas), *masterKey))
		return err
	}
	if postponedFunctionsContainer != nil && postponeAllMatchOperations != nil && postponeAllMatchOperations(candidateReplica, false) {
		postponedFunctionsContainer.AddPostponedFunction(allMatchingFunc, fmt.Sprintf("regroup-replicas-pseudo-gtid %+v", candidateReplica.Key))
	} else {
		err = allMatchingFunc()
	}
	log.Debugf("RegroupReplicas: done")
	// aheadReplicas are lost (they were ahead in replication as compared to promoted replica)
	return aheadReplicas, equalReplicas, laterReplicas, cannotReplicateReplicas, candidateReplica, err
}

// MoveEquivalent will attempt moving instance indicated by instanceKey below another instance,
// based on known master coordinates equivalence
func MoveEquivalent(instanceKey, otherKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	if instance.Key.Equals(otherKey) {
		return instance, fmt.Errorf("MoveEquivalent: attempt to move an instance below itself %+v", instance.Key)
	}

	// Are there equivalent coordinates to this instance?
	instanceCoordinates := &dtstruct.InstanceBinlogCoordinates{Key: instance.UpstreamKey, Coordinates: instance.ExecBinlogCoordinates}
	binlogCoordinates, err := GetEquivalentBinlogCoordinatesFor(instanceCoordinates, otherKey)
	if err != nil {
		return instance, err
	}
	if binlogCoordinates == nil {
		return instance, fmt.Errorf("No equivalent coordinates found for %+v replicating from %+v at %+v", instance.Key, instance.UpstreamKey, instance.ExecBinlogCoordinates)
	}
	// For performance reasons, we did all the above before even checking the replica is stopped or stopping it at all.
	// This allows us to quickly skip the entire operation should there NOT be coordinates.
	// To elaborate: if the replica is actually running AND making progress, it is unlikely/impossible for it to have
	// equivalent coordinates, as the current coordinates are like to have never been seen.
	// This excludes the case, for example, that the master is itself not replicating.
	// Now if we DO get to happen on equivalent coordinates, we need to double check. For CHANGE MASTER to happen we must
	// stop the replica anyhow. But then let's verify the position hasn't changed.
	knownExecBinlogCoordinates := instance.ExecBinlogCoordinates

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}
	if !instance.ExecBinlogCoordinates.Equals(&knownExecBinlogCoordinates) {
		// Seems like things were still running... We don't have an equivalence point
		err = fmt.Errorf("MoveEquivalent(): ExecBinlogCoordinates changed after stopping replication on %+v; aborting", instance.Key)
		goto Cleanup
	}
	instance, err = ChangeMasterTo(instanceKey, otherKey, binlogCoordinates, false, constant.GTIDHintNeutral)

Cleanup:
	instance, _ = StartReplication(context.TODO(), instanceKey)

	if err == nil {
		message := fmt.Sprintf("moved %+v via equivalence coordinates below %+v", *instanceKey, *otherKey)
		log.Debugf(message)
		base.AuditOperation("move-equivalent", instanceKey, instance.ClusterName, message)
	}
	return instance, err
}

// MoveUpReplicas will attempt moving up all replicas of a given instance, at the same time.
// Clock-time, this is fater than moving one at a time. However this means all replicas of the given instance, and the instance itself,
// will all stop replicating together.
func MoveUpReplicas(instanceKey *dtstruct.InstanceKey, pattern string) ([]*mdtstruct.MysqlInstance, *mdtstruct.MysqlInstance, error, []error) {
	res := []*mdtstruct.MysqlInstance{}
	errs := []error{}
	replicaMutex := make(chan bool, 1)
	var barrier chan *dtstruct.InstanceKey

	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return res, nil, err, errs
	}
	if !instance.IsReplica() {
		return res, instance, fmt.Errorf("instance is not a replica: %+v", instanceKey), errs
	}
	_, err = GetInfoFromInstance(&instance.UpstreamKey, false, false, nil, "")
	if err != nil {
		return res, instance, log.Errorf("Cannot GetInstanceMaster() for %+v. error=%+v", instance.Key, err), errs
	}

	if instance.IsReplicaServer() {
		replicas, err, errors := RepointReplicasTo(instanceKey, pattern, &instance.UpstreamKey)
		// Bail out!
		return replicas, instance, err, errors
	}

	replicas, err := ReadReplicaInstances(instanceKey)
	if err != nil {
		return res, instance, err, errs
	}
	replicas = mutil.FilterInstancesByPattern(replicas, pattern)
	if len(replicas) == 0 {
		return res, instance, nil, errs
	}
	log.Infof("Will move replicas of %+v up the topology", *instanceKey)

	if maintenanceToken, merr := base.BeginMaintenance(instanceKey, dtstruct.GetMaintenanceOwner(), "move up replicas"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer base.EndMaintenance(maintenanceToken)
	}
	for _, replica := range replicas {
		if maintenanceToken, merr := base.BeginMaintenance(&replica.Key, dtstruct.GetMaintenanceOwner(), fmt.Sprintf("%+v moves up", replica.Key)); merr != nil {
			err = fmt.Errorf("Cannot begin maintenance on %+v: %v", replica.Key, merr)
			goto Cleanup
		} else {
			defer base.EndMaintenance(maintenanceToken)
		}
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	barrier = make(chan *dtstruct.InstanceKey)
	for _, replica := range replicas {
		replica := replica
		go func() {
			defer func() {
				defer func() { barrier <- &replica.Key }()
				StartReplication(context.TODO(), &replica.Key)
			}()

			var replicaErr error
			limiter.ExecuteOnTopology(func() {
				if canReplicate, err := CanReplicateFrom(replica, instance); canReplicate == false || err != nil {
					replicaErr = err
					return
				}
				if instance.IsReplicaServer() {
					// Special case. Just repoint
					replica, err = Repoint(&replica.Key, instanceKey, constant.GTIDHintDeny)
					if err != nil {
						replicaErr = err
						return
					}
				} else {
					// Normal case. Do the math.
					replica, err = StopReplication(&replica.Key)
					if err != nil {
						replicaErr = err
						return
					}
					replica, err = StartReplicationUntilMasterCoordinates(&replica.Key, &instance.SelfBinlogCoordinates)
					if err != nil {
						replicaErr = err
						return
					}

					replica, err = ChangeMasterTo(&replica.Key, &instance.UpstreamKey, &instance.ExecBinlogCoordinates, false, constant.GTIDHintDeny)
					if err != nil {
						replicaErr = err
						return
					}
				}
			})

			func() {
				replicaMutex <- true
				defer func() { <-replicaMutex }()
				if replicaErr == nil {
					res = append(res, replica)
				} else {
					errs = append(errs, replicaErr)
				}
			}()
		}()
	}
	for range replicas {
		<-barrier
	}

Cleanup:
	instance, _ = StartReplication(context.TODO(), instanceKey)
	if err != nil {
		return res, instance, log.Errore(err), errs
	}
	if len(errs) == len(replicas) {
		// All returned with error
		return res, instance, log.Errorf("Error on all operations"), errs
	}
	base.AuditOperation("move-up-replicas", instanceKey, instance.ClusterName, fmt.Sprintf("moved up %d/%d replicas of %+v. New master: %+v", len(res), len(replicas), *instanceKey, instance.UpstreamKey))

	return res, instance, err, errs
}

// MoveBelow will attempt moving instance indicated by instanceKey below its supposed sibling indicated by sinblingKey.
// It will perform all safety and sanity checks and will tamper with this instance's replication
// as well as its sibling.
func MoveBelow(instanceKey, siblingKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {

	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	sibling, err := GetInfoFromInstance(siblingKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, log.Errorf("MoveBelow: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}

	if sibling.IsReplicaServer() {
		// Binlog server has same coordinates as master
		// Easy solution!
		return Repoint(instanceKey, &sibling.Key, constant.GTIDHintDeny)
	}

	rinstance, _, _ := ReadFromBackendDB(&instance.Key)
	if canMove, merr := rinstance.CanMove(); !canMove {
		return instance, merr
	}

	rinstance, _, _ = ReadFromBackendDB(&sibling.Key)
	if canMove, merr := rinstance.CanMove(); !canMove {
		return instance, merr
	}
	if !dtstruct.IsSibling(instance, sibling) {
		return instance, fmt.Errorf("instances are not siblings: %+v, %+v", *instanceKey, *siblingKey)
	}

	if canReplicate, err := CanReplicateFrom(instance, sibling); !canReplicate {
		return instance, err
	}
	log.Infof("Will move %+v below %+v", instanceKey, siblingKey)

	if maintenanceToken, merr := base.BeginMaintenance(instanceKey, dtstruct.GetMaintenanceOwner(), fmt.Sprintf("move below %+v", *siblingKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer base.EndMaintenance(maintenanceToken)
	}
	if maintenanceToken, merr := base.BeginMaintenance(siblingKey, dtstruct.GetMaintenanceOwner(), fmt.Sprintf("%+v moves below this", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *siblingKey, merr)
		goto Cleanup
	} else {
		defer base.EndMaintenance(maintenanceToken)
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	sibling, err = StopReplication(siblingKey)
	if err != nil {
		goto Cleanup
	}
	if instance.ExecBinlogCoordinates.SmallerThan(&sibling.ExecBinlogCoordinates) {
		instance, err = StartReplicationUntilMasterCoordinates(instanceKey, &sibling.ExecBinlogCoordinates)
		if err != nil {
			goto Cleanup
		}
	} else if sibling.ExecBinlogCoordinates.SmallerThan(&instance.ExecBinlogCoordinates) {
		sibling, err = StartReplicationUntilMasterCoordinates(siblingKey, &instance.ExecBinlogCoordinates)
		if err != nil {
			goto Cleanup
		}
	}
	// At this point both siblings have executed exact same statements and are identical

	instance, err = ChangeMasterTo(instanceKey, &sibling.Key, &sibling.SelfBinlogCoordinates, false, constant.GTIDHintDeny)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(context.TODO(), instanceKey)
	sibling, _ = StartReplication(context.TODO(), siblingKey)

	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	base.AuditOperation("move-below", instanceKey, instance.ClusterName, fmt.Sprintf("moved %+v below %+v", *instanceKey, *siblingKey))

	return instance, err
}
func canReplicateAssumingOracleGTID(instance, masterInstance *mdtstruct.MysqlInstance) (canReplicate bool, err error) {
	subtract, err := GTIDSubtract(&instance.Key, masterInstance.GtidPurged, instance.ExecutedGtidSet)
	if err != nil {
		return false, err
	}
	subtractGtidSet, err := NewOracleGtidSet(subtract)
	if err != nil {
		return false, err
	}
	return subtractGtidSet.IsEmpty(), nil
}

func instancesAreGTIDAndCompatible(instance, otherInstance *mdtstruct.MysqlInstance) (isOracleGTID bool, isMariaDBGTID, compatible bool) {
	isOracleGTID = (instance.UsingOracleGTID && otherInstance.SupportsOracleGTID)
	isMariaDBGTID = (instance.UsingMariaDBGTID && otherInstance.IsMariaDB())
	compatible = isOracleGTID || isMariaDBGTID
	return isOracleGTID, isMariaDBGTID, compatible
}

func CheckMoveViaGTID(instance, otherInstance *mdtstruct.MysqlInstance) (err error) {
	isOracleGTID, _, moveCompatible := instancesAreGTIDAndCompatible(instance, otherInstance)
	if !moveCompatible {
		return fmt.Errorf("Instances %+v, %+v not GTID compatible or not using GTID", instance.Key, otherInstance.Key)
	}
	if isOracleGTID {
		canReplicate, err := canReplicateAssumingOracleGTID(instance, otherInstance)
		if err != nil {
			return err
		}
		if !canReplicate {
			return fmt.Errorf("Instance %+v has purged GTID entries not found on %+v", otherInstance.Key, instance.Key)
		}
	}

	return nil
}

// moveInstanceBelowViaGTID will attempt moving given instance below another instance using either Oracle GTID or MariaDB GTID.
func moveInstanceBelowViaGTID(instance, otherInstance *mdtstruct.MysqlInstance) (*mdtstruct.MysqlInstance, error) {
	rinstance, _, _ := ReadFromBackendDB(&instance.Key)
	if canMove, merr := rinstance.CanMoveViaMatch(); !canMove {
		return instance, merr
	}

	if canReplicate, err := CanReplicateFrom(instance, otherInstance); !canReplicate {
		return instance, err
	}
	if err := CheckMoveViaGTID(instance, otherInstance); err != nil {
		return instance, err
	}
	log.Infof("Will move %+v below %+v via GTID", instance.Key, otherInstance.Key)

	instanceKey := &instance.Key
	otherInstanceKey := &otherInstance.Key

	var err error
	if maintenanceToken, merr := base.BeginMaintenance(instanceKey, dtstruct.GetMaintenanceOwner(), fmt.Sprintf("move below %+v", *otherInstanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer base.EndMaintenance(maintenanceToken)
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	instance, err = ChangeMasterTo(instanceKey, &otherInstance.Key, &otherInstance.SelfBinlogCoordinates, false, constant.GTIDHintForce)
	if err != nil {
		goto Cleanup
	}
Cleanup:
	instance, _ = StartReplication(context.TODO(), instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	base.AuditOperation("move-below-gtid", instanceKey, instance.ClusterName, fmt.Sprintf("moved %+v below %+v", *instanceKey, *otherInstanceKey))

	return instance, err
}

// MoveBelowGTID will attempt moving instance indicated by instanceKey below another instance using either Oracle GTID or MariaDB GTID.
func MoveBelowGTID(instanceKey, otherKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	other, err := GetInfoFromInstance(otherKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, log.Errorf("MoveBelowGTID: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}
	return moveInstanceBelowViaGTID(instance, other)
}

// moveReplicasViaGTID moves a list of replicas under another instance via GTID, returning those replicas
// that could not be moved (do not use GTID or had GTID errors)
func moveReplicasViaGTID(replicas []*mdtstruct.MysqlInstance, other *mdtstruct.MysqlInstance, postponedFunctionsContainer *dtstruct.PostponedFunctionsContainer) (movedReplicas []*mdtstruct.MysqlInstance, unmovedReplicas []*mdtstruct.MysqlInstance, err error, errs []error) {
	replicas = mutil.RemoveNilInstances(replicas)
	replicas = mutil.RemoveInstance(replicas, &other.Key)
	if len(replicas) == 0 {
		// Nothing to do
		return movedReplicas, unmovedReplicas, nil, errs
	}

	log.Infof("moveReplicasViaGTID: Will move %+v replicas below %+v via GTID, max concurrency: %v",
		len(replicas),
		other.Key,
		config.Config.MaxConcurrentReplicaOperations)

	var waitGroup sync.WaitGroup
	var replicaMutex sync.Mutex

	var concurrencyChan = make(chan bool, config.Config.MaxConcurrentReplicaOperations)

	for _, replica := range replicas {
		replica := replica

		waitGroup.Add(1)
		// Parallelize repoints
		go func() {
			defer waitGroup.Done()
			moveFunc := func() error {

				concurrencyChan <- true
				defer func() { recover(); <-concurrencyChan }()

				movedReplica, replicaErr := moveInstanceBelowViaGTID(replica, other)
				if replicaErr != nil && movedReplica != nil {
					replica = movedReplica
				}

				// After having moved replicas, update local shared variables:
				replicaMutex.Lock()
				defer replicaMutex.Unlock()

				if replicaErr == nil {
					movedReplicas = append(movedReplicas, replica)
				} else {
					unmovedReplicas = append(unmovedReplicas, replica)
					errs = append(errs, replicaErr)
				}
				return replicaErr
			}
			if ShouldPostponeRelocatingReplica(replica, postponedFunctionsContainer) {
				postponedFunctionsContainer.AddPostponedFunction(moveFunc, fmt.Sprintf("move-replicas-gtid %+v", replica.Key))
				// We bail out and trust our invoker to later call upon this postponed function
			} else {
				limiter.ExecuteOnTopology(func() { moveFunc() })
			}
		}()
	}
	waitGroup.Wait()

	if len(errs) == len(replicas) {
		// All returned with error
		return movedReplicas, unmovedReplicas, fmt.Errorf("moveReplicasViaGTID: Error on all %+v operations", len(errs)), errs
	}
	base.AuditOperation("move-replicas-gtid", &other.Key, other.ClusterName, fmt.Sprintf("moved %d/%d replicas below %+v via GTID", len(movedReplicas), len(replicas), other.Key))

	return movedReplicas, unmovedReplicas, err, errs
}

// MoveReplicasGTID will (attempt to) move all replicas of given master below given instance.
func MoveReplicasGTID(masterKey *dtstruct.InstanceKey, belowKey *dtstruct.InstanceKey, pattern string) (movedReplicas [](*mdtstruct.MysqlInstance), unmovedReplicas [](*mdtstruct.MysqlInstance), err error, errs []error) {
	belowInstance, err := GetInfoFromInstance(belowKey, false, false, nil, "")
	if err != nil {
		// Can't access "below" ==> can't move replicas beneath it
		return movedReplicas, unmovedReplicas, err, errs
	}

	// replicas involved
	replicas, err := ReadReplicaInstancesIncludingBinlogServerSubReplicas(masterKey)
	if err != nil {
		return movedReplicas, unmovedReplicas, err, errs
	}
	replicas = mutil.FilterInstancesByPattern(replicas, pattern)
	movedReplicas, unmovedReplicas, err, errs = moveReplicasViaGTID(replicas, belowInstance, nil)
	if err != nil {
		log.Errore(err)
	}

	if len(unmovedReplicas) > 0 {
		err = fmt.Errorf("MoveReplicasGTID: only moved %d out of %d replicas of %+v; error is: %+v", len(movedReplicas), len(replicas), *masterKey, err)
	}

	return movedReplicas, unmovedReplicas, err, errs
}

// RepointReplicasTo repoints replicas of a given instance (possibly filtered) onto another master.
// Binlog Server is the major use case
func RepointReplicasTo(instanceKey *dtstruct.InstanceKey, pattern string, belowKey *dtstruct.InstanceKey) ([]*mdtstruct.MysqlInstance, error, []error) {
	res := []*mdtstruct.MysqlInstance{}
	errs := []error{}

	replicas, err := ReadReplicaInstances(instanceKey)
	if err != nil {
		return res, err, errs
	}

	replicas = mutil.RemoveInstance(replicas, belowKey)
	replicas = mutil.FilterInstancesByPattern(replicas, pattern)
	if len(replicas) == 0 {
		// Nothing to do
		return res, nil, errs
	}
	if belowKey == nil {
		// Default to existing master. All replicas are of the same master, hence just pick one.
		belowKey = &replicas[0].UpstreamKey
	}
	log.Infof("Will repoint replicas of %+v to %+v", *instanceKey, *belowKey)
	return RepointTo(replicas, belowKey)
}

// RepointReplicas repoints all replicas of a given instance onto its existing master.
func RepointReplicas(instanceKey *dtstruct.InstanceKey, pattern string) ([]*mdtstruct.MysqlInstance, error, []error) {
	return RepointReplicasTo(instanceKey, pattern, nil)
}

// Repoint connects a replica to a master using its exact same executing coordinates.
// The given masterKey can be null, in which case the existing master is used.
// Two use cases:
// - masterKey is nil: use case is corrupted relay logs on replica
// - masterKey is not nil: using Binlog servers (coordinates remain the same)
func Repoint(instanceKey *dtstruct.InstanceKey, masterKey *dtstruct.InstanceKey, gtidHint constant.OperationGTIDHint) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", *instanceKey)
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, fmt.Errorf("repoint: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}
	if masterKey == nil {
		masterKey = &instance.UpstreamKey
	}
	// With repoint we *prefer* the master to be alive, but we don't strictly require it.
	// The use case for the master being alive is with hostname-resolve or hostname-unresolve: asking the replica
	// to reconnect to its same master while changing the MASTER_HOST in CHANGE MASTER TO due to DNS changes etc.
	master, _, err := ReadFromBackendDB(masterKey)
	masterIsAccessible := (err == nil)
	if !masterIsAccessible {
		master, _, err = ReadFromBackendDB(masterKey)
		if master == nil || err != nil {
			return instance, err
		}
	}
	if canReplicate, err := CanReplicateFrom(instance, master); !canReplicate {
		return instance, err
	}

	// if a binlog server check it is sufficiently up to date
	if master.IsReplicaServer() {
		// "Repoint" operation trusts the user. But only so much. Repoiting to a binlog server which is not yet there is strictly wrong.
		if !instance.ExecBinlogCoordinates.SmallerThanOrEquals(&master.SelfBinlogCoordinates) {
			return instance, fmt.Errorf("repoint: binlog server %+v is not sufficiently up to date to repoint %+v below it", *masterKey, *instanceKey)
		}
	}

	log.Infof("Will repoint %+v to master %+v", *instanceKey, *masterKey)

	if maintenanceToken, merr := base.BeginMaintenance(instanceKey, dtstruct.GetMaintenanceOwner(), "repoint"); merr != nil {
		err = fmt.Errorf("cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer base.EndMaintenance(maintenanceToken)
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	// See above, we are relaxed about the master being accessible/inaccessible.
	// If accessible, we wish to do hostname-unresolve. If inaccessible, we can skip the test and not fail the
	// ChangeMasterTo operation. This is why we pass "!masterIsAccessible" below.
	if instance.ExecBinlogCoordinates.IsEmpty() {
		instance.ExecBinlogCoordinates.LogFile = "ham4db-unknown-log-file"
	}
	instance, err = ChangeMasterTo(instanceKey, masterKey, &instance.ExecBinlogCoordinates, !masterIsAccessible, gtidHint)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(context.TODO(), instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	base.AuditOperation("repoint", instanceKey, "", fmt.Sprintf("replica %+v repointed to master: %+v", *instanceKey, *masterKey))

	return instance, err

}

// MakeCoMaster will attempt to make an instance co-master with its master, by making its master a replica of its own.
// This only works out if the master is not replicating; the master does not have a known master (it may have an unknown master).
func MakeCoMaster(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	if canMove, merr := instance.CanMove(); !canMove {
		return instance, merr
	}
	master, err := GetInfoFromInstance(&instance.UpstreamKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, fmt.Errorf("MakeCoMaster: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}
	log.Debugf("Will check whether %+v's master (%+v) can become its co-master", instance.Key, master.Key)
	if canMove, merr := master.CanMoveAsCoMaster(); !canMove {
		return instance, merr
	}
	if instanceKey.Equals(&master.UpstreamKey) {
		return instance, fmt.Errorf("instance %+v is already co master of %+v", instance.Key, master.Key)
	}
	if !instance.ReadOnly {
		return instance, fmt.Errorf("instance %+v is not read-only; first make it read-only before making it co-master", instance.Key)
	}
	if master.IsCoUpstream {
		// We allow breaking of an existing co-master replication. Here's the breakdown:
		// Ideally, this would not eb allowed, and we would first require the user to RESET SLAVE on 'master'
		// prior to making it participate as co-master with our 'instance'.
		// However there's the problem that upon RESET SLAVE we lose the replication's user/password info.
		// Thus, we come up with the following rule:
		// If S replicates from M1, and M1<->M2 are co masters, we allow S to become co-master of M1 (S<->M1) if:
		// - M1 is writeable
		// - M2 is read-only or is unreachable/invalid
		// - S  is read-only
		// And so we will be replacing one read-only co-master with another.
		otherCoMaster, found, _ := ReadFromBackendDB(&master.UpstreamKey)
		if found && otherCoMaster.IsLastCheckValid && !otherCoMaster.ReadOnly {
			return instance, fmt.Errorf("master %+v is already co-master with %+v, and %+v is alive, and not read-only; cowardly refusing to demote it. Please set it as read-only beforehand", master.Key, otherCoMaster.Key, otherCoMaster.Key)
		}
		// OK, good to go.
	} else if _, found, _ := ReadFromBackendDB(&master.UpstreamKey); found {
		return instance, fmt.Errorf("%+v is not a real master; it replicates from: %+v", master.Key, master.UpstreamKey)
	}
	if canReplicate, err := CanReplicateFrom(master, instance); !canReplicate {
		return instance, err
	}
	log.Infof("Will make %+v co-master of %+v", instanceKey, master.Key)

	var gitHint = constant.GTIDHintNeutral
	if maintenanceToken, merr := base.BeginMaintenance(instanceKey, dtstruct.GetMaintenanceOwner(), fmt.Sprintf("make co-master of %+v", master.Key)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer base.EndMaintenance(maintenanceToken)
	}
	if maintenanceToken, merr := base.BeginMaintenance(&master.Key, dtstruct.GetMaintenanceOwner(), fmt.Sprintf("%+v turns into co-master of this", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", master.Key, merr)
		goto Cleanup
	} else {
		defer base.EndMaintenance(maintenanceToken)
	}

	// the coMaster used to be merely a replica. Just point master into *some* position
	// within coMaster...
	if master.IsReplica() {
		// this is the case of a co-master. For masters, the StopReplication operation throws an error, and
		// there's really no point in doing it.
		master, err = StopReplication(&master.Key)
		if err != nil {
			goto Cleanup
		}
	}
	if !master.HasReplicationCredentials {
		// Let's try , if possible, to get credentials from replica. Best effort.
		if credentials, credentialsErr := ReadReplicationCredentials(&instance.Key); credentialsErr == nil {
			log.Debugf("Got credentials from a replica. will now apply")
			_, err = ChangeMasterCredentials(&master.Key, credentials)
			if err != nil {
				goto Cleanup
			}
		}
	}

	if instance.AllowTLS {
		log.Debugf("Enabling SSL replication")
		_, err = EnableMasterSSL(&master.Key)
		if err != nil {
			goto Cleanup
		}
	}

	if instance.UsingOracleGTID {
		gitHint = constant.GTIDHintForce
	}
	master, err = ChangeMasterTo(&master.Key, instanceKey, &instance.SelfBinlogCoordinates, false, constant.OperationGTIDHint(gitHint))
	if err != nil {
		goto Cleanup
	}

Cleanup:
	master, _ = StartReplication(context.TODO(), &master.Key)
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	base.AuditOperation("make-co-master", instanceKey, instance.ClusterName, fmt.Sprintf("%+v made co-master of %+v", *instanceKey, master.Key))

	return instance, err
}

// Attempt to read and return replication credentials from the mysql.slave_master_info system table
func ReadReplicationCredentials(instanceKey *dtstruct.InstanceKey) (creds *dtstruct.ReplicationCredentials, err error) {
	creds = &dtstruct.ReplicationCredentials{}
	if config.Config.ReplicationCredentialsQuery != "" {
		err = ScanInstanceRow(instanceKey, config.Config.ReplicationCredentialsQuery,
			&creds.User,
			&creds.Password,
			&creds.SSLCaCert,
			&creds.SSLCert,
			&creds.SSLKey,
		)
		if err == nil && creds.User == "" {
			err = fmt.Errorf("Empty username retrieved by ReplicationCredentialsQuery")
		}
		if err == nil {
			return creds, nil
		}
		log.Errore(err)
	}
	// Didn't get credentials from ReplicationCredentialsQuery, or ReplicationCredentialsQuery doesn't exist in the first place?
	// We brute force our way through mysql.slave_master_info
	{
		query := `
			select
				ifnull(max(User_name), '') as user,
				ifnull(max(User_password), '') as password
			from
				mysql.slave_master_info
		`
		err = ScanInstanceRow(instanceKey, query, &creds.User, &creds.Password)
		if err == nil && creds.User == "" {
			err = fmt.Errorf("Empty username found in mysql.slave_master_info")
		}
	}
	return creds, log.Errore(err)
}

func MakeMaster(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	masterInstance, err := GetInfoFromInstance(&instance.UpstreamKey, false, false, nil, "")
	if err == nil {
		// If the read succeeded, check the master status.
		if masterInstance.IsReplica() {
			return instance, fmt.Errorf("MakeMaster: instance's master %+v seems to be replicating", masterInstance.Key)
		}
		if masterInstance.IsLastCheckValid {
			return instance, fmt.Errorf("MakeMaster: instance's master %+v seems to be accessible", masterInstance.Key)
		}
	}
	// Continue anyway if the read failed, because that means the master is
	// inaccessible... So it's OK to do the promotion.
	if !instance.SQLThreadUpToDate() {
		return instance, fmt.Errorf("MakeMaster: instance's SQL thread must be up-to-date with I/O thread for %+v", *instanceKey)
	}
	siblings, err := ReadReplicaInstances(&masterInstance.Key)
	if err != nil {
		return instance, err
	}
	for _, sibling := range siblings {
		if instance.ExecBinlogCoordinates.SmallerThan(&sibling.ExecBinlogCoordinates) {
			return instance, fmt.Errorf("MakeMaster: instance %+v has more advanced sibling: %+v", *instanceKey, sibling.GetInstance().Key)
		}
	}

	if maintenanceToken, merr := base.BeginMaintenance(instanceKey, dtstruct.GetMaintenanceOwner(), fmt.Sprintf("siblings match below this: %+v", *instanceKey)); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer base.EndMaintenance(maintenanceToken)
	}

	_, _, err, _ = MultiMatchBelow(siblings, instanceKey, nil)
	if err != nil {
		goto Cleanup
	}

	SetReadOnly(instanceKey, false)

Cleanup:
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	base.AuditOperation("make-master", instanceKey, instance.ClusterName, fmt.Sprintf("made master of %+v", *instanceKey))

	return instance, err
}

// TakeMaster will move an instance up the chain and cause its master to become its replica.
// It's almost a role change, just that other replicas of either 'instance' or its master are currently unaffected
// (they continue replicate without change)
// Note that the master must itself be a replica; however the grandparent does not necessarily have to be reachable
// and can in fact be dead.
func TakeMaster(instanceKey *dtstruct.InstanceKey, allowTakingCoMaster bool) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, fmt.Errorf("takeMaster: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}
	masterInstance, found, err := ReadFromBackendDB(&instance.UpstreamKey)
	if err != nil || !found {
		return instance, err
	}
	if masterInstance.IsCoUpstream && !allowTakingCoMaster {
		return instance, fmt.Errorf("%+v is co-master. Cannot take it.", masterInstance.Key)
	}
	log.Debugf("TakeMaster: will attempt making %+v take its master %+v, now resolved as %+v", *instanceKey, instance.UpstreamKey, masterInstance.Key)

	if canReplicate, err := CanReplicateFrom(masterInstance, instance); canReplicate == false {
		return instance, err
	}
	// We begin
	masterInstance, err = StopReplication(&masterInstance.Key)
	if err != nil {
		goto Cleanup
	}
	instance, err = StopReplication(&instance.Key)
	if err != nil {
		goto Cleanup
	}

	instance, err = StartReplicationUntilMasterCoordinates(&instance.Key, &masterInstance.SelfBinlogCoordinates)
	if err != nil {
		goto Cleanup
	}

	// instance and masterInstance are equal
	// We skip name unresolve. It is OK if the master's master is dead, unreachable, does not resolve properly.
	// We just copy+paste info from the master.
	// In particular, this is commonly calledin DeadMaster recovery
	instance, err = ChangeMasterTo(&instance.Key, &masterInstance.UpstreamKey, &masterInstance.ExecBinlogCoordinates, true, constant.GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}
	// instance is now sibling of master
	masterInstance, err = ChangeMasterTo(&masterInstance.Key, &instance.Key, &instance.SelfBinlogCoordinates, false, constant.GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}
	// swap is done!

Cleanup:
	if instance != nil {
		instance, _ = StartReplication(context.TODO(), &instance.Key)
	}
	if masterInstance != nil {
		masterInstance, _ = StartReplication(context.TODO(), &masterInstance.Key)
	}
	if err != nil {
		return instance, err
	}
	base.AuditOperation("take-master", instanceKey, instance.ClusterName, fmt.Sprintf("took master: %+v", masterInstance.Key))

	// Created this to enable a custom hook to be called after a TakeMaster success.
	// This only runs if there is a hook configured in ham4db.conf.json
	demoted := masterInstance
	successor := instance
	if config.Config.PostTakeMasterProcesses != nil {
		TakeMasterHook(successor, demoted)
	}

	return instance, err
}

// Created this function to allow a hook to be called after a successful TakeMaster event
func TakeMasterHook(successor *mdtstruct.MysqlInstance, demoted *mdtstruct.MysqlInstance) {
	if demoted == nil {
		return
	}
	if successor == nil {
		return
	}
	successorKey := successor.Key
	demotedKey := demoted.Key
	env := goos.Environ()

	env = append(env, fmt.Sprintf("ORC_SUCCESSOR_HOST=%s", successorKey))
	env = append(env, fmt.Sprintf("ORC_FAILED_HOST=%s", demotedKey))

	successorStr := fmt.Sprintf("%s", successorKey)
	demotedStr := fmt.Sprintf("%s", demotedKey)

	processCount := len(config.Config.PostTakeMasterProcesses)
	for i, command := range config.Config.PostTakeMasterProcesses {
		fullDescription := fmt.Sprintf("PostTakeMasterProcesses hook %d of %d", i+1, processCount)
		log.Debugf("Take-Master: PostTakeMasterProcesses: Calling %+s", fullDescription)
		start := time.Now()
		if err := osp.CommandRun(command, env, successorStr, demotedStr); err == nil {
			info := fmt.Sprintf("Completed %s in %v", fullDescription, time.Since(start))
			log.Infof("Take-Master: %s", info)
		} else {
			info := fmt.Sprintf("Execution of PostTakeMasterProcesses failed in %v with error: %v", time.Since(start), err)
			log.Errorf("Take-Master: %s", info)
		}
	}
}
func MakeLocalMaster(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	masterInstance, found, err := ReadFromBackendDB(&instance.UpstreamKey)
	if err != nil || !found {
		return instance, err
	}
	grandparentInstance, err := GetInfoFromInstance(&masterInstance.UpstreamKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	siblings, err := ReadReplicaInstances(&masterInstance.Key)
	if err != nil {
		return instance, err
	}
	for _, sibling := range siblings {
		if instance.ExecBinlogCoordinates.SmallerThan(&sibling.ExecBinlogCoordinates) {
			return instance, fmt.Errorf("MakeMaster: instance %+v has more advanced sibling: %+v", *instanceKey, sibling.Key)
		}
	}

	instance, err = StopReplicationNicely(instanceKey, 0)
	if err != nil {
		goto Cleanup
	}

	_, _, err = MatchBelow(instanceKey, &grandparentInstance.Key, true)
	if err != nil {
		goto Cleanup
	}

	_, _, err, _ = MultiMatchBelow(siblings, instanceKey, nil)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	base.AuditOperation("make-local-master", instanceKey, instance.ClusterName, fmt.Sprintf("made master of %+v", *instanceKey))

	return instance, err
}

// relocateBelowInternal is a protentially recursive function which chooses how to relocate an instance below another.
// It may choose to use Pseudo-GTID, or normal binlog positions, or take advantage of binlog servers,
// or it may combine any of the above in a multi-step operation.
func relocateBelowInternal(instance, other *mdtstruct.MysqlInstance) (*mdtstruct.MysqlInstance, error) {
	if canReplicate, err := CanReplicateFrom(instance, other); !canReplicate {
		return instance, log.Errorf("%+v cannot replicate from %+v. Reason: %+v", instance.Key, other.Key, err)
	}
	// simplest:
	if dtstruct.IsUpstreamOf(other, instance) {
		// already the desired setup.
		return Repoint(&instance.Key, &other.Key, constant.GTIDHintNeutral)
	}
	// Do we have record of equivalent coordinates?
	if !instance.IsReplicaServer() {
		if movedInstance, err := MoveEquivalent(&instance.Key, &other.Key); err == nil {
			return movedInstance, nil
		}
	}
	// Try and take advantage of binlog servers:
	if dtstruct.IsSibling(instance, other) && other.IsReplicaServer() {
		return MoveBelow(&instance.Key, &other.Key)
	}
	instanceMaster, _, err := ReadFromBackendDB(&instance.UpstreamKey)
	if err != nil {
		return instance, err
	}
	if instanceMaster != nil && instanceMaster.UpstreamKey.Equals(&other.Key) && instanceMaster.IsReplicaServer() {
		// Moving to grandparent via binlog server
		return Repoint(&instance.Key, &instanceMaster.UpstreamKey, constant.GTIDHintDeny)
	}
	if other.IsReplicaServer() {
		if instanceMaster != nil && instanceMaster.IsReplicaServer() && dtstruct.IsSibling(instanceMaster, other) {
			// Special case: this is a binlog server family; we move under the uncle, in one single step
			return Repoint(&instance.Key, &other.Key, constant.GTIDHintDeny)
		}

		// Relocate to its master, then repoint to the binlog server
		otherMaster, found, err := ReadFromBackendDB(&other.UpstreamKey)
		if err != nil {
			return instance, err
		}
		if !found {
			return instance, log.Errorf("Cannot find master %+v", other.UpstreamKey)
		}
		if !other.IsLastCheckValid {
			return instance, log.Errorf("Binlog server %+v is not reachable. It would take two steps to relocate %+v below it, and I won't even do the first step.", other.Key, instance.Key)
		}

		log.Debugf("Relocating to a binlog server; will first attempt to relocate to the binlog server's master: %+v, and then repoint down", otherMaster.Key)
		if _, err := relocateBelowInternal(instance, otherMaster); err != nil {
			return instance, err
		}
		return Repoint(&instance.Key, &other.Key, constant.GTIDHintDeny)
	}
	if instance.IsReplicaServer() {
		// Can only move within the binlog-server family tree
		// And these have been covered just now: move up from a master binlog server, move below a binling binlog server.
		// sure, the family can be more complex, but we keep these operations atomic
		return nil, log.Errorf("Relocating binlog server %+v below %+v turns to be too complex; please do it manually", instance.Key, other.Key)
	}
	// Next, try GTID
	if _, _, gtidCompatible := instancesAreGTIDAndCompatible(instance, other); gtidCompatible {
		return moveInstanceBelowViaGTID(instance, other)
	}

	// Next, try Pseudo-GTID
	if instance.UsingPseudoGTID && other.UsingPseudoGTID {
		// We prefer PseudoGTID to anything else because, while it takes longer to run, it does not issue
		// a STOP SLAVE on any server other than "instance" itself.
		instance, _, err := MatchBelow(&instance.Key, &other.Key, true)
		return instance, err
	}
	// No Pseudo-GTID; cehck simple binlog file/pos operations:
	if dtstruct.IsSibling(instance, other) {
		// If comastering, only move below if it's read-only
		if !other.IsCoUpstream || other.ReadOnly {
			return MoveBelow(&instance.Key, &other.Key)
		}
	}
	// See if we need to MoveUp
	if instanceMaster != nil && instanceMaster.UpstreamKey.Equals(&other.Key) {
		// Moving to grandparent--handles co-mastering writable case
		return MoveUp(&instance.Key)
	}
	if instanceMaster != nil && instanceMaster.IsReplicaServer() {
		// Break operation into two: move (repoint) up, then continue
		if _, err := MoveUp(&instance.Key); err != nil {
			return instance, err
		}
		return relocateBelowInternal(instance, other)
	}
	// Too complex
	return nil, log.Errorf("Relocating %+v below %+v turns to be too complex; please do it manually", instance.Key, other.Key)
}

// RelocateBelow will attempt moving instance indicated by instanceKey below another instance.
// will try and figure out the best way to relocate the server. This could span normal
// binlog-position, pseudo-gtid, repointing, binlog servers...
func RelocateBelow(instanceKey, otherKey *dtstruct.InstanceKey) (interface{}, error) {
	instance, found, err := ReadFromBackendDB(instanceKey)
	if err != nil || !found {
		return instance, log.Errorf("Error reading %+v", *instanceKey)
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, log.Errorf("relocate: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}
	other, found, err := ReadFromBackendDB(otherKey)
	if err != nil || !found {
		return instance, log.Errorf("Error reading %+v", *otherKey)
	}
	// Disallow setting up a group primary to replicate from a group secondary
	if instance.IsReplicationGroupPrimary() && other.ReplicationGroupName == instance.ReplicationGroupName {
		return instance, log.Errorf("relocate: Setting a group primary to replicate from another member of its group is disallowed")
	}
	if other.IsDescendantOf(instance) {
		return instance, log.Errorf("relocate: %+v is a descendant of %+v", *otherKey, instance.Key)
	}
	instance, err = relocateBelowInternal(instance, other)
	if err == nil {
		base.AuditOperation("relocate-below", instanceKey, instance.ClusterName, fmt.Sprintf("relocated %+v below %+v", *instanceKey, *otherKey))
	}
	return instance, err
}

// MatchUp will move a replica up the replication chain, so that it becomes sibling of its master, via Pseudo-GTID
func MatchUp(instanceKey *dtstruct.InstanceKey, requireInstanceMaintenance bool) (*mdtstruct.MysqlInstance, *dtstruct.LogCoordinates, error) {
	instance, found, err := ReadFromBackendDB(instanceKey)
	if err != nil || !found {
		return nil, nil, err
	}
	if !instance.IsReplica() {
		return instance, nil, fmt.Errorf("instance is not a replica: %+v", instanceKey)
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, nil, fmt.Errorf("MatchUp: %+v is a secondary replication group member, hence, it cannot be relocated", instance.Key)
	}
	master, found, err := ReadFromBackendDB(&instance.UpstreamKey)
	if err != nil || !found {
		return instance, nil, log.Errorf("Cannot get master for %+v. error=%+v", instance.Key, err)
	}

	if !master.IsReplica() {
		return instance, nil, fmt.Errorf("master is not a replica itself: %+v", master.Key)
	}

	return MatchBelow(instanceKey, &master.UpstreamKey, requireInstanceMaintenance)
}

func MatchBelow(instanceKey, otherKey *dtstruct.InstanceKey, requireInstanceMaintenance bool) (*mdtstruct.MysqlInstance, *dtstruct.LogCoordinates, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, nil, err
	}
	// Relocation of group secondaries makes no sense, group secondaries, by definition, always replicate from the group
	// primary
	if instance.IsReplicationGroupSecondary() {
		return instance, nil, fmt.Errorf("MatchBelow: %+v is a secondary replication group member, hence, it cannot be relocated", *instanceKey)
	}
	if config.Config.PseudoGTIDPattern == "" {
		return instance, nil, fmt.Errorf("PseudoGTIDPattern not configured; cannot use Pseudo-GTID")
	}
	if instanceKey.Equals(otherKey) {
		return instance, nil, fmt.Errorf("MatchBelow: attempt to match an instance below itself %+v", *instanceKey)
	}
	otherInstance, err := GetInfoFromInstance(otherKey, false, false, nil, "")
	if err != nil {
		return instance, nil, err
	}

	rinstance, _, _ := ReadFromBackendDB(&instance.Key)
	if canMove, merr := rinstance.CanMoveViaMatch(); !canMove {
		return instance, nil, merr
	}

	if canReplicate, err := CanReplicateFrom(instance, otherInstance); !canReplicate {
		return instance, nil, err
	}
	var nextBinlogCoordinatesToMatch *dtstruct.LogCoordinates
	var countMatchedEvents int

	if otherInstance.IsReplicaServer() {
		// A Binlog Server does not do all the SHOW BINLOG EVENTS stuff
		err = fmt.Errorf("Cannot use PseudoGTID with Binlog Server %+v", otherInstance.Key)
		goto Cleanup
	}

	log.Infof("Will match %+v below %+v", *instanceKey, *otherKey)

	if requireInstanceMaintenance {
		if maintenanceToken, merr := base.BeginMaintenance(instanceKey, dtstruct.GetMaintenanceOwner(), fmt.Sprintf("match below %+v", *otherKey)); merr != nil {
			err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
			goto Cleanup
		} else {
			defer base.EndMaintenance(maintenanceToken)
		}

		// We don't require grabbing maintenance lock on otherInstance, but we do request
		// that it is not already under maintenance.
		if inMaintenance, merr := base.InMaintenance(&otherInstance.Key); merr != nil {
			err = merr
			goto Cleanup
		} else if inMaintenance {
			err = fmt.Errorf("Cannot match below %+v; it is in maintenance", otherInstance.Key)
			goto Cleanup
		}
	}

	log.Debugf("Stopping replica on %+v", *instanceKey)
	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	nextBinlogCoordinatesToMatch, countMatchedEvents, err = CorrelateBinlogCoordinates(instance, nil, otherInstance)

	if countMatchedEvents == 0 {
		err = fmt.Errorf("Unexpected: 0 events processed while iterating logs. Something went wrong; aborting. nextBinlogCoordinatesToMatch: %+v", nextBinlogCoordinatesToMatch)
		goto Cleanup
	}
	log.Debugf("%+v will match below %+v at %+v; validated events: %d", *instanceKey, *otherKey, *nextBinlogCoordinatesToMatch, countMatchedEvents)

	// Drum roll...
	instance, err = ChangeMasterTo(instanceKey, otherKey, nextBinlogCoordinatesToMatch, false, constant.GTIDHintDeny)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(context.TODO(), instanceKey)
	if err != nil {
		return instance, nextBinlogCoordinatesToMatch, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	base.AuditOperation("match-below", instanceKey, instance.ClusterName, fmt.Sprintf("matched %+v below %+v", *instanceKey, *otherKey))

	return instance, nextBinlogCoordinatesToMatch, err
}

// EnableGTID will attempt to enable GTID-mode (either Oracle or MariaDB)
func EnableGTID(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	if instance.UsingGTID() {
		return instance, fmt.Errorf("%+v already uses GTID", *instanceKey)
	}

	log.Infof("Will attempt to enable GTID on %+v", *instanceKey)

	instance, err = Repoint(instanceKey, nil, constant.GTIDHintForce)
	if err != nil {
		return instance, err
	}
	if !instance.UsingGTID() {
		return instance, fmt.Errorf("Cannot enable GTID on %+v", *instanceKey)
	}

	base.AuditOperation("enable-gtid", instanceKey, instance.ClusterName, fmt.Sprintf("enabled GTID on %+v", *instanceKey))

	return instance, err
}

// DisableGTID will attempt to disable GTID-mode (either Oracle or MariaDB) and revert to binlog file:pos replication
func DisableGTID(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	if !instance.UsingGTID() {
		return instance, fmt.Errorf("%+v is not using GTID", *instanceKey)
	}

	log.Infof("Will attempt to disable GTID on %+v", *instanceKey)

	instance, err = Repoint(instanceKey, nil, constant.GTIDHintDeny)
	if err != nil {
		return instance, err
	}
	if instance.UsingGTID() {
		return instance, fmt.Errorf("Cannot disable GTID on %+v", *instanceKey)
	}

	base.AuditOperation("disable-gtid", instanceKey, instance.ClusterName, fmt.Sprintf("disabled GTID on %+v", *instanceKey))

	return instance, err
}

func LocateErrantGTID(instanceKey *dtstruct.InstanceKey) (errantBinlogs []string, err error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return errantBinlogs, err
	}
	errantSearch := instance.GtidErrant
	if errantSearch == "" {
		return errantBinlogs, log.Errorf("locate-errant-gtid: no errant-gtid on %+v", *instanceKey)
	}
	subtract, err := GTIDSubtract(instanceKey, errantSearch, instance.GtidPurged)
	if err != nil {
		return errantBinlogs, err
	}
	if subtract != errantSearch {
		return errantBinlogs, fmt.Errorf("locate-errant-gtid: %+v is already purged on %+v", subtract, *instanceKey)
	}
	binlogs, err := ShowBinaryLogs(instanceKey)
	if err != nil {
		return errantBinlogs, err
	}
	previousGTIDs := make(map[string]*OracleGtidSet)
	for _, binlog := range binlogs {
		oracleGTIDSet, err := GetPreviousGTIDs(instanceKey, binlog)
		if err != nil {
			return errantBinlogs, err
		}
		previousGTIDs[binlog] = oracleGTIDSet
	}
	for i, binlog := range binlogs {
		if errantSearch == "" {
			break
		}
		previousGTID := previousGTIDs[binlog]
		subtract, err := GTIDSubtract(instanceKey, errantSearch, previousGTID.String())
		if err != nil {
			return errantBinlogs, err
		}
		if subtract != errantSearch {
			// binlogs[i-1] is safe to use when i==0. because that implies GTIDs have been purged,
			// which covered by an earlier assertion
			errantBinlogs = append(errantBinlogs, binlogs[i-1])
			errantSearch = subtract
		}
	}
	if errantSearch != "" {
		// then it's in the last binary log
		errantBinlogs = append(errantBinlogs, binlogs[len(binlogs)-1])
	}
	return errantBinlogs, err
}

// ErrantGTIDInjectEmpty will inject an empty transaction on the master of an instance's cluster in order to get rid
// of an errant transaction observed on the instance.
func ErrantGTIDInjectEmpty(instanceKey *dtstruct.InstanceKey) (instance *mdtstruct.MysqlInstance, clusterMaster *mdtstruct.MysqlInstance, countInjectedTransactions int64, err error) {
	instance, err = GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, clusterMaster, countInjectedTransactions, err
	}
	if instance.GtidErrant == "" {
		return instance, clusterMaster, countInjectedTransactions, log.Errorf("gtid-errant-inject-empty will not operate on %+v because no errant GTID is found", *instanceKey)
	}
	if !instance.SupportsOracleGTID {
		return instance, clusterMaster, countInjectedTransactions, log.Errorf("gtid-errant-inject-empty requested for %+v but it does not support oracle-gtid", *instanceKey)
	}

	masters, err := ReadClusterWriteableMaster(instance.ClusterName)
	if err != nil {
		return instance, clusterMaster, countInjectedTransactions, err
	}
	if len(masters) == 0 {
		return instance, clusterMaster, countInjectedTransactions, log.Errorf("gtid-errant-inject-empty found no writabel master for %+v cluster", instance.ClusterName)
	}
	clusterMaster = masters[0]

	if !clusterMaster.SupportsOracleGTID {
		return instance, clusterMaster, countInjectedTransactions, log.Errorf("gtid-errant-inject-empty requested for %+v but the cluster's master %+v does not support oracle-gtid", *instanceKey, clusterMaster.Key)
	}

	gtidSet, err := NewOracleGtidSet(instance.GtidErrant)
	if err != nil {
		return instance, clusterMaster, countInjectedTransactions, err
	}
	explodedEntries := gtidSet.Explode()
	log.Infof("gtid-errant-inject-empty: about to inject %+v empty transactions %+v on cluster master %+v", len(explodedEntries), gtidSet.String(), clusterMaster.Key)
	for _, entry := range explodedEntries {
		if err := injectEmptyGTIDTransaction(&clusterMaster.Key, entry); err != nil {
			return instance, clusterMaster, countInjectedTransactions, err
		}
		countInjectedTransactions++
	}

	// and we're done (pending deferred functions)
	base.AuditOperation("gtid-errant-inject-empty", instanceKey, instance.ClusterName, fmt.Sprintf("injected %+v empty transactions on %+v", countInjectedTransactions, clusterMaster.Key))

	return instance, clusterMaster, countInjectedTransactions, err
}

// injectEmptyGTIDTransaction
func injectEmptyGTIDTransaction(instanceKey *dtstruct.InstanceKey, gtidEntry *OracleGtidSetEntry) error {
	db, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return err
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`SET GTID_NEXT="%s"`, gtidEntry.String())); err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `SET GTID_NEXT="AUTOMATIC"`); err != nil {
		return err
	}
	return nil
}

// FindLastPseudoGTIDEntry will search an instance's binary logs or relay logs for the last pseudo-GTID entry,
// and return found coordinates as well as entry text
func FindLastPseudoGTIDEntry(instance *mdtstruct.MysqlInstance, recordedInstanceRelayLogCoordinates dtstruct.LogCoordinates, maxBinlogCoordinates *dtstruct.LogCoordinates, exhaustiveSearch bool, expectedBinlogFormat *string) (instancePseudoGtidCoordinates *dtstruct.LogCoordinates, instancePseudoGtidText string, err error) {

	if config.Config.PseudoGTIDPattern == "" {
		return instancePseudoGtidCoordinates, instancePseudoGtidText, fmt.Errorf("PseudoGTIDPattern not configured; cannot use Pseudo-GTID")
	}

	if instance.LogBinEnabled && instance.LogReplicationUpdatesEnabled && !*dtstruct.RuntimeCLIFlags.SkipBinlogSearch && (expectedBinlogFormat == nil || instance.Binlog_format == *expectedBinlogFormat) {
		minBinlogCoordinates, _, _ := GetHeuristiclyRecentCoordinatesForInstance(&instance.Key)
		// Well no need to search this instance's binary logs if it doesn't have any...
		// With regard log-slave-updates, some edge cases are possible, like having this instance's log-slave-updates
		// enabled/disabled (of course having restarted it)
		// The approach is not to take chances. If log-slave-updates is disabled, fail and go for relay-logs.
		// If log-slave-updates was just enabled then possibly no pseudo-gtid is found, and so again we will go
		// for relay logs.
		// Also, if master has STATEMENT binlog format, and the replica has ROW binlog format, then comparing binlog entries would urely fail if based on the replica's binary logs.
		// Instead, we revert to the relay logs.
		instancePseudoGtidCoordinates, instancePseudoGtidText, err = getLastPseudoGTIDEntryInInstance(instance, minBinlogCoordinates, maxBinlogCoordinates, exhaustiveSearch)
	}
	if err != nil || instancePseudoGtidCoordinates == nil {
		minRelaylogCoordinates, _ := GetPreviousKnownRelayLogCoordinatesForInstance(instance)
		// Unable to find pseudo GTID in binary logs.
		// Then MAYBE we are lucky enough (chances are we are, if this replica did not crash) that we can
		// extract the Pseudo GTID entry from the last (current) relay log file.
		instancePseudoGtidCoordinates, instancePseudoGtidText, err = getLastPseudoGTIDEntryInRelayLogs(instance, minRelaylogCoordinates, recordedInstanceRelayLogCoordinates, exhaustiveSearch)
	}
	return instancePseudoGtidCoordinates, instancePseudoGtidText, err
}

// MatchUpReplicas will move all replicas of given master up the replication chain,
// so that they become siblings of their master.
// This should be called when the local master dies, and all its replicas are to be resurrected via Pseudo-GTID
func MatchUpReplicas(masterKey *dtstruct.InstanceKey, pattern string) ([](*mdtstruct.MysqlInstance), *mdtstruct.MysqlInstance, error, []error) {
	res := [](*mdtstruct.MysqlInstance){}
	errs := []error{}

	masterInstance, found, err := ReadFromBackendDB(masterKey)
	if err != nil || !found {
		return res, nil, err, errs
	}

	return MultiMatchReplicas(masterKey, &masterInstance.UpstreamKey, pattern)
}

// MultiMatchBelow will efficiently match multiple replicas below a given instance.
// It is assumed that all given replicas are siblings
func MultiMatchBelow(replicas [](*mdtstruct.MysqlInstance), belowKey *dtstruct.InstanceKey, postponedFunctionsContainer *dtstruct.PostponedFunctionsContainer) (matchedReplicas [](*mdtstruct.MysqlInstance), belowInstance *mdtstruct.MysqlInstance, err error, errs []error) {
	belowInstance, found, err := ReadFromBackendDB(belowKey)
	if err != nil || !found {
		return matchedReplicas, belowInstance, err, errs
	}

	replicas = mutil.RemoveInstance(replicas, belowKey)
	if len(replicas) == 0 {
		// Nothing to do
		return replicas, belowInstance, err, errs
	}

	log.Infof("Will match %+v replicas below %+v via Pseudo-GTID, independently", len(replicas), belowKey)

	barrier := make(chan *dtstruct.InstanceKey)
	replicaMutex := &sync.Mutex{}

	for _, replica := range replicas {
		replica := replica

		// Parallelize repoints
		go func() {
			defer func() { barrier <- &replica.Key }()
			matchFunc := func() error {
				replica, _, replicaErr := MatchBelow(&replica.Key, belowKey, true)

				replicaMutex.Lock()
				defer replicaMutex.Unlock()

				if replicaErr == nil {
					matchedReplicas = append(matchedReplicas, replica)
				} else {
					errs = append(errs, replicaErr)
				}
				return replicaErr
			}
			if ShouldPostponeRelocatingReplica(replica, postponedFunctionsContainer) {
				postponedFunctionsContainer.AddPostponedFunction(matchFunc, fmt.Sprintf("multi-match-below-independent %+v", replica.Key))
				// We bail out and trust our invoker to later call upon this postponed function
			} else {
				limiter.ExecuteOnTopology(func() { matchFunc() })
			}
		}()
	}
	for range replicas {
		<-barrier
	}
	if len(errs) == len(replicas) {
		// All returned with error
		return matchedReplicas, belowInstance, fmt.Errorf("MultiMatchBelowIndependently: Error on all %+v operations", len(errs)), errs
	}
	base.AuditOperation("multi-match-below-independent", belowKey, belowInstance.ClusterName, fmt.Sprintf("matched %d/%d replicas below %+v via Pseudo-GTID", len(matchedReplicas), len(replicas), belowKey))

	return matchedReplicas, belowInstance, err, errs
}

// TakeSiblings is a convenience method for turning siblings of a replica to be its subordinates.
// This operation is a syntatctic sugar on top relocate-replicas, which uses any available means to the objective:
// GTID, Pseudo-GTID, binlog servers, standard replication...
func TakeSiblings(instanceKey *dtstruct.InstanceKey) (instance *mdtstruct.MysqlInstance, takenSiblings int, err error) {
	instance, err = GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, 0, err
	}
	if !instance.IsReplica() {
		return instance, takenSiblings, log.Errorf("take-siblings: instance %+v is not a replica.", *instanceKey)
	}
	relocatedReplicas, _, err, _ := RelocateReplicas(&instance.UpstreamKey, instanceKey, "")

	return instance, len(relocatedReplicas), err
}

// RematchReplica will re-match a replica to its master, using pseudo-gtid
func RematchReplica(instanceKey *dtstruct.InstanceKey, requireInstanceMaintenance bool) (*mdtstruct.MysqlInstance, *dtstruct.LogCoordinates, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, nil, err
	}
	masterInstance, found, err := ReadFromBackendDB(&instance.UpstreamKey)
	if err != nil || !found {
		return instance, nil, err
	}
	return MatchBelow(instanceKey, &masterInstance.Key, requireInstanceMaintenance)
}

// RelocateReplicas will attempt moving replicas of an instance indicated by instanceKey below another instance.
// will try and figure out the best way to relocate the servers. This could span normal
// binlog-position, pseudo-gtid, repointing, binlog servers...
func RelocateReplicas(instanceKey, otherKey *dtstruct.InstanceKey, pattern string) (replicas []*mdtstruct.MysqlInstance, other *mdtstruct.MysqlInstance, err error, errs []error) {

	instance, found, err := ReadFromBackendDB(instanceKey)
	if err != nil || !found {
		return replicas, other, log.Errorf("Error reading %+v", *instanceKey), errs
	}
	other, found, err = ReadFromBackendDB(otherKey)
	if err != nil || !found {
		return replicas, other, log.Errorf("Error reading %+v", *otherKey), errs
	}

	replicas, err = ReadReplicaInstances(instanceKey)
	if err != nil {
		return replicas, other, err, errs
	}
	replicas = mutil.RemoveInstance(replicas, otherKey)
	replicas = mutil.FilterInstancesByPattern(replicas, pattern)
	if len(replicas) == 0 {
		// Nothing to do
		return replicas, other, nil, errs
	}
	for _, replica := range replicas {
		if other.IsDescendantOf(replica) {
			return replicas, other, log.Errorf("relocate-replicas: %+v is a descendant of %+v", *otherKey, replica.Key), errs
		}
	}
	replicas, err, errs = relocateReplicasInternal(replicas, instance, other)

	if err == nil {
		base.AuditOperation("relocate-replicas", instanceKey, instance.ClusterName, fmt.Sprintf("relocated %+v replicas of %+v below %+v", len(replicas), *instanceKey, *otherKey))
	}
	return replicas, other, err, errs
}

// relocateReplicasInternal is a protentially recursive function which chooses how to relocate
// replicas of an instance below another.
// It may choose to use Pseudo-GTID, or normal binlog positions, or take advantage of binlog servers,
// or it may combine any of the above in a multi-step operation.
func relocateReplicasInternal(replicas [](*mdtstruct.MysqlInstance), instance, other *mdtstruct.MysqlInstance) ([](*mdtstruct.MysqlInstance), error, []error) {
	errs := []error{}
	var err error
	// simplest:
	if instance.Key.Equals(&other.Key) {
		// already the desired setup.
		return RepointTo(replicas, &other.Key)
	}
	// Try and take advantage of binlog servers:
	if dtstruct.IsUpstreamOf(other, instance) && instance.IsReplicaServer() {
		// Up from a binlog server
		return RepointTo(replicas, &other.Key)
	}
	if dtstruct.IsUpstreamOf(instance, other) && other.IsReplicaServer() {
		// Down under a binlog server
		return RepointTo(replicas, &other.Key)
	}
	if dtstruct.IsSibling(instance, other) && instance.IsReplicaServer() && other.IsReplicaServer() {
		// Between siblings
		return RepointTo(replicas, &other.Key)
	}
	if other.IsReplicaServer() {
		// Relocate to binlog server's parent (recursive call), then repoint down
		otherMaster, found, err := ReadFromBackendDB(&other.UpstreamKey)
		if err != nil || !found {
			return nil, err, errs
		}
		replicas, err, errs = relocateReplicasInternal(replicas, instance, otherMaster)
		if err != nil {
			return replicas, err, errs
		}

		return RepointTo(replicas, &other.Key)
	}
	// GTID
	{
		movedReplicas, unmovedReplicas, err, errs := moveReplicasViaGTID(replicas, other, nil)

		if len(movedReplicas) == len(replicas) {
			// Moved (or tried moving) everything via GTID
			return movedReplicas, err, errs
		} else if len(movedReplicas) > 0 {
			// something was moved via GTID; let's try further on
			return relocateReplicasInternal(unmovedReplicas, instance, other)
		}
		// Otherwise nothing was moved via GTID. Maybe we don't have any GTIDs, we continue.
	}

	// Pseudo GTID
	if other.UsingPseudoGTID {
		// Which replicas are using Pseudo GTID?
		var pseudoGTIDReplicas [](*mdtstruct.MysqlInstance)
		for _, replica := range replicas {
			_, _, hasToBeGTID := instancesAreGTIDAndCompatible(replica, other)
			if replica.UsingPseudoGTID && !hasToBeGTID {
				pseudoGTIDReplicas = append(pseudoGTIDReplicas, replica)
			}
		}
		pseudoGTIDReplicas, _, err, errs = MultiMatchBelow(pseudoGTIDReplicas, &other.Key, nil)
		return pseudoGTIDReplicas, err, errs
	}

	// Normal binlog file:pos
	if dtstruct.IsUpstreamOf(other, instance) {
		// MoveUpReplicas -- but not supporting "replicas" argument at this time.
	}

	// Too complex
	return nil, log.Errorf("Relocating %+v replicas of %+v below %+v turns to be too complex; please do it manually", len(replicas), instance.Key, other.Key), errs
}

// PurgeBinaryLogsTo attempts to 'PURGE BINARY LOGS' until given binary log is reached
func PurgeBinaryLogsTo(instanceKey *dtstruct.InstanceKey, logFile string, force bool) (*mdtstruct.MysqlInstance, error) {
	replicas, err := ReadReplicaInstances(instanceKey)
	if err != nil {
		return nil, err
	}
	if !force {
		purgeCoordinates := &dtstruct.LogCoordinates{LogFile: logFile, LogPos: 0}
		for _, replica := range replicas {
			if !purgeCoordinates.SmallerThan(&replica.ExecBinlogCoordinates) {
				return nil, log.Errorf("Unsafe to purge binary logs on %+v up to %s because replica %+v has only applied up to %+v", *instanceKey, logFile, replica.Key, replica.ExecBinlogCoordinates)
			}
		}
	}
	return purgeBinaryLogsTo(instanceKey, logFile)
}

// purgeBinaryLogsTo attempts to 'PURGE BINARY LOGS' until given binary log is reached
func purgeBinaryLogsTo(instanceKey *dtstruct.InstanceKey, logFile string) (*mdtstruct.MysqlInstance, error) {
	if *dtstruct.RuntimeCLIFlags.Noop {
		return nil, fmt.Errorf("noop: aborting purge-binary-logs operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err := ExecSQLOnInstance(instanceKey, "purge binary logs to ?", logFile)
	if err != nil {
		return nil, log.Errore(err)
	}

	log.Infof("purge-binary-logs to=%+v on %+v", logFile, *instanceKey)

	clusterName, _ := base.GetClusterName(instanceKey)
	base.AuditOperation("purge-binary-logs", instanceKey, clusterName, "success")

	return GetInfoFromInstance(instanceKey, false, false, nil, "")
}

// CorrelateBinlogCoordinates find out, if possible, the binlog coordinates of given otherInstance that correlate
// with given coordinates of given instance.
func CorrelateBinlogCoordinates(instance *mdtstruct.MysqlInstance, binlogCoordinates *dtstruct.LogCoordinates, otherInstance *mdtstruct.MysqlInstance) (*dtstruct.LogCoordinates, int, error) {
	// We record the relay log coordinates just after the instance stopped since the coordinates can change upon
	// a FLUSH LOGS/FLUSH RELAY LOGS (or a START SLAVE, though that's an altogether different problem) etc.
	// We want to be on the safe side; we don't utterly trust that we are the only ones playing with the instance.
	recordedInstanceRelayLogCoordinates := instance.RelaylogCoordinates
	instancePseudoGtidCoordinates, instancePseudoGtidText, err := FindLastPseudoGTIDEntry(instance, recordedInstanceRelayLogCoordinates, binlogCoordinates, true, &otherInstance.Binlog_format)

	if err != nil {
		return nil, 0, err
	}
	entriesMonotonic := (config.Config.PseudoGTIDMonotonicHint != "") && strings.Contains(instancePseudoGtidText, config.Config.PseudoGTIDMonotonicHint)
	minBinlogCoordinates, _, err := GetHeuristiclyRecentCoordinatesForInstance(&otherInstance.Key)
	otherInstancePseudoGtidCoordinates, err := SearchEntryInInstanceBinlogs(otherInstance, instancePseudoGtidText, entriesMonotonic, minBinlogCoordinates)
	if err != nil {
		return nil, 0, err
	}

	// We've found a match: the latest Pseudo GTID position within instance and its identical twin in otherInstance
	// We now iterate the events in both, up to the completion of events in instance (recall that we looked for
	// the last entry in instance, hence, assuming pseudo GTID entries are frequent, the amount of entries to read
	// from instance is not long)
	// The result of the iteration will be either:
	// - bad conclusion that instance is actually more advanced than otherInstance (we find more entries in instance
	//   following the pseudo gtid than we can match in otherInstance), hence we cannot ask instance to replicate
	//   from otherInstance
	// - good result: both instances are exactly in same shape (have replicated the exact same number of events since
	//   the last pseudo gtid). Since they are identical, it is easy to point instance into otherInstance.
	// - good result: the first position within otherInstance where instance has not replicated yet. It is easy to point
	//   instance into otherInstance.
	nextBinlogCoordinatesToMatch, countMatchedEvents, err := GetNextBinlogCoordinatesToMatch(instance, *instancePseudoGtidCoordinates,
		recordedInstanceRelayLogCoordinates, binlogCoordinates, otherInstance, *otherInstancePseudoGtidCoordinates)
	if err != nil {
		return nil, 0, err
	}
	if countMatchedEvents == 0 {
		err = fmt.Errorf("Unexpected: 0 events processed while iterating logs. Something went wrong; aborting. nextBinlogCoordinatesToMatch: %+v", nextBinlogCoordinatesToMatch)
		return nil, 0, err
	}
	return nextBinlogCoordinatesToMatch, countMatchedEvents, nil
}

func CorrelateRelaylogCoordinates(instance *mdtstruct.MysqlInstance, relaylogCoordinates *dtstruct.LogCoordinates, otherInstance *mdtstruct.MysqlInstance) (instanceCoordinates, correlatedCoordinates, nextCoordinates *dtstruct.LogCoordinates, found bool, err error) {
	// The two servers are expected to have the same master, or this doesn't work
	if !instance.UpstreamKey.Equals(&otherInstance.UpstreamKey) {
		return instanceCoordinates, correlatedCoordinates, nextCoordinates, found, log.Errorf("CorrelateRelaylogCoordinates requires sibling instances, however %+v has master %+v, and %+v has master %+v", instance.Key, instance.UpstreamKey, otherInstance.Key, otherInstance.UpstreamKey)
	}
	var binlogEvent *dtstruct.BinlogEvent
	if relaylogCoordinates == nil {
		instanceCoordinates = &instance.RelaylogCoordinates
		if minCoordinates, err := GetPreviousKnownRelayLogCoordinatesForInstance(instance); err != nil {
			return instanceCoordinates, correlatedCoordinates, nextCoordinates, found, err
		} else if binlogEvent, err = GetLastExecutedEntryInRelayLogs(instance, minCoordinates, instance.RelaylogCoordinates); err != nil {
			return instanceCoordinates, correlatedCoordinates, nextCoordinates, found, err
		}
	} else {
		instanceCoordinates = relaylogCoordinates
		relaylogCoordinates.Type = gconstant.RelayLog
		if binlogEvent, err = ReadBinlogEventAtRelayLogCoordinates(&instance.Key, relaylogCoordinates); err != nil {
			return instanceCoordinates, correlatedCoordinates, nextCoordinates, found, err
		}
	}

	_, minCoordinates, err := GetHeuristiclyRecentCoordinatesForInstance(&otherInstance.Key)
	if err != nil {
		return instanceCoordinates, correlatedCoordinates, nextCoordinates, found, err
	}
	correlatedCoordinates, nextCoordinates, found, err = SearchEventInRelayLogs(binlogEvent, otherInstance, minCoordinates, otherInstance.RelaylogCoordinates)
	return instanceCoordinates, correlatedCoordinates, nextCoordinates, found, err
}

// isValidAsCandidateMasterInBinlogServerTopology let's us know whether a given replica is generally
// valid to promote to be master.
func isValidAsCandidateMasterInBinlogServerTopology(replica *mdtstruct.MysqlInstance) bool {
	if !replica.IsLastCheckValid {
		// something wrong with this replica right now. We shouldn't hope to be able to promote it
		return false
	}
	if !replica.LogBinEnabled {
		return false
	}
	if replica.LogReplicationUpdatesEnabled {
		// That's right: we *disallow* log-replica-updates
		return false
	}
	if replica.IsReplicaServer() {
		return false
	}

	return true
}

// GetSortedReplicas reads list of replicas of a given master, and returns them sorted by exec coordinates
// (most up-to-date replica first).
func GetSortedReplicas(masterKey *dtstruct.InstanceKey, stopReplicationMethod gconstant.StopReplicationMethod) (replicas [](*mdtstruct.MysqlInstance), err error) {
	if replicas, err = getReplicasForSorting(masterKey, false); err != nil {
		return replicas, err
	}

	replicas = SortedReplicasDataCenterHint(replicas, stopReplicationMethod, "")

	if len(replicas) == 0 {
		return replicas, fmt.Errorf("No replicas found for %+v", *masterKey)
	}
	return replicas, err
}

// GetNextBinlogCoordinatesToMatch is given a twin-coordinates couple for a would-be replica (instance) and another
// instance (other).
// This is part of the match-below process, and is the heart of the operation: matching the binlog events starting
// the twin-coordinates (where both share the same Pseudo-GTID) until "instance" runs out of entries, hopefully
// before "other" runs out.
// If "other" runs out that means "instance" is more advanced in replication than "other", in which case we can't
// turn it into a replica of "other".
func GetNextBinlogCoordinatesToMatch(
	instance *mdtstruct.MysqlInstance,
	instanceCoordinates dtstruct.LogCoordinates,
	recordedInstanceRelayLogCoordinates dtstruct.LogCoordinates,
	maxBinlogCoordinates *dtstruct.LogCoordinates,
	other *mdtstruct.MysqlInstance,
	otherCoordinates dtstruct.LogCoordinates) (*dtstruct.LogCoordinates, int, error) {

	const noMatchedEvents int = 0 // to make return statements' intent clearer

	// create instanceCursor for scanning instance binlog events
	fetchNextEvents := func(binlogCoordinates dtstruct.LogCoordinates) ([]dtstruct.BinlogEvent, error) {
		return getNextBinlogEventsChunk(instance, binlogCoordinates, 0)
	}
	instanceCursor := dtstruct.NewBinlogEventCursor(instanceCoordinates, fetchNextEvents)

	// create otherCursor for scanning other binlog events
	fetchOtherNextEvents := func(binlogCoordinates dtstruct.LogCoordinates) ([]dtstruct.BinlogEvent, error) {
		return getNextBinlogEventsChunk(other, binlogCoordinates, 0)
	}
	otherCursor := dtstruct.NewBinlogEventCursor(otherCoordinates, fetchOtherNextEvents)

	// for 5.6 to 5.7 replication special processing may be needed.
	applyInstanceSpecialFiltering, applyOtherSpecialFiltering, err := special56To57filterProcessing(instance, other)
	if err != nil {
		return nil, noMatchedEvents, log.Errore(err)
	}

	var (
		beautifyCoordinatesLength    int = 0
		countMatchedEvents           int = 0
		lastConsumedEventCoordinates dtstruct.LogCoordinates
	)

	for {
		// Exhaust binlogs/relaylogs on instance. While iterating them, also iterate the otherInstance binlogs.
		// We expect entries on both to match, sequentially, until instance's binlogs/relaylogs are exhausted.
		var (
			// the whole event to make things simpler
			instanceEvent dtstruct.BinlogEvent
			otherEvent    dtstruct.BinlogEvent
		)

		{
			// we may need to skip Anonymous GTID Next Events so loop here over any we find
			var event *dtstruct.BinlogEvent
			var err error
			for done := false; !done; {
				// Extract next binlog/relaylog entry from instance:
				event, err = instanceCursor.NextRealEvent(0)
				if err != nil {
					return nil, noMatchedEvents, log.Errore(err)
				}
				if event != nil {
					lastConsumedEventCoordinates = event.Coordinates
				}
				if event == nil || !applyInstanceSpecialFiltering || !mutil.SpecialEventToSkip(event) {
					done = true
				}
			}

			switch instanceCoordinates.Type {
			case gconstant.BinaryLog:
				if event == nil {
					// end of binary logs for instance:
					otherNextCoordinates, err := otherCursor.GetNextCoordinates()
					if err != nil {
						return nil, noMatchedEvents, log.Errore(err)
					}
					instanceNextCoordinates, err := instanceCursor.GetNextCoordinates()
					if err != nil {
						return nil, noMatchedEvents, log.Errore(err)
					}
					// sanity check
					if instanceNextCoordinates.SmallerThan(&instance.SelfBinlogCoordinates) {
						return nil, noMatchedEvents, log.Errorf("Unexpected problem: instance binlog iteration ended before self coordinates. Ended with: %+v, self coordinates: %+v", instanceNextCoordinates, instance.SelfBinlogCoordinates)
					}
					// Possible good exit point.
					log.Debugf("Reached end of binary logs for instance, at %+v. Other coordinates: %+v", instanceNextCoordinates, otherNextCoordinates)
					return &otherNextCoordinates, countMatchedEvents, nil
				}
			case gconstant.RelayLog:
				// Argghhhh! SHOW RELAY LOG EVENTS IN '...' statement returns CRAPPY values for End_log_pos:
				// instead of returning the end log pos of the current statement in the *relay log*, it shows
				// the end log pos of the matching statement in the *master's binary log*!
				// Yes, there's logic to this. But this means the next-ccordinates are meaningless.
				// As result, in the case where we exhaust (following) the relay log, we cannot do our last
				// nice sanity test that we've indeed reached the Relay_log_pos coordinate; we are only at the
				// last statement, which is SMALLER than Relay_log_pos; and there isn't a "Rotate" entry to make
				// a place holder or anything. The log just ends and we can't be absolutely certain that the next
				// statement is indeed (futuristically) as End_log_pos.
				endOfScan := false
				if event == nil {
					// End of relay log...
					endOfScan = true
					log.Debugf("Reached end of relay log at %+v", recordedInstanceRelayLogCoordinates)
				} else if recordedInstanceRelayLogCoordinates.Equals(&event.Coordinates) {
					// We've passed the maxScanInstanceCoordinates (applies for relay logs)
					endOfScan = true
					log.Debugf("Reached replica relay log coordinates at %+v", recordedInstanceRelayLogCoordinates)
				} else if recordedInstanceRelayLogCoordinates.SmallerThan(&event.Coordinates) {
					return nil, noMatchedEvents, log.Errorf("Unexpected problem: relay log scan passed relay log position without hitting it. Ended with: %+v, relay log position: %+v", event.Coordinates, recordedInstanceRelayLogCoordinates)
				}
				if endOfScan {
					// end of binary logs for instance:
					otherNextCoordinates, err := otherCursor.GetNextCoordinates()
					if err != nil {
						log.Debugf("otherCursor.getNextCoordinates() failed. otherCoordinates=%+v, cached events in cursor: %d; index=%d", otherCoordinates, len(otherCursor.CachedEvents), otherCursor.CurrentEventIndex)
						return nil, noMatchedEvents, log.Errore(err)
					}
					// Possible good exit point.
					// No further sanity checks (read the above lengthy explanation)
					log.Debugf("Reached limit of relay logs for instance, just after %+v. Other coordinates: %+v", lastConsumedEventCoordinates, otherNextCoordinates)
					return &otherNextCoordinates, countMatchedEvents, nil
				}
			}

			instanceEvent = *event // make a physical copy
			log.Debugf("> %s", dtstruct.FormatEventCleanly(instanceEvent, &beautifyCoordinatesLength))
		}
		{
			// Extract next binlog/relaylog entry from other (intended master):
			// - this must have binlogs. We may need to filter anonymous events if we were processing
			//   a relay log on instance and the instance's master runs 5.6
			var event *dtstruct.BinlogEvent
			var err error
			for done := false; !done; {
				// Extract next binlog entry from other:
				event, err = otherCursor.NextRealEvent(0)
				if err != nil {
					return nil, noMatchedEvents, log.Errore(err)
				}
				if event == nil || !applyOtherSpecialFiltering || !mutil.SpecialEventToSkip(event) {
					done = true
				}
			}

			if event == nil {
				// end of binary logs for otherInstance: this is unexpected and means instance is more advanced
				// than otherInstance
				return nil, noMatchedEvents, log.Errorf("Unexpected end of binary logs for assumed master (%+v). This means the instance which attempted to be a replica (%+v) was more advanced. Try the other way round", other.Key, instance.Key)
			}

			otherEvent = *event // make a physical copy
			log.Debugf("< %s", dtstruct.FormatEventCleanly(otherEvent, &beautifyCoordinatesLength))
		}
		// Verify things are sane (the two extracted entries are identical):
		// (not strictly required by the algorithm but adds such a lovely self-sanity-testing essence)
		if instanceEvent.Info != otherEvent.Info {
			return nil, noMatchedEvents, log.Errorf("Mismatching entries, aborting: %+v <-> %+v", instanceEvent.Info, otherEvent.Info)
		}
		countMatchedEvents++
		if maxBinlogCoordinates != nil {
			// Possible good exit point.
			// Not searching till end of binary logs/relay log exec pos. Instead, we're stopping at an instructed position.
			if instanceEvent.Coordinates.Equals(maxBinlogCoordinates) {
				log.Debugf("maxBinlogCoordinates specified as %+v and reached. Stopping", *maxBinlogCoordinates)
				return &otherEvent.Coordinates, countMatchedEvents, nil
			} else if maxBinlogCoordinates.SmallerThan(&instanceEvent.Coordinates) {
				return nil, noMatchedEvents, log.Errorf("maxBinlogCoordinates (%+v) exceeded but not met", *maxBinlogCoordinates)
			}
		}
	}
	// Won't get here
}

func GetPreviousGTIDs(instanceKey *dtstruct.InstanceKey, binlog string) (previousGTIDs *OracleGtidSet, err error) {
	if binlog == "" {
		return nil, log.Errorf("GetPreviousGTIDs: empty binlog file name for %+v", *instanceKey)
	}
	db, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf("show binlog events in '%s' LIMIT 5", binlog)

	err = sqlutil.QueryRowsMapBuffered(db, query, func(m sqlutil.RowMap) error {
		eventType := m.GetString("Event_type")
		if eventType == "Previous_gtids" {
			var e error
			if previousGTIDs, e = NewOracleGtidSet(m.GetString("Info")); e != nil {
				return e
			}
		}
		return nil
	})
	return previousGTIDs, err
}

// Return the next chunk of binlog events; skip to next binary log file if need be; return empty result only
// if reached end of binary logs
func getNextBinlogEventsChunk(instance *mdtstruct.MysqlInstance, startingCoordinates dtstruct.LogCoordinates, numEmptyBinlogs int) ([]dtstruct.BinlogEvent, error) {
	if numEmptyBinlogs > maxEmptyBinlogFiles {
		log.Debugf("Reached maxEmptyBinlogFiles (%d) at %+v", maxEmptyBinlogFiles, startingCoordinates)
		// Give up and return empty results
		return []dtstruct.BinlogEvent{}, nil
	}
	coordinatesExceededCurrent := false
	switch startingCoordinates.Type {
	case gconstant.BinaryLog:
		coordinatesExceededCurrent = instance.SelfBinlogCoordinates.FileSmallerThan(&startingCoordinates)
	case gconstant.RelayLog:
		coordinatesExceededCurrent = instance.RelaylogCoordinates.FileSmallerThan(&startingCoordinates)
	}
	if coordinatesExceededCurrent {
		// We're past the last file. This is a non-error: there are no more events.
		log.Debugf("Coordinates overflow: %+v; terminating search", startingCoordinates)
		return []dtstruct.BinlogEvent{}, nil
	}
	events, err := readBinlogEventsChunk(&instance.Key, startingCoordinates)
	if err != nil {
		return events, err
	}
	if len(events) > 0 {
		log.Debugf("Returning %d events at %+v", len(events), startingCoordinates)
		return events, nil
	}

	// events are empty
	if nextCoordinates, err := instance.GetNextBinaryLog(startingCoordinates); err == nil {
		log.Debugf("Recursing into %+v", nextCoordinates)
		return getNextBinlogEventsChunk(instance, nextCoordinates, numEmptyBinlogs+1)
	}
	// on error
	return events, err
}

// Only do special filtering if instance is MySQL-5.7 and other
// is MySQL-5.6 and in pseudo-gtid mode.
// returns applyInstanceSpecialFiltering, applyOtherSpecialFiltering, err
func special56To57filterProcessing(instance *mdtstruct.MysqlInstance, other *mdtstruct.MysqlInstance) (bool, bool, error) {
	// be paranoid
	if instance == nil || other == nil {
		return false, false, fmt.Errorf("special56To57filterProcessing: instance or other is nil. Should not happen")
	}

	filterInstance := instance.FlavorNameAndMajorVersion() == "MySQL-5.7" && // 5.7 replica
		other.FlavorNameAndMajorVersion() == "MySQL-5.6" // replicating under 5.6 master

	// The logic for other is a bit weird and may require us
	// to check the instance's master.  To avoid this do some
	// preliminary checks first to avoid the "master" access
	// unless absolutely needed.
	if instance.LogBinEnabled || // instance writes binlogs (not relay logs)
		instance.FlavorNameAndMajorVersion() != "MySQL-5.7" || // instance NOT 5.7 replica
		other.FlavorNameAndMajorVersion() != "MySQL-5.7" { // new master is NOT 5.7
		return filterInstance, false /* good exit status avoiding checking master */, nil
	}

	// We need to check if the master is 5.6
	// - Do not call GetInstanceMaster() as that requires the
	//   master to be available, and this code may be called
	//   during a master/intermediate master failover when the
	//   master may not actually be reachable.
	master, _, err := ReadFromBackendDB(&instance.UpstreamKey)
	if err != nil {
		return false, false, log.Errorf("special56To57filterProcessing: ReadFromBackendDB(%+v) fails: %+v", instance.UpstreamKey, err)
	}

	filterOther := master.FlavorNameAndMajorVersion() == "MySQL-5.6" // master(instance) == 5.6

	return filterInstance, filterOther, nil
}

// Read (as much as possible of) a chunk of binary log events starting the given startingCoordinates
func readBinlogEventsChunk(instanceKey *dtstruct.InstanceKey, startingCoordinates dtstruct.LogCoordinates) ([]dtstruct.BinlogEvent, error) {
	events := []dtstruct.BinlogEvent{}
	db, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return events, err
	}
	commandToken := math.TernaryString(startingCoordinates.Type == gconstant.BinaryLog, "binlog", "relaylog")
	if startingCoordinates.LogFile == "" {
		return events, log.Errorf("readBinlogEventsChunk: empty binlog file name for %+v.", *instanceKey)
	}
	query := fmt.Sprintf("show %s events in '%s' FROM %d LIMIT %d", commandToken, startingCoordinates.LogFile, startingCoordinates.LogPos, config.Config.BinlogEventsChunkSize)
	err = sqlutil.QueryRowsMap(db, query, func(m sqlutil.RowMap) error {
		binlogEvent := dtstruct.BinlogEvent{}
		binlogEvent.Coordinates.LogFile = m.GetString("Log_name")
		binlogEvent.Coordinates.LogPos = m.GetInt64("Pos")
		binlogEvent.Coordinates.Type = startingCoordinates.Type
		binlogEvent.NextEventPos = m.GetInt64("End_log_pos")
		binlogEvent.EventType = m.GetString("Event_type")
		binlogEvent.Info = m.GetString("Info")

		events = append(events, binlogEvent)
		return nil
	})
	return events, err
}

// GetPreviousKnownRelayLogCoordinatesForInstance returns known relay log coordinates, that are not the
// exact current coordinates
func GetPreviousKnownRelayLogCoordinatesForInstance(instance *mdtstruct.MysqlInstance) (relayLogCoordinates *dtstruct.LogCoordinates, err error) {
	query := `
		select
			relay_log_file, relay_log_pos
		from
			mysql_database_instance_coordinate_history
		where
			hostname = ?
			and port = ?
			and (relay_log_file, relay_log_pos) < (?, ?)
			and relay_log_file != ''
			and relay_log_pos != 0
		order by
			record_timestamp desc
			limit 1
			`
	err = db.Query(query, sqlutil.Args(
		instance.Key.Hostname,
		instance.Key.Port,
		instance.RelaylogCoordinates.LogFile,
		instance.RelaylogCoordinates.LogPos,
	), func(m sqlutil.RowMap) error {
		relayLogCoordinates = &dtstruct.LogCoordinates{LogFile: m.GetString("relay_log_file"), LogPos: m.GetInt64("relay_log_pos")}

		return nil
	})
	return relayLogCoordinates, err
}
func ReadBinlogEventAtRelayLogCoordinates(instanceKey *dtstruct.InstanceKey, relaylogCoordinates *dtstruct.LogCoordinates) (binlogEvent *dtstruct.BinlogEvent, err error) {
	db, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf("show relaylog events in '%s' FROM %d LIMIT 1", relaylogCoordinates.LogFile, relaylogCoordinates.LogPos)
	binlogEvent = &dtstruct.BinlogEvent{
		Coordinates: *relaylogCoordinates,
	}
	err = sqlutil.QueryRowsMapBuffered(db, query, func(m sqlutil.RowMap) error {
		return readBinlogEvent(binlogEvent, m)
	})
	return binlogEvent, err
}

func readBinlogEvent(binlogEvent *dtstruct.BinlogEvent, m sqlutil.RowMap) error {
	binlogEvent.NextEventPos = m.GetInt64("End_log_pos")
	binlogEvent.Coordinates.LogPos = m.GetInt64("Pos")
	binlogEvent.EventType = m.GetString("Event_type")
	binlogEvent.Info = m.GetString("Info")
	return nil
}

// GetHeuristiclyRecentCoordinatesForInstance returns valid and reasonably recent coordinates for given instance.
func GetHeuristiclyRecentCoordinatesForInstance(instanceKey *dtstruct.InstanceKey) (selfCoordinates *dtstruct.LogCoordinates, relayLogCoordinates *dtstruct.LogCoordinates, err error) {
	query := `
		select
			binary_log_file, binary_log_pos, relay_log_file, relay_log_pos
		from
			mysql_database_instance_coordinate_history
		where
			hostname = ?
			and port = ?
			and record_timestamp <= NOW() - INTERVAL ? MINUTE
		order by
			record_timestamp desc
			limit 1
			`
	err = db.Query(query, sqlutil.Args(instanceKey.Hostname, instanceKey.Port, config.PseudoGTIDCoordinatesHistoryHeuristicMinutes), func(m sqlutil.RowMap) error {
		selfCoordinates = &dtstruct.LogCoordinates{LogFile: m.GetString("binary_log_file"), LogPos: m.GetInt64("binary_log_pos")}
		relayLogCoordinates = &dtstruct.LogCoordinates{LogFile: m.GetString("relay_log_file"), LogPos: m.GetInt64("relay_log_pos")}

		return nil
	})
	return selfCoordinates, relayLogCoordinates, err
}

// SearchEntryInInstanceBinlogs will search for a specific text entry within the binary logs of a given instance.
func SearchEntryInInstanceBinlogs(instance *mdtstruct.MysqlInstance, entryText string, monotonicPseudoGTIDEntries bool, minBinlogCoordinates *dtstruct.LogCoordinates) (*dtstruct.LogCoordinates, error) {
	pseudoGTIDRegexp, err := compilePseudoGTIDPattern()
	if err != nil {
		return nil, err
	}
	cacheKey := mutil.GetInstanceBinlogEntryKey(&instance.Key, entryText)
	coords, found := instanceBinlogEntryCache.Get(cacheKey)
	if found {
		// This is wonderful. We can skip the tedious GTID search in the binary log
		log.Debugf("Found instance Pseudo GTID entry coordinates in cache: %+v, %+v, %+v", instance.Key, entryText, coords)
		return coords.(*dtstruct.LogCoordinates), nil
	}

	// Look for GTID entry in given instance:
	log.Debugf("Searching for given pseudo gtid entry in %+v. monotonicPseudoGTIDEntries=%+v", instance.Key, monotonicPseudoGTIDEntries)
	currentBinlog := instance.SelfBinlogCoordinates
	err = nil
	for {
		log.Debugf("Searching for given pseudo gtid entry in binlog %+v of %+v", currentBinlog.LogFile, instance.Key)
		// loop iteration per binary log. This might turn to be a heavyweight operation. We wish to throttle the operation such that
		// the instance does not suffer. If it is a replica, we will only act as long as it's not lagging too much.
		if instance.ReplicaRunning() {
			for {
				log.Debugf("%+v is a replicating replica. Verifying lag", instance.Key)
				instance, err = GetInfoFromInstance(&instance.Key, false, false, nil, "")
				if err != nil {
					break
				}
				if instance.HasReasonableMaintenanceReplicationLag() {
					// is good to go!
					break
				}
				log.Debugf("lag is too high on %+v. Throttling the search for pseudo gtid entry", instance.Key)
				time.Sleep(time.Duration(config.Config.ReasonableMaintenanceReplicationLagSeconds) * time.Second)
			}
		}
		var resultCoordinates dtstruct.LogCoordinates
		var found bool = false
		resultCoordinates, found, err = SearchEntryInBinlog(pseudoGTIDRegexp, &instance.Key, currentBinlog.LogFile, entryText, monotonicPseudoGTIDEntries, minBinlogCoordinates)
		if err != nil {
			break
		}
		if found {
			log.Debugf("Matched entry in %+v: %+v", instance.Key, resultCoordinates)
			instanceBinlogEntryCache.Set(cacheKey, &resultCoordinates, 0)
			return &resultCoordinates, nil
		}
		// Got here? Unfound. Keep looking
		if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == currentBinlog.LogFile {
			log.Debugf("Heuristic master binary logs search failed; continuing exhaustive search")
			minBinlogCoordinates = nil
		} else {
			currentBinlog, err = currentBinlog.PreviousFileCoordinates()
			if err != nil {
				break
			}
			log.Debugf("- Will move next to binlog %+v", currentBinlog.LogFile)
		}
	}

	return nil, log.Errorf("Cannot match pseudo GTID entry in binlogs of %+v; err: %+v", instance.Key, err)
}

func compilePseudoGTIDPattern() (pseudoGTIDRegexp *regexp.Regexp, err error) {
	log.Debugf("PseudoGTIDPatternIsFixedSubstring: %+v", config.Config.PseudoGTIDPatternIsFixedSubstring)
	if config.Config.PseudoGTIDPatternIsFixedSubstring {
		return nil, nil
	}
	log.Debugf("Compiling PseudoGTIDPattern: %q", config.Config.PseudoGTIDPattern)
	return regexp.Compile(config.Config.PseudoGTIDPattern)
}

// pseudoGTIDMatches attempts to match given string with pseudo GTID pattern/text.
func pseudoGTIDMatches(pseudoGTIDRegexp *regexp.Regexp, binlogEntryInfo string) (found bool) {
	if config.Config.PseudoGTIDPatternIsFixedSubstring {
		return strings.Contains(binlogEntryInfo, config.Config.PseudoGTIDPattern)
	}
	return pseudoGTIDRegexp.MatchString(binlogEntryInfo)
}

// getLastPseudoGTIDEntryInInstance will search for the last pseudo GTID entry in an instance's binary logs. Arguments:
// - instance
// - minBinlogCoordinates: a hint, suggested coordinates to start with. The search will _attempt_ to begin search from
//   these coordinates, but if search is empty, then we failback to full search, ignoring this hint
// - maxBinlogCoordinates: a hard limit on the maximum position we're allowed to investigate.
// - exhaustiveSearch: when 'true', continue iterating binary logs. When 'false', only investigate most recent binary log.
func getLastPseudoGTIDEntryInInstance(instance *mdtstruct.MysqlInstance, minBinlogCoordinates *dtstruct.LogCoordinates, maxBinlogCoordinates *dtstruct.LogCoordinates, exhaustiveSearch bool) (*dtstruct.LogCoordinates, string, error) {
	pseudoGTIDRegexp, err := compilePseudoGTIDPattern()
	if err != nil {
		return nil, "", err
	}
	// Look for last GTID in instance:
	currentBinlog := instance.SelfBinlogCoordinates

	err = nil
	for err == nil {
		log.Debugf("Searching for latest pseudo gtid entry in binlog %+v of %+v", currentBinlog.LogFile, instance.Key)
		resultCoordinates, entryInfo, err := getLastPseudoGTIDEntryInBinlog(pseudoGTIDRegexp, &instance.Key, currentBinlog.LogFile, gconstant.BinaryLog, minBinlogCoordinates, maxBinlogCoordinates)
		if err != nil {
			return nil, "", err
		}
		if resultCoordinates != nil {
			log.Debugf("Found pseudo gtid entry in %+v, %+v", instance.Key, resultCoordinates)
			return resultCoordinates, entryInfo, err
		}
		if !exhaustiveSearch {
			log.Debugf("Not an exhaustive search. Bailing out")
			break
		}
		if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == currentBinlog.LogFile {
			// We tried and failed with the minBinlogCoordinates heuristic/hint. We no longer require it,
			// and continue with exhaustive search, on same binlog.
			minBinlogCoordinates = nil
			log.Debugf("Heuristic binlog search failed; continuing exhaustive search")
			// And we do NOT iterate the log file: we scan same log file again, with no heuristic
			//return nil, "", log.Errorf("past minBinlogCoordinates (%+v); skipping iteration over rest of binary logs", *minBinlogCoordinates)
		} else {
			currentBinlog, err = currentBinlog.PreviousFileCoordinates()
			if err != nil {
				return nil, "", err
			}
		}
	}
	return nil, "", log.Errorf("Cannot find pseudo GTID entry in binlogs of %+v", instance.Key)
}

// Try and find the last position of a pseudo GTID query entry in the given binary log.
// Also return the full text of that entry.
// maxCoordinates is the position beyond which we should not read. This is relevant when reading relay logs; in particular,
// the last relay log. We must be careful not to scan for Pseudo-GTID entries past the position executed by the SQL thread.
// maxCoordinates == nil means no limit.
func getLastPseudoGTIDEntryInBinlog(pseudoGTIDRegexp *regexp.Regexp, instanceKey *dtstruct.InstanceKey, binlog string, binlogType gconstant.LogType, minCoordinates *dtstruct.LogCoordinates, maxCoordinates *dtstruct.LogCoordinates) (*dtstruct.LogCoordinates, string, error) {
	if binlog == "" {
		return nil, "", log.Errorf("getLastPseudoGTIDEntryInBinlog: empty binlog file name for %+v. maxCoordinates = %+v", *instanceKey, maxCoordinates)
	}
	binlogCoordinates := dtstruct.LogCoordinates{LogFile: binlog, LogPos: 0, Type: binlogType}
	db, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, "", err
	}

	moreRowsExpected := true
	var nextPos int64 = 0
	var relyLogMinPos int64 = 0
	if minCoordinates != nil && minCoordinates.LogFile == binlog {
		log.Debugf("getLastPseudoGTIDEntryInBinlog: starting with %+v", *minCoordinates)
		nextPos = minCoordinates.LogPos
		relyLogMinPos = minCoordinates.LogPos
	}
	step := 0

	entryText := ""
	for moreRowsExpected {
		query := ""
		if binlogCoordinates.Type == gconstant.BinaryLog {
			query = fmt.Sprintf("show binlog events in '%s' FROM %d LIMIT %d", binlog, nextPos, config.Config.BinlogEventsChunkSize)
		} else {
			query = fmt.Sprintf("show relaylog events in '%s' FROM %d LIMIT %d,%d", binlog, relyLogMinPos, (step * config.Config.BinlogEventsChunkSize), config.Config.BinlogEventsChunkSize)
		}

		moreRowsExpected = false

		err = sqlutil.QueryRowsMapBuffered(db, query, func(m sqlutil.RowMap) error {
			moreRowsExpected = true
			nextPos = m.GetInt64("End_log_pos")
			binlogEntryInfo := m.GetString("Info")
			if pseudoGTIDMatches(pseudoGTIDRegexp, binlogEntryInfo) {
				if maxCoordinates != nil && maxCoordinates.SmallerThan(&dtstruct.LogCoordinates{LogFile: binlog, LogPos: m.GetInt64("Pos")}) {
					// past the limitation
					moreRowsExpected = false
					return nil
				}
				binlogCoordinates.LogPos = m.GetInt64("Pos")
				entryText = binlogEntryInfo
				// Found a match. But we keep searching: we're interested in the LAST entry, and, alas,
				// we can only search in ASCENDING order...
			}
			return nil
		})
		if err != nil {
			return nil, "", err
		}
		step++
	}

	// Not found? return nil. an error is reserved to SQL problems.
	if binlogCoordinates.LogPos == 0 {
		return nil, "", nil
	}
	return &binlogCoordinates, entryText, err
}

// SearchEntryInBinlog Given a binlog entry text (query), search it in the given binary log of a given instance
func SearchEntryInBinlog(pseudoGTIDRegexp *regexp.Regexp, instanceKey *dtstruct.InstanceKey, binlog string, entryText string, monotonicPseudoGTIDEntries bool, minBinlogCoordinates *dtstruct.LogCoordinates) (dtstruct.LogCoordinates, bool, error) {
	binlogCoordinates := dtstruct.LogCoordinates{LogFile: binlog, LogPos: 0, Type: gconstant.BinaryLog}
	if binlog == "" {
		return binlogCoordinates, false, log.Errorf("SearchEntryInBinlog: empty binlog file name for %+v", *instanceKey)
	}

	db, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return binlogCoordinates, false, err
	}

	moreRowsExpected := true
	skipRestOfBinlog := false
	alreadyMatchedAscendingPseudoGTID := false
	var nextPos int64 = 0
	if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == binlog {
		log.Debugf("SearchEntryInBinlog: starting with %+v", *minBinlogCoordinates)
		nextPos = minBinlogCoordinates.LogPos
	}

	for moreRowsExpected {
		query := fmt.Sprintf("show binlog events in '%s' FROM %d LIMIT %d", binlog, nextPos, config.Config.BinlogEventsChunkSize)

		// We don't know in advance when we will hit the end of the binlog. We will implicitly understand it when our
		// `show binlog events` query does not return any row.
		moreRowsExpected = false

		err = sqlutil.QueryRowsMapBuffered(db, query, func(m sqlutil.RowMap) error {
			if binlogCoordinates.LogPos != 0 {
				// Entry found!
				skipRestOfBinlog = true
				return nil
			}
			if skipRestOfBinlog {
				return nil
			}
			moreRowsExpected = true
			nextPos = m.GetInt64("End_log_pos")
			binlogEntryInfo := m.GetString("Info")
			//
			if binlogEntryInfo == entryText {
				// found it!
				binlogCoordinates.LogPos = m.GetInt64("Pos")
			} else if monotonicPseudoGTIDEntries && !alreadyMatchedAscendingPseudoGTID {
				// This part assumes we're searching for Pseudo-GTID.Typically that is the case, however this function can
				// also be used for generic searches through the binary log.
				// More heavyweight computation here. Need to verify whether the binlog entry we have is a pseudo-gtid entry
				// We only want to check for ASCENDING once in the top of the binary log.
				// If we find the first entry to be higher than the searched one, clearly we are done.
				// If not, then by virtue of binary logs, we still have to full-scan the entrie binlog sequentially; we
				// do not check again for ASCENDING (no point), so we save up CPU energy wasted in regexp.
				if pseudoGTIDMatches(pseudoGTIDRegexp, binlogEntryInfo) {
					alreadyMatchedAscendingPseudoGTID = true
					log.Debugf("Matched ascending Pseudo-GTID entry in %+v", binlog)
					if binlogEntryInfo > entryText {
						// Entries ascending, and current entry is larger than the one we are searching for.
						// There is no need to scan further on. We can skip the entire binlog
						log.Debugf(`Pseudo GTID entries are monotonic and we hit "%+v" > "%+v"; skipping binlog %+v`, m.GetString("Info"), entryText, binlogCoordinates.LogFile)
						skipRestOfBinlog = true
						return nil
					}
				}
			}
			return nil
		})
		if err != nil {
			return binlogCoordinates, (binlogCoordinates.LogPos != 0), err
		}
		if skipRestOfBinlog {
			return binlogCoordinates, (binlogCoordinates.LogPos != 0), err
		}
	}

	return binlogCoordinates, (binlogCoordinates.LogPos != 0), err
}

func SearchEventInRelayLogs(searchEvent *dtstruct.BinlogEvent, instance *mdtstruct.MysqlInstance, minBinlogCoordinates *dtstruct.LogCoordinates, recordedInstanceRelayLogCoordinates dtstruct.LogCoordinates) (binlogCoordinates, nextCoordinates *dtstruct.LogCoordinates, found bool, err error) {
	// Since MySQL does not provide with a SHOW RELAY LOGS command, we heuristically start from current
	// relay log (indiciated by Relay_log_file) and walk backwards.
	log.Debugf("will search for event %+v", *searchEvent)
	if minBinlogCoordinates != nil {
		log.Debugf("Starting with coordinates: %+v", *minBinlogCoordinates)
	}
	currentRelayLog := recordedInstanceRelayLogCoordinates
	for err == nil {
		log.Debugf("Searching for event in relaylog %+v of %+v, up to pos %+v", currentRelayLog.LogFile, instance.Key, recordedInstanceRelayLogCoordinates)
		if binlogCoordinates, nextCoordinates, found, err = searchEventInRelaylog(&instance.Key, currentRelayLog.LogFile, searchEvent, minBinlogCoordinates); err != nil {
			return nil, nil, false, err
		} else if binlogCoordinates != nil && found {
			log.Debugf("Found event in %+v, %+v", instance.Key, *binlogCoordinates)
			return binlogCoordinates, nextCoordinates, found, err
		}
		if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == currentRelayLog.LogFile {
			// We tried and failed with the minBinlogCoordinates hint. We no longer require it,
			// and continue with exhaustive search.
			minBinlogCoordinates = nil
			log.Debugf("Heuristic relaylog search failed; continuing exhaustive search")
			// And we do NOT iterate to previous log file: we scan same log faile again, with no heuristic
		} else {
			currentRelayLog, err = currentRelayLog.PreviousFileCoordinates()
		}
	}
	return binlogCoordinates, nextCoordinates, found, err
}

// SearchBinlogEntryInRelaylog
func searchEventInRelaylog(instanceKey *dtstruct.InstanceKey, binlog string, searchEvent *dtstruct.BinlogEvent, minCoordinates *dtstruct.LogCoordinates) (binlogCoordinates, nextCoordinates *dtstruct.LogCoordinates, found bool, err error) {
	binlogCoordinates = &dtstruct.LogCoordinates{LogFile: binlog, LogPos: 0, Type: gconstant.RelayLog}
	nextCoordinates = &dtstruct.LogCoordinates{LogFile: binlog, LogPos: 0, Type: gconstant.RelayLog}
	if binlog == "" {
		return binlogCoordinates, nextCoordinates, false, log.Errorf("SearchEventInRelaylog: empty relaylog file name for %+v", *instanceKey)
	}

	db, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return binlogCoordinates, nextCoordinates, false, err
	}

	moreRowsExpected := true
	var relyLogMinPos int64 = 0
	if minCoordinates != nil && minCoordinates.LogFile == binlog {
		log.Debugf("SearchEventInRelaylog: starting with %+v", *minCoordinates)
		relyLogMinPos = minCoordinates.LogPos
	}
	binlogEvent := &dtstruct.BinlogEvent{
		Coordinates: dtstruct.LogCoordinates{LogFile: binlog, LogPos: 0, Type: gconstant.RelayLog},
	}

	skipRestOfBinlog := false

	step := 0
	for moreRowsExpected {
		query := fmt.Sprintf("show relaylog events in '%s' FROM %d LIMIT %d,%d", binlog, relyLogMinPos, (step * config.Config.BinlogEventsChunkSize), config.Config.BinlogEventsChunkSize)

		// We don't know in advance when we will hit the end of the binlog. We will implicitly understand it when our
		// `show binlog events` query does not return any row.
		moreRowsExpected = false
		err = sqlutil.QueryRowsMapBuffered(db, query, func(m sqlutil.RowMap) error {
			if binlogCoordinates.LogPos != 0 && nextCoordinates.LogPos != 0 {
				// Entry found!
				skipRestOfBinlog = true
				return nil
			}
			if skipRestOfBinlog {
				return nil
			}
			moreRowsExpected = true

			if binlogCoordinates.LogPos == 0 {
				readBinlogEvent(binlogEvent, m)
				if binlogEvent.EqualsIgnoreCoordinates(searchEvent) {
					// found it!
					binlogCoordinates.LogPos = m.GetInt64("Pos")
				}
			} else if nextCoordinates.LogPos == 0 {
				// found binlogCoordinates: the next coordinates are nextCoordinates :P
				nextCoordinates.LogPos = m.GetInt64("Pos")
			}
			return nil
		})
		if err != nil {
			return binlogCoordinates, nextCoordinates, (binlogCoordinates.LogPos != 0), err
		}
		if skipRestOfBinlog {
			return binlogCoordinates, nextCoordinates, (binlogCoordinates.LogPos != 0), err
		}
		step++
	}
	return binlogCoordinates, nextCoordinates, (binlogCoordinates.LogPos != 0), err
}

func getLastPseudoGTIDEntryInRelayLogs(instance *mdtstruct.MysqlInstance, minBinlogCoordinates *dtstruct.LogCoordinates, recordedInstanceRelayLogCoordinates dtstruct.LogCoordinates, exhaustiveSearch bool) (*dtstruct.LogCoordinates, string, error) {
	// Look for last GTID in relay logs:
	// Since MySQL does not provide with a SHOW RELAY LOGS command, we heuristically start from current
	// relay log (indiciated by Relay_log_file) and walk backwards.
	// Eventually we will hit a relay log name which does not exist.
	pseudoGTIDRegexp, err := compilePseudoGTIDPattern()
	if err != nil {
		return nil, "", err
	}

	currentRelayLog := recordedInstanceRelayLogCoordinates
	err = nil
	for err == nil {
		log.Debugf("Searching for latest pseudo gtid entry in relaylog %+v of %+v, up to pos %+v", currentRelayLog.LogFile, instance.Key, recordedInstanceRelayLogCoordinates)
		if resultCoordinates, entryInfo, err := getLastPseudoGTIDEntryInBinlog(pseudoGTIDRegexp, &instance.Key, currentRelayLog.LogFile, gconstant.RelayLog, minBinlogCoordinates, &recordedInstanceRelayLogCoordinates); err != nil {
			return nil, "", err
		} else if resultCoordinates != nil {
			log.Debugf("Found pseudo gtid entry in %+v, %+v", instance.Key, resultCoordinates)
			return resultCoordinates, entryInfo, err
		}
		if !exhaustiveSearch {
			break
		}
		if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == currentRelayLog.LogFile {
			// We tried and failed with the minBinlogCoordinates hint. We no longer require it,
			// and continue with exhaustive search.
			minBinlogCoordinates = nil
			log.Debugf("Heuristic relaylog search failed; continuing exhaustive search")
			// And we do NOT iterate to previous log file: we scan same log file again, with no heuristic
		} else {
			currentRelayLog, err = currentRelayLog.PreviousFileCoordinates()
		}
	}
	return nil, "", log.Errorf("Cannot find pseudo GTID entry in relay logs of %+v", instance.Key)
}

// Try and find the last position of a pseudo GTID query entry in the given binary log.
// Also return the full text of that entry.
// maxCoordinates is the position beyond which we should not read. This is relevant when reading relay logs; in particular,
// the last relay log. We must be careful not to scan for Pseudo-GTID entries past the position executed by the SQL thread.
// maxCoordinates == nil means no limit.
func getLastExecutedEntryInRelaylog(instanceKey *dtstruct.InstanceKey, binlog string, minCoordinates *dtstruct.LogCoordinates, maxCoordinates *dtstruct.LogCoordinates) (binlogEvent *dtstruct.BinlogEvent, err error) {
	if binlog == "" {
		return nil, log.Errorf("getLastExecutedEntryInRelaylog: empty binlog file name for %+v. maxCoordinates = %+v", *instanceKey, maxCoordinates)
	}
	db, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, err
	}
	binlogEvent = &dtstruct.BinlogEvent{
		Coordinates: dtstruct.LogCoordinates{LogFile: binlog, LogPos: 0, Type: gconstant.RelayLog},
	}

	moreRowsExpected := true
	var relyLogMinPos int64 = 0
	if minCoordinates != nil && minCoordinates.LogFile == binlog {
		log.Debugf("getLastExecutedEntryInRelaylog: starting with %+v", *minCoordinates)
		relyLogMinPos = minCoordinates.LogPos
	}

	step := 0
	for moreRowsExpected {
		query := fmt.Sprintf("show relaylog events in '%s' FROM %d LIMIT %d,%d", binlog, relyLogMinPos, (step * config.Config.BinlogEventsChunkSize), config.Config.BinlogEventsChunkSize)

		moreRowsExpected = false
		err = sqlutil.QueryRowsMapBuffered(db, query, func(m sqlutil.RowMap) error {
			moreRowsExpected = true
			return readBinlogEvent(binlogEvent, m)
		})
		if err != nil {
			return nil, err
		}
		step++
	}

	// Not found? return nil. an error is reserved to SQL problems.
	if binlogEvent.Coordinates.LogPos == 0 {
		return nil, nil
	}
	return binlogEvent, err
}

func GetLastExecutedEntryInRelayLogs(instance *mdtstruct.MysqlInstance, minBinlogCoordinates *dtstruct.LogCoordinates, recordedInstanceRelayLogCoordinates dtstruct.LogCoordinates) (binlogEvent *dtstruct.BinlogEvent, err error) {
	// Look for last GTID in relay logs:
	// Since MySQL does not provide with a SHOW RELAY LOGS command, we heuristically start from current
	// relay log (indiciated by Relay_log_file) and walk backwards.

	currentRelayLog := recordedInstanceRelayLogCoordinates
	for err == nil {
		log.Debugf("Searching for latest entry in relaylog %+v of %+v, up to pos %+v", currentRelayLog.LogFile, instance.Key, recordedInstanceRelayLogCoordinates)
		if binlogEvent, err = getLastExecutedEntryInRelaylog(&instance.Key, currentRelayLog.LogFile, minBinlogCoordinates, &recordedInstanceRelayLogCoordinates); err != nil {
			return nil, err
		} else if binlogEvent != nil {
			log.Debugf("Found entry in %+v, %+v", instance.Key, binlogEvent.Coordinates)
			return binlogEvent, err
		}
		if minBinlogCoordinates != nil && minBinlogCoordinates.LogFile == currentRelayLog.LogFile {
			// We tried and failed with the minBinlogCoordinates hint. We no longer require it,
			// and continue with exhaustive search.
			minBinlogCoordinates = nil
			log.Debugf("Heuristic relaylog search failed; continuing exhaustive search")
			// And we do NOT iterate to previous log file: we scan same log faile again, with no heuristic
		} else {
			currentRelayLog, err = currentRelayLog.PreviousFileCoordinates()
		}
	}
	return binlogEvent, err
}

// ChangeMasterCredentials issues a CHANGE MASTER TO... MASTER_USER=, MASTER_PASSWORD=...
func ChangeMasterCredentials(instanceKey *dtstruct.InstanceKey, creds *dtstruct.ReplicationCredentials) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}
	if creds.User == "" {
		return instance, log.Errorf("Empty user in ChangeMasterCredentials() for %+v", *instanceKey)
	}

	if instance.ReplicationThreadsExist() && !instance.ReplicationThreadsStopped() {
		return instance, fmt.Errorf("ChangeMasterTo: Cannot change master on: %+v because replication is running", *instanceKey)
	}
	log.Debugf("ChangeMasterTo: will attempt changing master credentials on %+v", *instanceKey)

	if *dtstruct.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting CHANGE MASTER TO operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	var query_params []string
	var query_params_args []interface{}

	// User
	query_params = append(query_params, "master_user = ?")
	query_params_args = append(query_params_args, creds.User)
	// Password
	if creds.Password != "" {
		query_params = append(query_params, "master_password = ?")
		query_params_args = append(query_params_args, creds.Password)
	}

	// SSL CA cert
	if creds.SSLCaCert != "" {
		query_params = append(query_params, "master_ssl_ca = ?")
		query_params_args = append(query_params_args, creds.SSLCaCert)
	}
	// SSL cert
	if creds.SSLCert != "" {
		query_params = append(query_params, "master_ssl_cert = ?")
		query_params_args = append(query_params_args, creds.SSLCert)
	}
	// SSL key
	if creds.SSLKey != "" {
		query_params = append(query_params, "master_ssl = 1")
		query_params = append(query_params, "master_ssl_key = ?")
		query_params_args = append(query_params_args, creds.SSLKey)
	}

	query := fmt.Sprintf("change master to %s", strings.Join(query_params, ", "))
	_, err = ExecSQLOnInstance(instanceKey, query, query_params_args...)

	if err != nil {
		return instance, log.Errore(err)
	}

	log.Infof("ChangeMasterTo: Changed master credentials on %+v", *instanceKey)

	instance, err = GetInfoFromInstance(instanceKey, false, false, nil, "")
	return instance, err
}

// SkipQuery skip a single query in a failed replication instance
func SkipQuery(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", instanceKey)
	}
	if instance.ReplicationSQLThreadRuning {
		return instance, fmt.Errorf("Replication SQL thread is running on %+v", instanceKey)
	}
	if instance.LastSQLError == "" {
		return instance, fmt.Errorf("No SQL error on %+v", instanceKey)
	}

	if *dtstruct.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting skip-query operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	log.Debugf("Skipping one query on %+v", instanceKey)
	if instance.UsingOracleGTID {
		err = skipQueryOracleGtid(instance)
	} else if instance.UsingMariaDBGTID {
		return instance, log.Errorf("%+v is replicating with MariaDB GTID. To skip a query first disable GTID, then skip, then enable GTID again", *instanceKey)
	} else {
		err = skipQueryClassic(instance)
	}
	if err != nil {
		return instance, log.Errore(err)
	}
	base.AuditOperation("skip-query", instanceKey, instance.ClusterName, "Skipped one query")
	return StartReplication(context.TODO(), instanceKey)
}

// skipQueryClassic skips a query in normal binlog file:pos replication
func skipQueryClassic(instance *mdtstruct.MysqlInstance) error {
	_, err := ExecSQLOnInstance(&instance.Key, `set global sql_slave_skip_counter := 1`)
	return err
}

// skipQueryOracleGtid skips a single query in an Oracle GTID replicating replica, by injecting an empty transaction
func skipQueryOracleGtid(instance *mdtstruct.MysqlInstance) error {
	nextGtid, err := instance.NextGTID()
	if err != nil {
		return err
	}
	if nextGtid == "" {
		return fmt.Errorf("Empty NextGTID() in skipQueryGtid() for %+v", instance.Key)
	}
	if _, err := ExecSQLOnInstance(&instance.Key, `SET GTID_NEXT=?`, nextGtid); err != nil {
		return err
	}
	if err := EmptyCommitInstance(&instance.Key); err != nil {
		return err
	}
	if _, err := ExecSQLOnInstance(&instance.Key, `SET GTID_NEXT='AUTOMATIC'`); err != nil {
		return err
	}
	return nil
}

// ReattachMaster reattaches a replica back onto its master by undoing a DetachMaster operation
func ReattachMaster(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", *instanceKey)
	}
	if !instance.UpstreamKey.IsDetached() {
		return instance, fmt.Errorf("instance does not seem to be detached: %+v", *instanceKey)
	}

	reattachedMasterKey := instance.UpstreamKey.ReattachedKey()

	log.Infof("Will reattach master host on %+v. Reattached key is %+v", *instanceKey, *reattachedMasterKey)

	if maintenanceToken, merr := base.BeginMaintenance(instanceKey, dtstruct.GetMaintenanceOwner(), "reattach-replica-master-host"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer base.EndMaintenance(maintenanceToken)
	}

	instance, err = StopReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

	instance, err = ChangeMasterTo(instanceKey, reattachedMasterKey, &instance.ExecBinlogCoordinates, true, constant.GTIDHintNeutral)
	if err != nil {
		goto Cleanup
	}
	// Just in case this instance used to be a master:
	base.ReplaceAliasClusterName(instanceKey.StringCode(), reattachedMasterKey.StringCode())

Cleanup:
	instance, _ = StartReplication(context.TODO(), instanceKey)
	if err != nil {
		return instance, log.Errore(err)
	}
	// and we're done (pending deferred functions)
	base.AuditOperation("repoint", instanceKey, instance.ClusterName, fmt.Sprintf("replica %+v reattached to master %+v", *instanceKey, *reattachedMasterKey))

	return instance, err
}

// MasterPosWait issues a MASTER_POS_WAIT() an given instance according to given coordinates.
func MasterPosWait(instanceKey *dtstruct.InstanceKey, binlogCoordinates *dtstruct.LogCoordinates) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}

	_, err = ExecSQLOnInstance(instanceKey, `select master_pos_wait(?, ?)`, binlogCoordinates.LogFile, binlogCoordinates.LogPos)
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Instance %+v has reached coordinates: %+v", instanceKey, binlogCoordinates)

	instance, err = GetInfoFromInstance(instanceKey, false, false, nil, "")
	return instance, err
}

func SetSemiSyncMaster(instanceKey *dtstruct.InstanceKey, enableMaster bool) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	if _, err := ExecSQLOnInstance(instanceKey, "set @@global.rpl_semi_sync_master_enabled=?", enableMaster); err != nil {
		return instance, log.Errore(err)
	}
	return GetInfoFromInstance(instanceKey, false, false, nil, "")
}

func SetSemiSyncReplica(instanceKey *dtstruct.InstanceKey, enableReplica bool) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}
	if instance.SemiSyncReplicaEnabled == enableReplica {
		return instance, nil
	}
	if _, err := ExecSQLOnInstance(instanceKey, "set @@global.rpl_semi_sync_slave_enabled=?", enableReplica); err != nil {
		return instance, log.Errore(err)
	}
	if instance.ReplicationIOThreadRuning {
		// Need to apply change by stopping starting IO thread
		ExecSQLOnInstance(instanceKey, "stop slave io_thread")
		if _, err := ExecSQLOnInstance(instanceKey, "start slave io_thread"); err != nil {
			return instance, log.Errore(err)
		}
	}
	return GetInfoFromInstance(instanceKey, false, false, nil, "")
}

// GetReplicationRestartPreserveStatements returns a sequence of statements that make sure a replica is stopped
// and then returned to the same state. For example, if the replica was fully running, this will issue
// a STOP on both io_thread and sql_thread, followed by START on both. If one of them is not running
// at the time this function is called, said thread will be neither stopped nor started.
// The caller may provide an injected statememt, to be executed while the replica is stopped.
// This is useful for CHANGE MASTER TO commands, that unfortunately must take place while the replica
// is completely stopped.
func GetReplicationRestartPreserveStatements(instanceKey *dtstruct.InstanceKey, injectedStatement string) (statements []string, err error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return statements, err
	}
	if instance.ReplicationIOThreadRuning {
		statements = append(statements, util.SemicolonTerminated(`stop slave io_thread`))
	}
	if instance.ReplicationSQLThreadRuning {
		statements = append(statements, util.SemicolonTerminated(`stop slave sql_thread`))
	}
	if injectedStatement != "" {
		statements = append(statements, util.SemicolonTerminated(injectedStatement))
	}
	if instance.ReplicationSQLThreadRuning {
		statements = append(statements, util.SemicolonTerminated(`start slave sql_thread`))
	}
	if instance.ReplicationIOThreadRuning {
		statements = append(statements, util.SemicolonTerminated(`start slave io_thread`))
	}
	return statements, err
}

// Topology returns a string representation of the topology of given cluster.
func Topology(request *dtstruct.Request, historyTimestampPattern string, tabulated bool, printTags bool) (result interface{}, err error) {
	fillerCharacter := asciiFillerCharacter
	var instances []*mdtstruct.MysqlInstance
	if historyTimestampPattern == "" {
		instances, err = ReadClusterInstancesByClusterIdOrHint(request)
	} else {
		instances, err = ReadHistoryClusterInstancesByClusterIdOrHint(request, historyTimestampPattern)
	}
	if err != nil {
		return "", err
	}

	instancesMap := make(map[dtstruct.InstanceKey](*mdtstruct.MysqlInstance))
	for _, instance := range instances {
		log.Debugf("instanceKey: %+v", instance.Key)
		instancesMap[instance.Key] = instance
	}

	replicationMap := make(map[*mdtstruct.MysqlInstance]([]*mdtstruct.MysqlInstance))
	var masterInstance *mdtstruct.MysqlInstance
	// Investigate replicas:
	for _, instance := range instances {
		var masterOrGroupPrimary *mdtstruct.MysqlInstance
		var ok bool
		// If the current instance is a a group member, get the group's primary instead of the classical replication
		// source.
		if instance.IsReplicationGroupMember() && instance.IsReplicationGroupSecondary() {
			masterOrGroupPrimary, ok = instancesMap[instance.ReplicationGroupPrimaryInstanceKey]
		} else {
			masterOrGroupPrimary, ok = instancesMap[instance.UpstreamKey]
		}
		if ok {
			if _, ok := replicationMap[masterOrGroupPrimary]; !ok {
				replicationMap[masterOrGroupPrimary] = [](*mdtstruct.MysqlInstance){}
			}
			if !instance.IsReplicationGroupPrimary() || (instance.IsReplicationGroupPrimary() && instance.IsReplica()) {
				replicationMap[masterOrGroupPrimary] = append(replicationMap[masterOrGroupPrimary], instance)
			}
		} else {
			masterInstance = instance
		}
	}
	// Get entries:
	var entries []string
	if masterInstance != nil {
		// Single master
		entries = getASCIITopologyEntry(0, masterInstance, replicationMap, historyTimestampPattern == "", fillerCharacter, tabulated, printTags)
	} else {
		// Co-masters? For visualization we put each in its own branch while ignoring its other co-masters.
		for _, instance := range instances {
			if instance.IsCoUpstream {
				entries = append(entries, getASCIITopologyEntry(1, instance, replicationMap, historyTimestampPattern == "", fillerCharacter, tabulated, printTags)...)
			}
		}
	}
	// Beautify: make sure the "[...]" part is nicely aligned for all instances.
	if tabulated {
		entries = gutil.Tabulate(entries, "|", "|", gutil.TabulateLeft, gutil.TabulateRight)
	} else {
		indentationCharacter := "["
		maxIndent := 0
		for _, entry := range entries {
			maxIndent = math.MaxInt(maxIndent, strings.Index(entry, indentationCharacter))
		}
		for i, entry := range entries {
			entryIndent := strings.Index(entry, indentationCharacter)
			if maxIndent > entryIndent {
				tokens := strings.SplitN(entry, indentationCharacter, 2)
				newEntry := fmt.Sprintf("%s%s%s%s", tokens[0], strings.Repeat(fillerCharacter, maxIndent-entryIndent), indentationCharacter, tokens[1])
				entries[i] = newEntry
			}
		}
	}
	// Turn into string
	result = strings.Join(entries, "\n")
	return result, nil
}

// getASCIITopologyEntry will get an ascii topology tree rooted at given instance. Ir recursively
// draws the tree
func getASCIITopologyEntry(depth int, instance *mdtstruct.MysqlInstance, replicationMap map[*mdtstruct.MysqlInstance]([]*mdtstruct.MysqlInstance), extendedOutput bool, fillerCharacter string, tabulated bool, printTags bool) []string {
	if instance == nil {
		return []string{}
	}
	if instance.IsCoUpstream && depth > 1 {
		return []string{}
	}
	prefix := ""
	if depth > 0 {
		prefix = strings.Repeat(fillerCharacter, (depth-1)*2)
		if instance.IsReplicationGroupSecondary() {
			prefix += "‡" + fillerCharacter
		} else {
			if instance.ReplicaRunning() && instance.IsLastCheckValid && instance.IsRecentlyChecked {
				prefix += "+" + fillerCharacter
			} else {
				prefix += "-" + fillerCharacter
			}
		}
	}
	entryAlias := ""
	if instance.InstanceAlias != "" {
		entryAlias = fmt.Sprintf(" (%s)", instance.InstanceAlias)
	}
	entry := fmt.Sprintf("%s%s%s", prefix, instance.Key.DisplayString(), entryAlias)
	if extendedOutput {
		if tabulated {
			entry = fmt.Sprintf("%s%s%s", entry, tabulatorScharacter, instance.TabulatedDescription(tabulatorScharacter))
		} else {
			entry = fmt.Sprintf("%s%s%s", entry, fillerCharacter, instance.HumanReadableDescription())
		}
		if printTags {
			tags, _ := base.ReadInstanceTags(&instance.Key)
			tagsString := make([]string, len(tags))
			for idx, tag := range tags {
				tagsString[idx] = tag.Display()
			}
			entry = fmt.Sprintf("%s [%s]", entry, strings.Join(tagsString, ","))
		}
	}
	result := []string{entry}
	for _, replica := range replicationMap[instance] {
		replicasResult := getASCIITopologyEntry(depth+1, replica, replicationMap, extendedOutput, fillerCharacter, tabulated, printTags)
		result = append(result, replicasResult...)
	}
	return result
}

// DelayReplication set the replication delay given seconds
// keeping the current state of the replication threads.
func DelayReplication(instanceKey *dtstruct.InstanceKey, seconds int) error {
	if seconds < 0 {
		return fmt.Errorf("invalid seconds: %d, it should be greater or equal to 0", seconds)
	}
	query := fmt.Sprintf("change master to master_delay=%d", seconds)
	statements, err := GetReplicationRestartPreserveStatements(instanceKey, query)
	if err != nil {
		return err
	}
	for _, cmd := range statements {
		if _, err := ExecSQLOnInstance(instanceKey, cmd); err != nil {
			return log.Errorf("%+v: DelayReplication: '%q' failed: %+v", *instanceKey, cmd, err)
		} else {
			log.Infof("DelayReplication: %s on %+v", cmd, *instanceKey)
		}
	}

	clusterName, _ := base.GetClusterName(instanceKey)
	base.AuditOperation("delay-replication", instanceKey, clusterName, fmt.Sprintf("set to %d", seconds))
	return nil
}

func WaitForExecBinlogCoordinatesToReach(instanceKey *dtstruct.InstanceKey, coordinates *dtstruct.LogCoordinates, maxWait time.Duration) (instance *mdtstruct.MysqlInstance, exactMatch bool, err error) {
	startTime := time.Now()
	for {
		if maxWait != 0 && time.Since(startTime) > maxWait {
			return nil, exactMatch, fmt.Errorf("WaitForExecBinlogCoordinatesToReach: reached maxWait %+v on %+v", maxWait, *instanceKey)
		}
		instance, err = GetInfoFromInstance(instanceKey, false, false, nil, "")
		if err != nil {
			return instance, exactMatch, log.Errore(err)
		}

		switch {
		case instance.ExecBinlogCoordinates.SmallerThan(coordinates):
			time.Sleep(gconstant.RetryInterval)
		case instance.ExecBinlogCoordinates.Equals(coordinates):
			return instance, true, nil
		case coordinates.SmallerThan(&instance.ExecBinlogCoordinates):
			return instance, false, nil
		}
	}
	return instance, exactMatch, err
}

// EnableMasterSSL issues CHANGE MASTER TO MASTER_SSL=1
func EnableMasterSSL(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.ReplicationThreadsExist() && !instance.ReplicationThreadsStopped() {
		return instance, fmt.Errorf("EnableMasterSSL: Cannot enable SSL replication on %+v because replication threads are not stopped", *instanceKey)
	}
	log.Debugf("EnableMasterSSL: Will attempt enabling SSL replication on %+v", *instanceKey)

	if *dtstruct.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting CHANGE MASTER TO MASTER_SSL=1 operation on %+v; signaling error but nothing went wrong.", *instanceKey)
	}
	_, err = ExecSQLOnInstance(instanceKey, "change master to master_ssl=1")

	if err != nil {
		return instance, log.Errore(err)
	}

	log.Infof("EnableMasterSSL: Enabled SSL replication on %+v", *instanceKey)

	instance, err = GetInfoFromInstance(instanceKey, false, false, nil, "")
	return instance, err
}

// getPriorityBinlogFormatForCandidate returns the primary (most common) binlog format found
// among given instances. This will be used for choosing best candidate for promotion.
func GetPriorityBinlogFormatForCandidate(replicas [](*mdtstruct.MysqlInstance)) (priorityBinlogFormat string, err error) {
	if len(replicas) == 0 {
		return "", log.Errorf("empty replicas list in getPriorityBinlogFormatForCandidate")
	}
	binlogFormatsCount := make(map[string]int)
	for _, replica := range replicas {
		binlogFormatsCount[replica.Binlog_format] = binlogFormatsCount[replica.Binlog_format] + 1
	}
	if len(binlogFormatsCount) == 1 {
		// all same binlog format, simple case
		return replicas[0].Binlog_format, nil
	}
	sorted := dtstruct.NewBinlogFormatSortedByCount(replicas[0].Key.DBType, binlogFormatsCount)
	sort.Sort(sort.Reverse(sorted))
	return sorted.First(), nil
}

func ShouldPostponeRelocatingReplica(replica *mdtstruct.MysqlInstance, postponedFunctionsContainer *dtstruct.PostponedFunctionsContainer) bool {
	if postponedFunctionsContainer == nil {
		return false
	}
	if config.Config.PostponeReplicaRecoveryOnLagMinutes > 0 &&
		replica.SQLDelay > config.Config.PostponeReplicaRecoveryOnLagMinutes*60 {
		// This replica is lagging very much, AND
		// we're configured to postpone operation on this replica so as not to delay everyone else.
		return true
	}
	if replica.GetInstance().LastDiscoveryLatency > dtstruct.ReasonableDiscoveryLatency {
		return true
	}
	return false
}

func ScanInstanceRow(instanceKey *dtstruct.InstanceKey, query string, dest ...interface{}) error {
	mdb, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return err
	}
	err = mdb.QueryRow(query).Scan(dest...)
	return err
}

func ExecSQLOnInstance(instanceKey *dtstruct.InstanceKey, query string, args ...interface{}) (sql.Result, error) {
	mdb, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return nil, err
	}
	return sqlutil.ExecNoPrepare(mdb, query, args...)
}

func OpenTopology(host string, port int, args ...interface{}) (db *sql.DB, err error) {
	uri := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=%ds&readTimeout=%ds&interpolateParams=true",
		config.Config.MysqlTopologyUser,
		config.Config.MysqlTopologyPassword,
		host, port,
		config.Config.ConnectTimeoutSeconds,
		config.Config.MySQLTopologyReadTimeoutSeconds,
	)
	if config.Config.MySQLTopologyUseMutualTLS || (config.Config.MySQLTopologyUseMixedTLS && requiresTLS(host, port, uri)) {
		if uri, err = SetupTopologyTLS(uri); err != nil {
			return nil, err
		}
	}
	if db, _, err = sqlutil.GetGenericDB("mysql", uri); err != nil {
		return nil, err
	}
	if config.Config.ConnectionLifetimeSeconds > 0 {
		db.SetConnMaxLifetime(time.Duration(config.Config.ConnectionLifetimeSeconds) * time.Second)
	}
	db.SetMaxOpenConns(config.TopologyMaxPoolConnections)
	db.SetMaxIdleConns(config.TopologyMaxPoolConnections)
	return db, err
}

// Create a TLS configuration from the config supplied CA, Certificate, and Private key.
// Register the TLS config with the mysql drivers as the "topology" config
// Modify the supplied URI to call the TLS config
func SetupTopologyTLS(uri string) (string, error) {
	if !topologyTLSConfigured {
		tlsConfig, err := ssl.NewTLSConfig(config.Config.MySQLTopologySSLCAFile, !config.Config.MySQLTopologySSLSkipVerify)
		// Drop to TLS 1.0 for talking to MySQL
		tlsConfig.MinVersion = tls.VersionTLS10
		if err != nil {
			return "", log.Errorf("Can't create TLS configuration for Topology connection %s: %s", uri, err)
		}
		tlsConfig.InsecureSkipVerify = config.Config.MySQLTopologySSLSkipVerify

		if (config.Config.MySQLTopologyUseMutualTLS && !config.Config.MySQLTopologySSLSkipVerify) &&
			config.Config.MySQLTopologySSLCertFile != "" &&
			config.Config.MySQLTopologySSLPrivateKeyFile != "" {
			if err = ssl.AppendKeyPair(tlsConfig, config.Config.MySQLTopologySSLCertFile, config.Config.MySQLTopologySSLPrivateKeyFile); err != nil {
				return "", log.Errorf("Can't setup TLS key pairs for %s: %s", uri, err)
			}
		}
		if err = mysql.RegisterTLSConfig("topology", tlsConfig); err != nil {
			return "", log.Errorf("Can't register TLS config for topology: %s", err)
		}
		topologyTLSConfigured = true
	}
	return fmt.Sprintf("%s&tls=topology", uri), nil
}

func requiresTLS(host string, port int, uri string) bool {
	cacheKey := fmt.Sprintf("%s:%d", host, port)

	if value, found := requireTLSCache.Get(cacheKey); found {
		readInstanceTLSCacheCounter.Inc(1)
		return value.(bool)
	}

	required := TLSCheck(uri)

	query := `
			insert into
				ham_database_instance_tls (
					hostname, port, required
				) values (
					?, ?, ?
				)
				on duplicate key update
					required=values(required)
				`
	if _, err := db.ExecSQL(query, host, port, required); err != nil {
		log.Errore(err)
	}
	writeInstanceTLSCounter.Inc(1)

	requireTLSCache.Set(cacheKey, required, cache.DefaultExpiration)
	writeInstanceTLSCacheCounter.Inc(1)

	return required
}

func TLSCheck(uri string) bool {
	db, _, _ := sqlutil.GetDB(uri)
	if err := db.Ping(); err != nil && (strings.Contains(err.Error(), constant.Error3159) || strings.Contains(err.Error(), constant.Error1045)) {
		return true
	}
	return false
}

func EmptyCommitInstance(instanceKey *dtstruct.InstanceKey) error {
	mdb, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return err
	}
	tx, err := mdb.Begin()
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return err
}
