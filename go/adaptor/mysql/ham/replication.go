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
	"fmt"
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/common/constant"
	mdtstruct "gitee.com/opengauss/ham4db/go/adaptor/mysql/dtstruct"
	"gitee.com/opengauss/ham4db/go/common"
	cconstant "gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"strings"
	"time"
)

func RestartReplicationQuick(instanceKey *dtstruct.InstanceKey) error {
	for _, cmd := range []string{`stop slave io_thread`, `start slave io_thread`} {
		if _, err := ExecSQLOnInstance(instanceKey, cmd); err != nil {
			return log.Errorf("%+v: RestartReplicationQuick: '%q' failed: %+v", *instanceKey, cmd, err)
		} else {
			log.Infof("%s on %+v as part of RestartReplicationQuick", cmd, *instanceKey)
		}
	}
	return nil
}

// ResetMaster issues a RESET MASTER statement on given instance. Use with extreme care!
func ResetMaster(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.ReplicationThreadsExist() && !instance.ReplicationThreadsStopped() {
		return instance, fmt.Errorf("Cannot reset master on: %+v because replication threads are not stopped", instanceKey)
	}

	if *dtstruct.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting reset-master operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err = ExecSQLOnInstance(instanceKey, `reset master`)
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Reset master %+v", instanceKey)

	instance, err = GetInfoFromInstance(instanceKey, false, false, nil, "")
	return instance, err
}

// StartReplication starts replication on a given instance.
func StartReplication(ctx context.Context, instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", instanceKey)
	}

	// If async fallback is disallowed, we'd better make sure to enable replicas to
	// send ACKs before START SLAVE. Replica ACKing is off at mysqld startup because
	// some replicas (those that must never be promoted) should never ACK.
	// Note: We assume that replicas use 'skip-slave-start' so they won't
	//       START SLAVE on their own upon restart.
	if instance.SemiSyncEnforced {
		// Send ACK only from promotable instances.
		sendACK := instance.PromotionRule != dtstruct.MustNotPromoteRule
		// Always disable master setting, in case we're converting a former master.
		if err := EnableSemiSync(instanceKey, false, sendACK); err != nil {
			return instance, log.Errore(err)
		}
	}

	_, err = ExecSQLOnInstance(instanceKey, `start slave`)
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Started replication on %+v", instanceKey)

	WaitForReplicationState(instanceKey, mdtstruct.ReplicationThreadStateRunning)

	instance, err = GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}
	if !instance.ReplicaRunning() {
		return instance, common.ReplicationNotRunningError
	}
	return instance, nil
}

func StopReplication(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.IsReplica() {
		return instance, fmt.Errorf("instance is not a replica: %+v", instanceKey)
	}
	_, err = ExecSQLOnInstance(instanceKey, `stop slave`)
	if err != nil {
		// Patch; current MaxScale behavior for STOP SLAVE is to throw an error if replica already stopped.
		if instance.IsMaxScale() && err.Error() == "Error 1199: Slave connection is not running" {
			err = nil
		}
	}
	if err != nil {
		return instance, log.Errore(err)
	}
	instance, err = GetInfoFromInstance(instanceKey, false, false, nil, "")

	log.Infof("Stopped replication on %+v, Self:%+v, Exec:%+v", *instanceKey, instance.SelfBinlogCoordinates, instance.ExecBinlogCoordinates)
	return instance, err
}

// RestartReplication stops & starts replication on a given instance
func RestartReplication(instanceKey *dtstruct.InstanceKey) (inst interface{}, err error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}
	instance, err = StartReplication(context.TODO(), instanceKey)
	return instance, log.Errore(err)
}

// ResetReplication resets a replica, breaking the replication
func ResetReplication(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}

	if instance.ReplicationThreadsExist() && !instance.ReplicationThreadsStopped() {
		return instance, fmt.Errorf("Cannot reset replication on: %+v because replication threads are not stopped", instanceKey)
	}

	if *dtstruct.RuntimeCLIFlags.Noop {
		return instance, fmt.Errorf("noop: aborting reset-replication operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	// MySQL's RESET SLAVE is done correctly; however SHOW SLAVE STATUS still returns old hostnames etc
	// and only resets till after next restart. This leads to ham4db still thinking the instance replicates
	// from old host. We therefore forcibly modify the hostname.
	// RESET SLAVE ALL command solves this, but only as of 5.6.3
	_, err = ExecSQLOnInstance(instanceKey, `change master to master_host='_'`)
	if err != nil {
		return instance, log.Errore(err)
	}
	_, err = ExecSQLOnInstance(instanceKey, `reset slave /*!50603 all */`)
	if err != nil && strings.Contains(err.Error(), constant.Error1201CouldnotInitializeMasterInfoStructure) {
		log.Debugf("ResetReplication: got %+v", err)
		workaroundBug83713(instanceKey)
		_, err = ExecSQLOnInstance(instanceKey, `reset slave /*!50603 all */`)
	}
	if err != nil {
		return instance, log.Errore(err)
	}
	log.Infof("Reset replication %+v", instanceKey)

	instance, err = GetInfoFromInstance(instanceKey, false, false, nil, "")
	return instance, err
}

// ResetReplicationOperation will reset a replica
func ResetReplicationOperation(instanceKey *dtstruct.InstanceKey) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, err
	}

	log.Infof("Will reset replica on %+v", instanceKey)

	if maintenanceToken, merr := base.BeginMaintenance(instanceKey, dtstruct.GetMaintenanceOwner(), "reset replica"); merr != nil {
		err = fmt.Errorf("Cannot begin maintenance on %+v: %v", *instanceKey, merr)
		goto Cleanup
	} else {
		defer base.EndMaintenance(maintenanceToken)
	}

	if instance.IsReplica() {
		instance, err = StopReplication(instanceKey)
		if err != nil {
			goto Cleanup
		}
	}

	instance, err = ResetReplication(instanceKey)
	if err != nil {
		goto Cleanup
	}

Cleanup:
	instance, _ = StartReplication(context.TODO(), instanceKey)

	if err != nil {
		return instance, log.Errore(err)
	}

	// and we're done (pending deferred functions)
	base.AuditOperation("reset-slave", instanceKey, instance.ClusterName, fmt.Sprintf("%+v replication reset", *instanceKey))

	return instance, err
}

// CanReplicateFrom uses heursitics to decide whether this instacne can practically replicate from other instance.
// Checks are made to binlog format, version number, binary logs etc.
func CanReplicateFrom(fst dtstruct.InstanceAdaptor, thr dtstruct.InstanceAdaptor) (bool, error) {
	first := fst.(*mdtstruct.MysqlInstance)
	other := thr.(*mdtstruct.MysqlInstance)
	if first.Key.Equals(&other.Key) {
		return false, fmt.Errorf("instance cannot replicate from itself: %+v", first.Key)
	}
	if !other.LogBinEnabled {
		return false, fmt.Errorf("instance does not have binary logs enabled: %+v", other.Key)
	}
	if other.IsReplica() {
		if !other.LogReplicationUpdatesEnabled {
			return false, fmt.Errorf("instance does not have log_slave_updates enabled: %+v", other.Key)
		}
		// OK for a master to not have log_slave_updates
		// Not OK for a replica, for it has to relay the logs.
	}
	if first.IsSmallerMajorVersion(other.Instance) && !first.IsReplicaServer() {
		return false, fmt.Errorf("instance %+v has version %s, which is lower than %s on %+v ", first.Key, first.Version, other.Version, other.Key)
	}
	if first.LogBinEnabled && first.LogReplicationUpdatesEnabled {
		if IsSmallerBinlogFormat(first.Binlog_format, other.Binlog_format) {
			return false, fmt.Errorf("Cannot replicate from %+v binlog format on %+v to %+v on %+v", other.Binlog_format, other.Key, first.Binlog_format, first.Key)
		}
	}
	if config.Config.VerifyReplicationFilters {
		if other.HasReplicationFilters && !first.HasReplicationFilters {
			return false, fmt.Errorf("%+v has replication filters", other.Key)
		}
	}
	if first.InstanceId == other.InstanceId && !first.IsReplicaServer() {
		return false, fmt.Errorf("Identical server id: %+v, %+v both have %s", other.Key, first.Key, first.InstanceId)
	}
	if first.InstanceId == other.InstanceId && first.InstanceId != "" && !first.IsReplicaServer() {
		return false, fmt.Errorf("Identical server UUID: %+v, %+v both have %s", other.Key, first.Key, first.InstanceId)
	}
	if first.SQLDelay < other.SQLDelay && int64(other.SQLDelay) > int64(config.Config.ReasonableMaintenanceReplicationLagSeconds) {
		return false, fmt.Errorf("%+v has higher SQL_Delay (%+v seconds) than %+v does (%+v seconds)", other.Key, other.SQLDelay, first.Key, first.SQLDelay)
	}
	return true, nil
}

// StopReplicationNicely stops a replica such that SQL_thread and IO_thread are aligned (i.e.
// SQL_thread consumes all relay log entries)
// It will actually START the sql_thread even if the replica is completely stopped.
func StopReplicationNicely(instanceKey *dtstruct.InstanceKey, timeout time.Duration) (*mdtstruct.MysqlInstance, error) {
	instance, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return instance, log.Errore(err)
	}

	if !instance.ReplicationThreadsExist() {
		return instance, fmt.Errorf("instance is not a replica: %+v", instanceKey)
	}

	// stop io_thread, start sql_thread but catch any errors
	for _, cmd := range []string{`stop slave io_thread`, `start slave sql_thread`} {
		if _, err := ExecSQLOnInstance(instanceKey, cmd); err != nil {
			return nil, log.Errorf("%+v: StopReplicationNicely: '%q' failed: %+v", *instanceKey, cmd, err)
		}
	}

	if instance.SQLDelay == 0 {
		// Otherwise we don't bother.
		if instance, err = WaitForSQLThreadUpToDate(instanceKey, timeout, 0); err != nil {
			return instance, err
		}
	}

	_, err = ExecSQLOnInstance(instanceKey, `stop slave`)
	if err != nil {
		// Patch; current MaxScale behavior for STOP SLAVE is to throw an error if replica already stopped.
		if instance.IsMaxScale() && err.Error() == "Error 1199: Slave connection is not running" {
			err = nil
		}
	}
	if err != nil {
		return instance, log.Errore(err)
	}

	instance, err = GetInfoFromInstance(instanceKey, false, false, nil, "")
	log.Infof("Stopped replication nicely on %+v, Self:%+v, Exec:%+v", *instanceKey, instance.SelfBinlogCoordinates, instance.ExecBinlogCoordinates)
	return instance, err
}

func WaitForSQLThreadUpToDate(instanceKey *dtstruct.InstanceKey, overallTimeout time.Duration, staleCoordinatesTimeout time.Duration) (instance *mdtstruct.MysqlInstance, err error) {
	// Otherwise we don't bother.
	var lastExecBinlogCoordinates dtstruct.LogCoordinates

	if overallTimeout == 0 {
		overallTimeout = 24 * time.Hour
	}
	if staleCoordinatesTimeout == 0 {
		staleCoordinatesTimeout = time.Duration(config.Config.ReasonableReplicationLagSeconds) * time.Second
	}
	generalTimer := time.NewTimer(overallTimeout)
	staleTimer := time.NewTimer(staleCoordinatesTimeout)
	for {
		instance, err = RetryInstanceFunction(func() (*mdtstruct.MysqlInstance, error) {
			return GetInfoFromInstance(instanceKey, false, false, nil, "")
		})
		if err != nil {
			return instance, log.Errore(err)
		}

		if instance.SQLThreadUpToDate() {
			// Woohoo
			return instance, nil
		}
		if instance.SQLDelay != 0 {
			return instance, log.Errorf("WaitForSQLThreadUpToDate: instance %+v has SQL Delay %+v. Operation is irrelevant", *instanceKey, instance.SQLDelay)
		}

		if !instance.ExecBinlogCoordinates.Equals(&lastExecBinlogCoordinates) {
			// means we managed to apply binlog events. We made progress...
			// so we reset the "staleness" timer
			if !staleTimer.Stop() {
				<-staleTimer.C
			}
			staleTimer.Reset(staleCoordinatesTimeout)
		}
		lastExecBinlogCoordinates = instance.ExecBinlogCoordinates

		select {
		case <-generalTimer.C:
			return instance, log.Errorf("WaitForSQLThreadUpToDate timeout on %+v after duration %+v", *instanceKey, overallTimeout)
		case <-staleTimer.C:
			return instance, log.Errorf("WaitForSQLThreadUpToDate stale coordinates timeout on %+v after duration %+v", *instanceKey, staleCoordinatesTimeout)
		default:
			log.Debugf("WaitForSQLThreadUpToDate waiting on %+v", *instanceKey)
			time.Sleep(cconstant.RetryInterval)
		}
	}
}

func ShowMasterStatus(instanceKey *dtstruct.InstanceKey) (masterStatusFound bool, executedGtidSet string, err error) {
	db, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return masterStatusFound, executedGtidSet, err
	}
	err = sqlutil.QueryRowsMap(db, "show master status", func(m sqlutil.RowMap) error {
		masterStatusFound = true
		executedGtidSet = m.GetStringD("Executed_Gtid_Set", "")
		return nil
	})
	return masterStatusFound, executedGtidSet, err
}

func ShowBinaryLogs(instanceKey *dtstruct.InstanceKey) (binlogs []string, err error) {
	db, err := OpenTopology(instanceKey.Hostname, instanceKey.Port)
	if err != nil {
		return binlogs, err
	}
	err = sqlutil.QueryRowsMap(db, "show binary logs", func(m sqlutil.RowMap) error {
		binlogs = append(binlogs, m.GetString("Log_name"))
		return nil
	})
	return binlogs, err
}

func RetryInstanceFunction(f func() (*mdtstruct.MysqlInstance, error)) (instance *mdtstruct.MysqlInstance, err error) {
	for i := 0; i < cconstant.RetryFunction; i++ {
		if instance, err = f(); err == nil {
			return instance, nil
		}
	}
	return instance, err
}

// KillQuery stops replication on a given instance
func KillQuery(instanceKey *dtstruct.InstanceKey, process int64) (*mdtstruct.MysqlInstance, error) {
	inst, err := GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return inst, log.Errore(err)
	}

	if *dtstruct.RuntimeCLIFlags.Noop {
		return inst, fmt.Errorf("noop: aborting kill-query operation on %+v; signalling error but nothing went wrong.", *instanceKey)
	}

	_, err = ExecSQLOnInstance(instanceKey, `kill query ?`, process)
	if err != nil {
		return inst, log.Errore(err)
	}

	inst, err = GetInfoFromInstance(instanceKey, false, false, nil, "")
	if err != nil {
		return inst, log.Errore(err)
	}

	log.Infof("Killed query on %+v", *instanceKey)
	base.AuditOperation("kill-query", instanceKey, inst.GetInstance().ClusterName, fmt.Sprintf("Killed query %d", process))
	return inst, err
}
