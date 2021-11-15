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

package rdms

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"gitee.com/opengauss/ham4db/go/core/base"
	orcraft "gitee.com/opengauss/ham4db/go/core/consensus/raft"
	"gitee.com/opengauss/ham4db/go/core/discover"

	"gitee.com/opengauss/ham4db/go/core/instance"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"io"

	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
)

type SnapshotDataCreatorApplier struct {
}

func NewSnapshotDataCreatorApplier() *SnapshotDataCreatorApplier {
	generator := &SnapshotDataCreatorApplier{}
	return generator
}

func (sdca *SnapshotDataCreatorApplier) Snapshot() (data []byte, err error) {
	snapshotData := createSnapshotData()
	b, err := json.Marshal(snapshotData)
	if err != nil {
		return b, err
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		return b, err
	}
	if err := zw.Close(); err != nil {
		return b, err
	}
	return buf.Bytes(), nil
}

func (sdca *SnapshotDataCreatorApplier) Restore(rc io.ReadCloser) error {
	snapshotData := dtstruct.NewSnapshotData()
	zr, err := gzip.NewReader(rc)
	if err != nil {
		return err
	}
	if err := json.NewDecoder(zr).Decode(&snapshotData); err != nil {
		return err
	}

	orcraft.LeaderURI.Set(snapshotData.LeaderURI)
	// keys
	{
		snapshotInstanceKeyMap := dtstruct.NewInstanceKeyMap()
		snapshotInstanceKeyMap.AddKeys(snapshotData.Keys)
		for _, minimalInstance := range snapshotData.MinimalInstances {
			snapshotInstanceKeyMap.AddKey(minimalInstance.Key)
		}

		discardedKeys := 0
		// Forget instances that were not in snapshot
		existingKeys, _ := base.ReadAllInstanceKeys()
		for _, existingKey := range existingKeys {
			if !snapshotInstanceKeyMap.HasKey(existingKey) {
				instance.ForgetInstance(&existingKey)
				discardedKeys++
			}
		}
		log.Debugf("raft snapshot restore: discarded %+v keys", discardedKeys)
		existingKeysMap := dtstruct.NewInstanceKeyMap()
		existingKeysMap.AddKeys(existingKeys)

		// Discover instances that are in snapshot and not in our own database.
		// Instances that _are_ in our own database will self-discover. No need
		// to explicitly discover them.
		discoveredKeys := 0
		// v2: read keys + master keys
		for _, minimalInstance := range snapshotData.MinimalInstances {
			if !existingKeysMap.HasKey(minimalInstance.Key) {
				inst := &dtstruct.Instance{}
				inst.Key = minimalInstance.Key
				inst.UpstreamKey = minimalInstance.MasterKey
				inst.ClusterName = minimalInstance.ClusterName
				instAdaptor := dtstruct.GetInstanceAdaptor(minimalInstance.Key.DBType)
				instAdaptor.SetInstance(inst)
				if err := instance.WriteInstance([]dtstruct.InstanceAdaptor{instAdaptor}, false, true, nil); err == nil {
					discoveredKeys++
				} else {
					log.Errore(err)
				}
			}
		}
		if len(snapshotData.MinimalInstances) == 0 {
			// v1: read keys (backwards support)
			for _, snapshotKey := range snapshotData.Keys {
				if !existingKeysMap.HasKey(snapshotKey) {
					ssKey := snapshotKey
					go func() {
						discover.SnapshotDiscoveryKeys <- ssKey
					}()
					discoveredKeys++
				}
			}
		}
		log.Debugf("raft snapshot restore: discovered %+v keys", discoveredKeys)
	}
	writeTableData("ham_cluster_alias", &snapshotData.ClusterAlias)
	writeTableData("ham_cluster_alias_override", &snapshotData.ClusterAliasOverride)
	writeTableData("ham_cluster_domain_name", &snapshotData.ClusterDomainName)
	writeTableData("ham_access_token", &snapshotData.AccessToken)
	writeTableData("ham_hostname_attribute", &snapshotData.HostAttributes)
	writeTableData("ham_database_instance_tag", &snapshotData.InstanceTags)
	writeTableData("ham_database_instance_pool", &snapshotData.PoolInstances)
	writeTableData("ham_hostname_resolve", &snapshotData.HostnameResolves)
	writeTableData("ham_hostname_unresolved", &snapshotData.HostnameUnresolves)
	writeTableData("ham_database_instance_downtime", &snapshotData.DowntimedInstances)
	writeTableData("ham_database_instance_candidate", &snapshotData.Candidates)
	writeTableData("ham_kv_store", &snapshotData.KVStore)
	writeTableData("ham_topology_recovery", &snapshotData.Recovery)
	writeTableData("ham_topology_failure_detection", &snapshotData.Detections)
	writeTableData("ham_topology_recovery_step", &snapshotData.RecoverySteps)

	// mysql
	writeTableData("mysql_cluster_injected_pseudo_gtid", &snapshotData.InjectedPseudoGTIDClusters)

	// recovery disable
	{
		base.SetRecoveryDisabled(snapshotData.RecoveryDisabled)
	}
	log.Debugf("raft snapshot restore applied")
	return nil
}

func readTableData(tableName string, data *sqlutil.NamedResultData) error {
	orcdb, err := db.GetDBClient()
	if err != nil {
		return log.Errore(err)
	}
	*data, err = sqlutil.ScanTable(orcdb, tableName)
	return log.Errore(err)
}

func writeTableData(tableName string, data *sqlutil.NamedResultData) error {
	orcdb, err := db.GetDBClient()
	if err != nil {
		return log.Errore(err)
	}
	err = sqlutil.WriteTable(orcdb, tableName, *data)
	return log.Errore(err)
}

func createSnapshotData() *dtstruct.SnapshotData {
	snapshotData := dtstruct.NewSnapshotData()

	snapshotData.LeaderURI = orcraft.LeaderURI.Get()
	// keys
	snapshotData.Keys, _ = base.ReadAllInstanceKeys()
	snapshotData.MinimalInstances, _ = instance.ReadInstanceMinimal()
	snapshotData.RecoveryDisabled, _ = base.IsRecoveryDisabled()

	readTableData("ham_cluster_alias", &snapshotData.ClusterAlias)
	readTableData("ham_cluster_alias_override", &snapshotData.ClusterAliasOverride)
	readTableData("ham_cluster_domain_name", &snapshotData.ClusterDomainName)
	readTableData("ham_access_token", &snapshotData.AccessToken)
	readTableData("ham_hostname_attribute", &snapshotData.HostAttributes)
	readTableData("ham_database_instance_tag", &snapshotData.InstanceTags)
	readTableData("ham_database_instance_pool", &snapshotData.PoolInstances)
	readTableData("ham_hostname_resolve", &snapshotData.HostnameResolves)
	readTableData("ham_hostname_unresolved", &snapshotData.HostnameUnresolves)
	readTableData("ham_database_instance_downtime", &snapshotData.DowntimedInstances)
	readTableData("ham_database_instance_candidate", &snapshotData.Candidates)
	readTableData("ham_topology_failure_detection", &snapshotData.Detections)
	readTableData("ham_kv_store", &snapshotData.KVStore)
	readTableData("ham_topology_recovery", &snapshotData.Recovery)
	readTableData("ham_topology_recovery_step", &snapshotData.RecoverySteps)

	// mysql
	readTableData("mysql_cluster_injected_pseudo_gtid", &snapshotData.InjectedPseudoGTIDClusters)

	log.Debugf("raft snapshot data created")
	return snapshotData
}
