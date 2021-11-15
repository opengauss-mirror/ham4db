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
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"io"
)

type SnapshotHandler interface {
	Snapshot() (data []byte, err error)
	Restore(rc io.ReadCloser) error
}

var SnapshotHandlerMap = make(map[string]SnapshotHandler)

type SnapshotData struct {
	Keys             []InstanceKey // Kept for backwards comapatibility
	MinimalInstances []MinimalInstance
	RecoveryDisabled bool

	ClusterAlias,
	ClusterAliasOverride,
	ClusterDomainName,
	HostAttributes,
	InstanceTags,
	AccessToken,
	PoolInstances,
	InjectedPseudoGTIDClusters,
	HostnameResolves,
	HostnameUnresolves,
	DowntimedInstances,
	Candidates,
	Detections,
	KVStore,
	Recovery,
	RecoverySteps sqlutil.NamedResultData

	LeaderURI string
}

func NewSnapshotData() *SnapshotData {
	return &SnapshotData{}
}
