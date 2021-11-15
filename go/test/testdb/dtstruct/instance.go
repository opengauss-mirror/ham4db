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
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/test/testdb/common/constant"
)

func init() {
	dtstruct.InstanceAdaptorMap[constant.DBTTestDB] = &TestDBInstance{}
}

type TestDBInstance struct {
	*dtstruct.Instance
}

func (t TestDBInstance) SetFlavorName() {
	panic("implement me")
}

func (t TestDBInstance) GetDatabaseType() string {
	return constant.DBTTestDB
}

func (t TestDBInstance) GetHostname() string {
	panic("implement me")
}

func (t TestDBInstance) GetPort() int {
	panic("implement me")
}

func (t TestDBInstance) SetPromotionRule(rule dtstruct.CandidatePromotionRule) {
	panic("implement me")
}

func (t TestDBInstance) SetHstPrtAndClusterName(hostname string, port int, upstreamHostname string, upstreamPort int, clusterName string) {
	panic("implement me")
}

func (t TestDBInstance) ReplicaRunning() bool {
	panic("implement me")
}

func (t TestDBInstance) IsReplicaServer() bool {
	panic("implement me")
}

func (t TestDBInstance) Less(handler dtstruct.InstanceAdaptor, dataCenter string) bool {
	panic("implement me")
}

func (t TestDBInstance) IsReplica() bool {
	panic("implement me")
}

func (t TestDBInstance) GetInstance() *dtstruct.Instance {
	return t.Instance
}

func (t TestDBInstance) SetInstance(instance *dtstruct.Instance) {
	panic("implement me")
}

func (t TestDBInstance) GetHandler() dtstruct.InstanceAdaptor {
	panic("implement me")
}

func (t TestDBInstance) GetReplicas() dtstruct.InstanceKeyMap {
	panic("implement me")
}

func (t TestDBInstance) HumanReadableDescription() string {
	panic("implement me")
}
