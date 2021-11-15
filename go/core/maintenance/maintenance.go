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
package maintenance

import "gitee.com/opengauss/ham4db/go/dtstruct"

// KillQuery kill a query on the given instance
func KillQuery(instanceKey *dtstruct.InstanceKey, process int64) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).KillQuery(instanceKey, process)
}

// SkipQuery skip a single query in a failed replication instance
func SkipQuery(instanceKey *dtstruct.InstanceKey) (dtstruct.InstanceAdaptor, error) {
	return dtstruct.GetHamHandler(instanceKey.DBType).SkipQuery(instanceKey)
}
