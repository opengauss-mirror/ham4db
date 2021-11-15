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

// InstancesByCountReplicas sorts instances by number of replicas, descending
type InstancesByCountReplicas []InstanceAdaptor

func (this InstancesByCountReplicas) Len() int {
	return len(this)
}
func (this InstancesByCountReplicas) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}
func (this InstancesByCountReplicas) Less(i, j int) bool {
	return len(this[i].GetReplicas()) < len(this[j].GetReplicas())
}
