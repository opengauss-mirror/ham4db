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

// InstancesSorterByExec sorts instances by executed binlog coordinates
type InstancesSorterByExec struct {
	instances  [](*MysqlInstance)
	dataCenter string
}

func NewInstancesSorterByExec(instances [](*MysqlInstance), dataCenter string) *InstancesSorterByExec {
	return &InstancesSorterByExec{
		instances:  instances,
		dataCenter: dataCenter,
	}
}

func (this *InstancesSorterByExec) Len() int { return len(this.instances) }
func (this *InstancesSorterByExec) Swap(i, j int) {
	this.instances[i], this.instances[j] = this.instances[j], this.instances[i]
}
func (this *InstancesSorterByExec) Less(i, j int) bool {
	// Returning "true" in this function means [i] is "smaller" than [j],
	// which will lead to [j] be a better candidate for promotion

	// Sh*t happens. We just might get nil while attempting to discover/recover
	if this.instances[i] == nil {
		return false
	}
	if this.instances[j] == nil {
		return true
	}
	if this.instances[i].ExecBinlogCoordinates.Equals(&this.instances[j].ExecBinlogCoordinates) {
		// Secondary sorting: "smaller" if not logging replica updates
		if this.instances[j].LogReplicationUpdatesEnabled && !this.instances[i].LogReplicationUpdatesEnabled {
			return true
		}
		// Next sorting: "smaller" if of higher version (this will be reversed eventually)
		// Idea is that given 5.6 a& 5.7 both of the exact position, we will want to promote
		// the 5.6 on top of 5.7, as the other way around is invalid
		if this.instances[j].Instance.IsSmallerMajorVersion(this.instances[i].Instance) {
			return true
		}
		// Next sorting: "smaller" if of larger binlog-format (this will be reversed eventually)
		// Idea is that given ROW & STATEMENT both of the exact position, we will want to promote
		// the STATEMENT on top of ROW, as the other way around is invalid
		if IsSmallerBinlogFormat(this.instances[j].Binlog_format, this.instances[i].Binlog_format) {
			return true
		}
		// Prefer local datacenter:
		if this.instances[j].DataCenter == this.dataCenter && this.instances[i].DataCenter != this.dataCenter {
			return true
		}
		// Prefer if not having errant GTID
		if this.instances[j].GtidErrant == "" && this.instances[i].GtidErrant != "" {
			return true
		}
		// Prefer candidates:
		if this.instances[j].PromotionRule.BetterThan(this.instances[i].PromotionRule) {
			return true
		}
	}
	return this.instances[i].ExecBinlogCoordinates.SmallerThan(&this.instances[j].ExecBinlogCoordinates)
}
