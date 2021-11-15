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

// majorVersionsSortedByCount sorts (major) versions:
// - primary sort: by count appearances
// - secondary sort: by version
type majorVersionsSortedByCount struct {
	versionsCount map[string]int
	versions      []string
}

func NewMajorVersionsSortedByCount(versionsCount map[string]int) *majorVersionsSortedByCount {
	versions := []string{}
	for v := range versionsCount {
		versions = append(versions, v)
	}
	return &majorVersionsSortedByCount{
		versionsCount: versionsCount,
		versions:      versions,
	}
}

func (this *majorVersionsSortedByCount) Len() int { return len(this.versions) }
func (this *majorVersionsSortedByCount) Swap(i, j int) {
	this.versions[i], this.versions[j] = this.versions[j], this.versions[i]
}
func (this *majorVersionsSortedByCount) Less(i, j int) bool {
	if this.versionsCount[this.versions[i]] == this.versionsCount[this.versions[j]] {
		return this.versions[i] > this.versions[j]
	}
	return this.versionsCount[this.versions[i]] < this.versionsCount[this.versions[j]]
}
func (this *majorVersionsSortedByCount) First() string {
	return this.versions[0]
}

// majorVersionsSortedByCount sorts (major) versions:
// - primary sort: by count appearances
// - secondary sort: by version
type BinlogFormatSortedByCount struct {
	databaseType string
	formatsCount map[string]int
	formats      []string
}

func NewBinlogFormatSortedByCount(dbt string, formatsCount map[string]int) *BinlogFormatSortedByCount {
	formats := []string{}
	for v := range formatsCount {
		formats = append(formats, v)
	}
	return &BinlogFormatSortedByCount{
		databaseType: dbt,
		formatsCount: formatsCount,
		formats:      formats,
	}
}

func (this *BinlogFormatSortedByCount) Len() int { return len(this.formats) }
func (this *BinlogFormatSortedByCount) Swap(i, j int) {
	this.formats[i], this.formats[j] = this.formats[j], this.formats[i]
}
func (this *BinlogFormatSortedByCount) Less(i, j int) bool {
	// TODO double check
	//if this.formatsCount[this.formats[i]] == this.formatsCount[this.formats[j]] {
	//	return core.IsSmallerBinlogFormat(this.databaseType, this.formats[j], this.formats[i])
	//}
	return this.formatsCount[this.formats[i]] < this.formatsCount[this.formats[j]]
}
func (this *BinlogFormatSortedByCount) First() string {
	return this.formats[0]
}
