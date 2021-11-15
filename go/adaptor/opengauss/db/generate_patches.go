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

package db

//	Patch for every version
//	Add new patch function for latest version so they can form a changelog.
//  if version is 1.0.0, function name should be patchV1d0d0

// GenerateSQLPatch contains all DDL for patching schema to the latest version.
func GenerateSQLPatch() (sqlPatch []string) {
	sqlPatch = append(sqlPatch, patchV1d0d0()...)
	return
}

// patchV1d0d0 patch for version v2.0.1
func patchV1d0d0() []string {
	return []string{}
}
