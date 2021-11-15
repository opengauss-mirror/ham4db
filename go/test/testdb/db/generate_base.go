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

// Hold all testdb schema define here
// All table define here should be with testdb prefix in its table name
// for example: testdb_xxx

// GenerateSQLBase lists all sql statements required to init the testdb schema
func GenerateSQLBase() (sqlBase []string) {
	sqlBase = append(sqlBase,
		`
			create table if not exists testdb_test (
				equivalence_id bigint unsigned not null,
				last_suggested_timestamp timestamp not null default current_timestamp,
	
				primary key (equivalence_id)
			)
		`,
	)
	return
}
