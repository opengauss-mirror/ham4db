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

// GenerateSQLBase lists all sql statements required to init the mysql schema
func GenerateSQLBase() (sqlBase []string) {

	// TODO create index for this table
	// opengauss_database_instance record opengauss replication info, such as xlog location/role/sync_percent and so on
	sqlBase = append(sqlBase,
		`
			create table if not exists opengauss_database_instance (
				hostname varchar(200) not null,
				port smallint unsigned not null,
				db_id varchar(128),
				sender_id int unsigned,
				receiver_id int unsigned,
				local_role varchar(128),
				local_state varchar(128),
				peer_role varchar(128),
				peer_state varchar(128),
				xlog_sender_sent_location varchar(128) not null,
				xlog_sender_write_location varchar(128) not null,
				xlog_sender_flush_location varchar(128) not null,
				xlog_sender_replay_location varchar(128) not null,
				xlog_receiver_received_location varchar(128) not null,
				xlog_receiver_write_location varchar(128) not null,
				xlog_receiver_flush_location varchar(128) not null,
				xlog_receiver_replay_location varchar(128) not null,
				sync_percent tinyint not null default 0,
				channel varchar(128) not null,
				last_update_timestamp timestamp not null default '1971-01-01 00:00:00',

				primary key (hostname, port)
			)
		`,
	)
	return
}
