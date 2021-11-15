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

// Hold all mysql schema define here
// All table define here should be with mysql prefix in its table name
// for example: mysql_xxx

// GenerateSQLBase lists all sql statements required to init the mysql schema
func GenerateSQLBase() (sqlBase []string) {

	// TODO need to create index for this table
	// mysql_database_instance record all mysql base info collect from mysql database instance managed by ham4db
	sqlBase = append(sqlBase,
		`
			create table if not exists mysql_database_instance (
				di_hostname varchar(200) not null,
				di_port smallint unsigned not null,
				server_uuid varchar(64) not null,
				version_major varchar(128),
				version_comment varchar(128),
				binlog_format varchar(16) not null,
				binlog_row_image varchar(16) not null,
				is_binlog_server tinyint unsigned not null,
				log_bin tinyint unsigned not null,
				log_slave_updates tinyint unsigned not null,
				binary_log_file varchar(128) not null,
				binary_log_pos bigint unsigned not null,
				slave_sql_running tinyint unsigned not null,
				slave_io_running tinyint unsigned not null,
				master_log_file varchar(128) not null,
				read_master_log_pos bigint unsigned not null,
				relay_log_file varchar(128) not null,
				relay_log_pos bigint unsigned not null,
				relay_master_log_file varchar(128) not null,
				exec_master_log_pos bigint unsigned not null,
				seconds_behind_master bigint unsigned default null,
				last_sql_error text not null,
				last_io_error text not null,
				sql_delay int unsigned not null,
				last_discovery_latency bigint not null,
				oracle_gtid tinyint unsigned not null,
				supports_oracle_gtid tinyint unsigned not null,
				mariadb_gtid tinyint unsigned not null,
				pseudo_gtid tinyint unsigned not null,
				executed_gtid_set text not null,
				gtid_purged text not null,
				gtid_mode varchar(32) not null,
				gtid_errant text not null,
				master_uuid varchar(64) not null,
				ancestry_uuid text not null,
				replication_sql_thread_state tinyint signed not null default 0,
				replication_io_thread_state tinyint signed not null default 0,
				semi_sync_master_enabled tinyint unsigned not null,
				semi_sync_replica_enabled tinyint unsigned not null,
				semi_sync_enforced tinyint unsigned not null,
				semi_sync_master_timeout int unsigned not null default 0,
				semi_sync_master_wait_for_slave_count int unsigned not null default 0,
				semi_sync_master_status tinyint unsigned not null default 0,
				semi_sync_replica_status tinyint unsigned not null default 0,
				semi_sync_master_clients int unsigned not null default 0,
				semi_sync_available tinyint unsigned not null default 0,
				replication_group_name varchar(64) not null default '',
				replication_group_is_single_primary_mode tinyint unsigned not null default 1,
				replication_group_member_state varchar(16) not null default '',
				replication_group_member_role varchar(16) not null default '',
				replication_group_members text not null,
				replication_group_primary_host varchar(200) not null default '',
				replication_group_primary_port smallint unsigned not null default 0,

				primary key (di_hostname, di_port)
			)
		`,
	)

	// mysql_database_instance_stale_binlog_coordinate record stale binlog for database
	sqlBase = append(sqlBase,
		`
			create table if not exists mysql_database_instance_stale_binlog_coordinate (
				hostname varchar(200) not null,
				port smallint unsigned not null,
				binary_log_file varchar(128) not null,
				binary_log_pos bigint unsigned not null,
				first_seen_timestamp timestamp not null default current_timestamp,

				primary key (hostname, port)
			)
		`,
		`
			create index idx_mdisbc_first_seen_timestamp on mysql_database_instance_stale_binlog_coordinate (first_seen_timestamp)
		`,
	)

	// mysql_database_instance_coordinate_history
	sqlBase = append(sqlBase,
		`
			create table if not exists mysql_database_instance_coordinate_history (
				history_id bigint unsigned not null auto_increment,
				hostname varchar(200) not null,
				port smallint unsigned not null,
				record_timestamp timestamp not null default current_timestamp,
				binary_log_file varchar(128) not null,
				binary_log_pos bigint unsigned not null,
				relay_log_file varchar(128) not null,
				relay_log_pos bigint unsigned not null,

				primary key (history_id)
			)
		`,
		`
			create index idx_mdich_hostname_port_recorded on mysql_database_instance_coordinate_history (hostname, port, record_timestamp)
		`,
		`
			create index idx_mdich_recorde_timestmp on mysql_database_instance_coordinate_history (record_timestamp)
		`,
	)

	// mysql_database_instance_binlog_file_history
	sqlBase = append(sqlBase,
		`
			create table if not exists mysql_database_instance_binlog_file_history (
				history_id bigint unsigned not null auto_increment,
				hostname varchar(200) not null,
				port smallint unsigned not null,
				binary_log_file varchar(128) not null,
				binary_log_pos bigint unsigned not null,
				first_seen_timestamp timestamp not null default current_timestamp,
				last_seen_timestamp timestamp not null default '1971-01-01 00:00:00',

				primary key (history_id)
			)
		`,
		`
			create unique index idx_mdibfh_hostname_port_file on mysql_database_instance_binlog_file_history (hostname, port, binary_log_file)
		`,
		`
			create index last_seen_idx_database_instance_binlog_files_history on mysql_database_instance_binlog_file_history (last_seen_timestamp)
		`,
	)

	// mysql_cluster_injected_pseudo_gtid
	sqlBase = append(sqlBase,
		`
			create table if not exists mysql_cluster_injected_pseudo_gtid (
				cluster_name varchar(200) not null,
				time_injected_timestamp timestamp not null default current_timestamp,

				primary key (cluster_name)
			)
		`,
	)

	// mysql_master_position_equivalence
	sqlBase = append(sqlBase,
		`
			create table if not exists mysql_master_position_equivalence (
				equivalence_id bigint unsigned not null auto_increment,
				master1_hostname varchar(200) not null,
				master1_port smallint unsigned not null,
				master1_binary_log_file varchar(128) not null,
				master1_binary_log_pos bigint unsigned not null,
				master2_hostname varchar(128) not null,
				master2_port smallint unsigned not null,
				master2_binary_log_file varchar(128) not null,
				master2_binary_log_pos bigint unsigned not null,
				last_suggested_timestamp timestamp not null default current_timestamp,
	
				primary key (equivalence_id)
			)
		`,
		`
			create unique index uidx_mmpe_equivalence on mysql_master_position_equivalence (master1_hostname, master1_port, master1_binary_log_file, master1_binary_log_pos, master2_hostname, master2_port)
		`,
		`
			create index idx_mmpe_master2 on mysql_master_position_equivalence (master2_hostname, master2_port, master2_binary_log_file, master2_binary_log_pos)
		`,
		`
			create index idx_mmpe_last_suggested on mysql_master_position_equivalence (last_suggested_timestamp)
		`,
	)
	return
}
