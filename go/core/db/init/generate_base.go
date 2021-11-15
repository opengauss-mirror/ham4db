/*
   copyright 2017 shlomi noach, github inc.

   licensed under the apache license, version 2.0 (the "license");
   you may not use this file except in compliance with the license.
   you may obtain a copy of the license at

       http://www.apache.org/licenses/license-2.0

   unless required by applicable law or agreed to in writing, software
   distributed under the license is distributed on an "as is" basis,
   without warranties or conditions of any kind, either express or implied.
   see the license for the specific language governing permissions and
   limitations under the license.
*/
package init

// GenerateSQLBase lists all sql statements required to init the ham4db backend
func GenerateSQLBase() (sqlBase []string) {
	// hamDatabaseInstance hold all base info for database that be managed
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_database_instance (
				uptime int unsigned,
				hostname varchar(200) not null,
				port smallint unsigned not null,
				cluster_id varchar(100),
				cluster_name varchar(200),
				cluster_alias varchar(200),
				db_type varchar(50) not null,
				db_id varchar(128),
				db_version varchar(200),
				db_alias varchar(200),
				db_state varchar(128),
				db_role varchar(128),
				pl_region varchar(200),
				pl_data_center varchar(200),
				pl_zone varchar(200),
				environment varchar(32),
				last_attempted_check_timestamp timestamp null default '1971-01-01 00:00:00',
				last_checked_timestamp timestamp null default current_timestamp,
				last_seen_timestamp timestamp null,
				is_last_check_partial_success tinyint unsigned,
				upstream_host varchar(200) not null default '',
				upstream_port smallint unsigned not null default 0,
				downstream_hosts text,
				downstream_count smallint unsigned,
				replication_state varchar(128),
				replication_depth tinyint unsigned,
				replication_downstream_lag int,
				is_co_master tinyint unsigned,
				is_read_only tinyint unsigned,
				is_allow_tls tinyint unsigned,
				is_replication_credentials_available tinyint unsigned,
				has_replication_filters tinyint unsigned,
				has_replication_credentials tinyint unsigned,
		
				primary key (hostname, port)
			)
		`,
		`
			create index idx_hdi_upstream_host_port on ham_database_instance (upstream_host, upstream_port)
		`,
		`
			create index idx_hdi_cluster on ham_database_instance (cluster_name)
		`,
		`
			create index idx_hdi_last_checked on ham_database_instance (last_checked_timestamp)
		`,
		`
			create index idx_hdi_last_seen on ham_database_instance (last_seen_timestamp)
		`,
	}...)

	// TODO delete table ham_database_instance_last_analysis and ham_database_instance_analysis_changelog, use ham_topology_failure_detection instead?
	// hamDatabaseInstanceLastAnalysis record database analysis code and keep update when analysis code change
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_database_instance_last_analysis (
				hostname varchar(200) not null,
				port smallint unsigned not null,
				analysis_timestamp timestamp not null default current_timestamp,
				analysis varchar(128) not null,

				primary key (hostname, port)
			)
		`,
		`
			create index idx_hdila_analysis_timestamp on ham_database_instance_last_analysis (analysis_timestamp)
		`,
	}...)
	// hamDatabaseInstanceAnalysisChangelog record database analysis code, delete when expire
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_database_instance_analysis_changelog (
				changelog_id bigint unsigned not null auto_increment,
				hostname varchar(200) not null,
				port smallint unsigned not null,
				analysis_timestamp timestamp not null default current_timestamp,
				analysis varchar(128) not null,

				primary key (changelog_id)
			)
		`,
		`
			create index idx_hdiac_analysis_timestamp on ham_database_instance_analysis_changelog (analysis_timestamp)
		`,
		`
			create index idx_hdiac_instance_timestamp on ham_database_instance_analysis_changelog (hostname, port, analysis_timestamp)
		`,
	}...)
	// hamTopologyFailureDetection record replication analysis entry for failure detection
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_topology_failure_detection (
				detection_id bigint unsigned not null auto_increment,
				hostname varchar(200) not null,
				port smallint unsigned not null,
				db_type varchar(50) not null,
				in_active_period tinyint unsigned not null default 0,
				start_active_period_timestamp timestamp not null default current_timestamp,
				end_active_period_unix_time int unsigned not null,
				processing_node_hostname varchar(200) not null,
				processing_node_token varchar(128) not null,
				analysis varchar(128) not null,
				cluster_name varchar(200) not null,
				cluster_alias varchar(200) not null,
				count_affected_downstream smallint unsigned not null,
				downstream_hosts text not null,
				is_actionable tinyint not null default 0,

				primary key (detection_id)
			)
		`,
		`
			create index idx_htfd_in_active_start_period on ham_topology_failure_detection (in_active_period, start_active_period_timestamp)
		`,
		`
			create unique index idx_htfd_host_port_active_recoverable on ham_topology_failure_detection (hostname, port, in_active_period, end_active_period_unix_time, is_actionable)
		`,
	}...)
	// hamTopologyRecovery record database recovery info like when to start, which node to process and so on
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_topology_recovery (
				recovery_id bigint unsigned not null auto_increment,
				db_type varchar(50) not null,
				hostname varchar(200) not null,
				port smallint unsigned not null,
				in_active_period tinyint unsigned not null default 0,
				start_active_period_timestamp timestamp not null default current_timestamp,
				end_active_period_unix_time int unsigned,
				end_recovery_timestamp timestamp null default null,
				processing_node_hostname varchar(200) not null,
				processing_node_token varchar(128) not null,
				is_successful tinyint unsigned not null default 0,
				successor_hostname varchar(200),
				successor_port smallint unsigned,
				successor_alias varchar(128),
				analysis varchar(128) not null,
				cluster_name varchar(200) not null,
				cluster_alias varchar(200) not null,
				count_affected_downstream int unsigned not null,
				downstream_hosts text not null,
				participating_instances text not null,
				lost_downstream text not null,
				all_errors text not null,
				is_acked tinyint unsigned not null default 0,
				acked_timestamp timestamp null default null,
				acked_by varchar(128) not null,
				acked_comment text not null,
				last_detection_id bigint unsigned not null,
				recovery_uid varchar(128) not null,

				primary key (recovery_id)
			)
		`,
		`
			create index idx_htr_in_active_start_period on ham_topology_recovery (in_active_period, start_active_period_timestamp)
		`,
		`
			create index idx_htr_start_active_period on ham_topology_recovery (start_active_period_timestamp)
		`,
		`
			create unique index uidx_htr_hostname_port_active_period on ham_topology_recovery (hostname, port, in_active_period, end_active_period_unix_time)
		`,
		`
			create index idx_htr_cluster_name_in_active on ham_topology_recovery (cluster_name, in_active_period)
		`,
		`
			create index idx_htr_end_recovery_timestamp on ham_topology_recovery (end_recovery_timestamp)
		`,
		`
			create index idx_htr_acked on ham_topology_recovery (is_acked, acked_timestamp)
		`,
		`
			create index idx_htr_last_detection_id on ham_topology_recovery (last_detection_id)
		`,
		`
			create index idx_htr_recovery_uid on ham_topology_recovery (recovery_uid)
		`,
	}...)
	// hamTopologyRecoveryBlocked record blocked topology and indicates what recovery they are blocked on
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_topology_recovery_blocked (
				hostname varchar(200) not null,
				port smallint unsigned not null,
				cluster_name varchar(200) not null,
				analysis varchar(128) not null,
				last_blocked_timestamp timestamp not null default current_timestamp,
				blocking_recovery_id bigint unsigned not null,

				primary key (hostname, port)
			)
		`,
		`
			create index idx_hbtr_cluster_blocked on ham_topology_recovery_blocked (cluster_name, last_blocked_timestamp)
		`,
		`
			create index idx_hbtr_last_blocked_timestamp on ham_topology_recovery_blocked (last_blocked_timestamp)
		`,
	}...)
	// hamTopologyRecoveryStep record all step in a recovery process
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_topology_recovery_step (
				step_id bigint unsigned not null auto_increment,
				recovery_uid varchar(128) not null,
				audit_timestamp timestamp not null default current_timestamp,
				message text not null,

				primary key (step_id)
			)
		`,
		`
			create index idx_htrs_recovery_uid on ham_topology_recovery_step (recovery_uid)
		`,
	}...)
	// hamDatabaseInstanceCandidate mark a given instance as suggested for successoring a master in the event of fail over, delete when expire
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_database_instance_candidate (
				hostname varchar(200) not null,
				port smallint unsigned not null,
				cluster_id varchar(100),
				db_type varchar(50) not null,
				last_suggested_timestamp timestamp not null default current_timestamp,
				priority tinyint unsigned not null default 1,
				promotion_rule varchar(20) not null,

				primary key (hostname, port)
			)
		`,
		`
			create index idx_hcdi_last_suggested_timestamp on ham_database_instance_candidate (last_suggested_timestamp)
		`,
	}...)
	// hamDatabaseInstanceDowntime record downtime info, when/why/who
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_database_instance_downtime (
				hostname varchar(200) not null,
				port smallint unsigned not null,
				downtime_active tinyint unsigned,
				begin_timestamp timestamp default current_timestamp,
				end_timestamp timestamp null default null,
				owner varchar(128) not null,
				reason text not null,

				primary key (hostname, port)
			)
		`,
		`
			create index idx_hdid_end_timestamp on ham_database_instance_downtime (end_timestamp)
		`,
	}...)
	// hamDatabaseInstanceMaintenance record who and why do maintenance on database
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_database_instance_maintenance (
				maintenance_id int unsigned not null auto_increment,
				db_type varchar(50) not null,
				hostname varchar(200) not null,
				port smallint unsigned not null,
				maintenance_active tinyint unsigned default null,
				begin_timestamp timestamp null default null,
				end_timestamp timestamp null default null,
				processing_node_hostname varchar(200) not null,
				processing_node_token varchar(128) not null,
				explicitly_bounded tinyint unsigned not null,
				owner varchar(128) not null,
				reason text not null,
	
				primary key (maintenance_id)
			)
		`,
		`
			create unique index uidx_hdim_maintenance on ham_database_instance_maintenance (maintenance_active, hostname, port)
		`,
		`
			create index idx_hdim_active_timestamp on ham_database_instance_maintenance (maintenance_active, begin_timestamp)
		`,
		`
			create index idx_hdim_active_end_timestamp on ham_database_instance_maintenance (maintenance_active, end_timestamp)
		`,
	}...)
	// hamDatabaseInstanceTopologyHistory record database topology periodically
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_database_instance_topology_history (
				snapshot_unix_time int unsigned not null,
				hostname varchar(200) not null,
				port smallint unsigned not null,
				cluster_name varchar(200) not null,
				upstream_host varchar(200) not null,
				upstream_port smallint unsigned not null,
				version varchar(128) not null,

				primary key (snapshot_unix_time, hostname, port)
			)
		`,
		`
			create index idx_dith_snpsht_unix_time_clst_name on ham_database_instance_topology_history (snapshot_unix_time, cluster_name(60))
		`,
	}...)

	// hamHostnameResolve record mapping for resolved hostname and hostname, delete when expire
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_hostname_resolve (
				hostname varchar(200) not null,
				resolved_hostname varchar(200) not null,
				resolved_timestamp timestamp not null default current_timestamp,
	
				primary key (hostname)
			)
		`,
		`
			create index idx_hhr_resolved_timestamp on ham_hostname_resolve (resolved_timestamp)
		`,
	}...)
	// hamHostnameResolveHistory record mapping for resolved hostname and hostname
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_hostname_resolve_history (
				resolved_hostname varchar(200) not null,
				hostname varchar(200) not null,
				resolved_timestamp timestamp not null default current_timestamp,

				primary key (resolved_hostname)
			)
		`,
		`
			create index idx_hhrh_hostname on ham_hostname_resolve_history (hostname)
		`,
		`
			create index idx_hhrh_resolved_timestamp on ham_hostname_resolve_history (resolved_timestamp)
		`,
	}...)
	// hamHostnameUnresolved record mapping for unresolved hostname and hostname, delete when expire
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_hostname_unresolved (
				hostname varchar(200) not null,
				unresolved_hostname varchar(200) not null,
				last_register_timestamp timestamp not null default current_timestamp,

				primary key (hostname)
			)
		`,
		`
			create index idx_hhu_unresolved_hostname on ham_hostname_unresolved (unresolved_hostname)
		`,
		`
			create index idx_hhu_last_register_timestamp on ham_hostname_unresolved (last_register_timestamp)
		`,
	}...)
	// hamHostnameUnresolvedHistory record history mapping for unresolved hostname and hostname, not delete
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_hostname_unresolved_history (
				unresolved_hostname varchar(200) not null,
				hostname varchar(200) not null,
				last_register_timestamp timestamp not null default current_timestamp,

				primary key (unresolved_hostname)
			)
		`,
		`
			create index idx_hhuh_hostname on ham_hostname_unresolved_history (hostname)
		`,
		`
			create index idx_hhuh_last_register_timestamp on ham_hostname_unresolved_history (last_register_timestamp)
		`,
	}...)
	// hamHostnameIp record ip for hostname, both ipv4 and ipv6
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_hostname_ip (
				hostname varchar(200) not null,
				ipv4 varchar(128) not null,
				ipv6 varchar(128) not null,
				last_updated_timestamp timestamp not null default current_timestamp,

				primary key (hostname)
			)
		`,
	}...)
	// hamHostAttribute record attribute and value for host
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_hostname_attribute (
				hostname varchar(200) not null,
				attribute_name varchar(128) not null,
				attribute_value varchar(128) not null,
				submit_timestamp timestamp not null default current_timestamp,
				expire_timestamp timestamp null,
	
				primary key (hostname, attribute_name)
			)
		`,
		`
			create index idx_hha_attribute_name on ham_hostname_attribute (attribute_name)
		`,
		`
			create index idx_hha_attribute_value on ham_hostname_attribute (attribute_value)
		`,
		`
			create index idx_hha_submit_timestamp on ham_hostname_attribute (submit_timestamp)
		`,
		`
			create index idx_hha_expire_timestamp on ham_hostname_attribute (expire_timestamp)
		`,
	}...)
	// hamClusterDomainName record mapping for cluster name and domain name
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_cluster_domain_name (
				cluster_name varchar(200) not null,
				domain_name varchar(200) not null,
				last_register_timestamp timestamp not null default current_timestamp,

				primary key (cluster_name)
			)
		`,
		`
			create index idx_hcdn_domain_name on ham_cluster_domain_name (domain_name(32))
		`,
		`
			create index idx_hcdn_last_register_timestamp on ham_cluster_domain_name (last_register_timestamp)
		`,
	}...)
	// hamClusterAlias record mapping for cluster name and alias
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_cluster_alias (
				cluster_name varchar(200) not null,
				alias varchar(200) not null,
				last_register_timestamp timestamp not null default current_timestamp,
	
				primary key (cluster_name)
			)
		`,
		`
			create unique index uidx_hca_alias on ham_cluster_alias (alias)
		`,
		`
			create index idx_hca_last_register_timestamp on ham_cluster_alias (last_register_timestamp)
		`,
	}...)
	// hamClusterAlias record mapping for cluster name and alias if manual override or need to replace when recovery
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_cluster_alias_override (
				cluster_name varchar(200) not null,
				alias varchar(200) not null,

				primary key (cluster_name)
			)
		`,
	}...)

	// hamDatabaseInstanceTls record if database need to connect by tls
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_database_instance_tls (
				hostname varchar(200) not null,
				port smallint unsigned not null,
				required tinyint unsigned not null default 0,

				primary key (hostname, port)
			)
		`,
	}...)
	// hamDatabaseInstanceTag record tag for database instance
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_database_instance_tag (
				hostname varchar(200) not null,
				port smallint unsigned not null,
				db_type varchar(50) not null,
				cluster_id varchar(100) not null,
				tag_name varchar(128) not null,
				tag_value varchar(128) not null,
				last_updated_timestamp timestamp not null default current_timestamp,

				primary key (hostname, port, db_type, cluster_id, tag_name)
			)
		`,
		`
			create index idx_hdit_tag_name on ham_database_instance_tag (tag_name)
		`,
	}...)
	// hamDatabaseInstancePool record the list of database for a pool, delete when expire
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_database_instance_pool (
				hostname varchar(200) not null,
				port smallint unsigned not null,
				pool varchar(128) not null,
				register_timestamp timestamp not null default '1971-01-01 00:00:00',

				primary key (hostname, port, pool)
			)
		`,
		`
			create index idx_hdi_pool on ham_database_instance_pool (pool)
		`,
	}...)

	// hamAccessToken record token info, delete when expire
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_access_token (
				token_id bigint unsigned not null auto_increment,
				public_token varchar(128) not null,
				secret_token varchar(128) not null,
				generate_timestamp timestamp not null default current_timestamp,
				generate_by varchar(128) not null,
				is_acquired tinyint unsigned not null default 0,
				is_reentrant tinyint unsigned not null default 0,
				acquired_timestamp timestamp not null default '1971-01-01 00:00:00',

				primary key (token_id)
			)
		`,
		`
			create unique index uidx_hat_public_token on ham_access_token (public_token)
		`,
		`
			create index idx_hat_generate_timestamp on ham_access_token (generate_timestamp)
		`,
	}...)
	// hamAudit record audit log, what happened to database
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_audit (
				audit_id bigint unsigned not null auto_increment,
				audit_timestamp timestamp not null default current_timestamp,
				audit_type varchar(128) not null,
				hostname varchar(200) not null,
				port smallint unsigned not null,
				cluster_id varchar(100),
				cluster_name varchar(200) not null,
				message text not null,
	
				primary key (audit_id)
		   )
		`,
		`
			create index idx_ha_timestamp on ham_audit (audit_timestamp)
		`,
		`
			create index idx_ha_host_port on ham_audit (hostname, port, audit_timestamp)
		`,
	}...)
	// hamGlobalRecoveryDisable enable or disable recovery globally, insert 1 to disable
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_global_recovery_disable (
				disable_recovery tinyint unsigned not null,

				primary key (disable_recovery)
			)
		`,
	}...)
	// hamDeployment record current ham4db version
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_deployment (
				deployed_version varchar(128) not null,
				deployed_timestamp timestamp not null default current_timestamp,

				primary key (deployed_version)
			)
		`,
	}...)

	// hamNodeActive record node anchor info used to grab leadership
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_node_active (
				anchor tinyint unsigned not null,
				hostname varchar(200) not null,
				token varchar(128) not null,
				first_seen_active_timestamp datetime not null default '1971-01-01 00:00:00',
				last_seen_active_timestamp datetime not null default current_timestamp,

				primary key (anchor)
			)
		`,
	}...)
	// hamNodeHealth record node when register and update periodically
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_node_health (
				hostname varchar(200) not null,
				token varchar(128) not null,
				first_seen_active_timestamp timestamp not null default '1971-01-01 00:00:00',
				last_seen_active_timestamp timestamp not null default current_timestamp,
				extra_info varchar(128) not null,
				command varchar(128) not null,
				app_version varchar(64) not null,
				db_backend varchar(255) not null,
				incrementing_indicator bigint unsigned not null default 0,

				primary key (hostname, token)
			)
		`,
		`
			create index idx_hnh_last_seen_active_timestamp on ham_node_health (last_seen_active_timestamp)
		`,
	}...)
	// hamNodeHealthHistory record node health when register, and delete when expire
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_node_health_history (
				history_id bigint unsigned not null auto_increment,
				hostname varchar(200) not null,
				token varchar(128) not null,
				first_seen_active_timestamp timestamp not null default current_timestamp,
				extra_info varchar(128) not null,
				command varchar(128) not null,
				app_version varchar(64) not null,

				primary key (history_id)
			)
		`,
		`
			create index idx_hnhh_first_seen_active_timestamp on ham_node_health_history (first_seen_active_timestamp)
		`,
		`
			create unique index uidx_hnhh_hostname_token on ham_node_health_history (hostname, token)
		`,
	}...)
	// hamKvStore record key/value info
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_kv_store (
				store_key varchar(255) not null,
				store_value text not null,
				last_updated_timestamp timestamp not null default current_timestamp,

				primary key (store_key)
			)
`,
	}...)
	// hamRaftTable hold all table and index for raft, include data/log/snapshot
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_raft_store (
				store_id bigint unsigned not null auto_increment,
				store_key varbinary(512) not null,
				store_value blob not null,

				primary key (store_id)
			)
		`,
		`
			create index idx_hrstore_store_key on ham_raft_store (store_key)
		`,
		`
			create table if not exists ham_raft_log (
				log_index bigint unsigned not null auto_increment,
				term bigint unsigned not null,
				log_type int unsigned not null,
				data blob not null,

				primary key (log_index)
			)
		`,
		`
			create table if not exists ham_raft_snapshot (
				snapshot_id bigint unsigned not null auto_increment,
				snapshot_name varchar(128) not null,
				snapshot_meta varchar(4096) not null,
				created_timestamp timestamp not null default current_timestamp,

				primary key (snapshot_id)
			)
		`,
		`
			create unique index uidx_hrsnp_snapshot_name on ham_raft_snapshot (snapshot_name)
		`,
	}...)

	// hamAgent record agent info which collect info from native database and exec command from ham4db for failure recovery or manage
	sqlBase = append(sqlBase, []string{
		`
			create table if not exists ham_agent (
				agt_hostname varchar(200) not null,
				agt_ip varchar(128) not null,
				agt_port smallint unsigned not null,
				check_interval int unsigned not null,
				last_updated_timestamp timestamp not null default current_timestamp,

				primary key (agt_hostname, agt_ip, agt_port)
			)
`,
	}...)

	return
}
