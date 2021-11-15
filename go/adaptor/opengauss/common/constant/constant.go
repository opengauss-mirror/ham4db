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
package constant

const (
	DefaultAgentPort = 15000
	DBTOpenGauss     = "opengauss"

	DefaultTimeout = 10

	// role define
	OpenGaussRolePrimary = "primary"
	OpenGaussRoleStandby = "standby"
	OpenGaussRoleCascade = "cascade"

	// database status
	DBNormal    string = "normal"           //表示单主机实例
	DBPrimary          = "primary"          //表示实例为主实例。
	DBStandby          = "standby"          //表示实例为备实例。
	DBCascade          = "cascade"          //表示实例为级联备实例。
	DBSecondary        = "secondary"        //表示实例为从备实例。
	DbPending          = "pending"          //表示该实例在仲裁阶段。
	DbUnknown          = "unknown"          //表示实例状态未知。
	DBDown             = "down"             //表示实例处于宕机状态
	DbAbnormal         = "abnormal"         //表示节点处于异常状态
	DbManually         = "manually stopped" //表示节点已经被手动停止

	Normal        string = "normal"         //表示节点启动正常
	NeedRepair           = "need repair"    //当前节点需要修复
	Starting             = "starting "      //节点正在启动中
	WaitPromoting        = "wait promoting" //节点正等待升级中, 例如备机向主机发送升级请求后, 正在等待主机回应时的状态
	Promoting            = "promoting"      //备节点正在升级为主节点的状态
	Demoting             = "demoting "      //节点正在降级中, 如主机正在降为备机中
	Building             = "building "      //备机启动失败, 需要重建,
	Catchup              = "catchup"        //备节点正在追赶主节点
	Coredump             = "coredump"       //节点程序崩溃
	Unknown              = "unknown"        //节点状态未知

	QuerySQL = `
		select
			*,
			unix_timestamp() - unix_timestamp(last_checked_timestamp) as seconds_since_last_checked,
			ifnull(last_checked_timestamp <= last_seen_timestamp, 0) as is_last_check_valid,
			unix_timestamp() - unix_timestamp(last_seen_timestamp) as seconds_since_last_seen,
			ham_database_instance_candidate.last_suggested_timestamp is not null and ham_database_instance_candidate.promotion_rule in ('must', 'prefer') as is_candidate,
			ifnull(nullif(ham_database_instance_candidate.promotion_rule, ''), 'neutral') as promotion_rule,
			ifnull(unresolved_hostname, '') as unresolved_hostname,
			(ham_database_instance_downtime.downtime_active is not null and ifnull(ham_database_instance_downtime.end_timestamp, now()) > now()) as is_downtimed,
			ifnull(ham_database_instance_downtime.reason, '') as downtime_reason,
			ifnull(ham_database_instance_downtime.owner, '') as downtime_owner,
			ifnull(unix_timestamp() - unix_timestamp(begin_timestamp), 0) as elapsed_downtime_seconds,
			ifnull(ham_database_instance_downtime.end_timestamp, '') as downtime_end_timestamp
		from
			ham_database_instance
			left join ham_database_instance_candidate using (hostname, port, db_type, cluster_id)
			left join ham_hostname_unresolved using (hostname)
			left join ham_database_instance_downtime using (hostname, port)
	`
)
