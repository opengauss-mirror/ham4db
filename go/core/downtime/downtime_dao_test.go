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
package downtime

import (
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/test"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
	"time"
)

func init() {
	test.DBTestInit()
}

func TestExpireDowntime(t *testing.T) {

	// insert new record to table
	res, err := db.ExecSQL(`insert into ham_database_instance values 
		(
			185465,
			'host_mysql_3308',
			3306,
			'xxxxx-yyyyy-zzzzz-aaaaa-bbbbb-cc',
			'host_mysql_3308:3306',
			'host_mysql_3308',
			'mysql',
			'7376416c-bd62-11eb-854b-0242ac120003',
			'5.7.34-log',
			'',
			NULL,
			NULL,
			'',
			'',
			NULL,
			NULL,
			'2021-07-10 04:21:17',
			'2021-07-10 04:21:17',
			'2021-07-10 04:21:17',
			1,
			'',
			0,
			'[{\"DBType\":\"\",\"Hostname\":\"mysql_3310.for_mysql\",\"Port\":3306},{\"DBType\":\"\",\"Hostname\":\"mysql_3311.for_mysql\",\"Port\":3306}]',
			2,
			NULL,
			0,
			NULL,
			0,
			0,
			0,
			0,
			0,
			0
		),
		(
			185462,
			'host_mysql_3309',
			3306,
			'xxxxx-yyyyy-zzzzz-aaaaa-bbbbb-cc',
			'host_mysql_3309:3306',
			'host_mysql_3309',
			'mysql',
			'926465cf-bee6-11eb-bb41-0242ac120004',
			'5.7.34-log',
			'',
			NULL,
			NULL,
			'',
			'',
			NULL,
			NULL,
			'2021-07-10 04:21:17',
			'2021-07-10 04:21:17',
			'2021-07-10 04:21:17',
			1,
			'//host_mysql_3312',
			3306,
			'[]',
			0,
			NULL,
			0,
			NULL,
			0,
			0,
			0,
			1,
			0,
			1
		),(
			1497696,
			'host_mysql_3310',
			3306,
			'xxxxx-yyyyy-zzzzz-aaaaa-bbbbb-cc',
			'host_mysql_3308:3306',
			'host_mysql_3308',
			'mysql',
			'659e59ac-c50f-11eb-b050-0242ac120005',
			'5.7.34-log',
			'',
			NULL,
			NULL,
			'',
			'',
			NULL,
			NULL,
			'2021-07-05 09:37:48',
			'2021-07-05 09:37:48',
			'2021-07-05 09:37:48',
			1,
			'host_mysql_3308',
			3306,
			'[{\"DBType\":\"mysql\",\"Hostname\":\"172.18.0.7\",\"Port\":3306}]',
			1,
			NULL,
			1,
			NULL,
			0,
			0,
			0,
			1,
			0,
			1
		),
		(
			185449,
			'host_mysql_3311',
			3306,
			'xxxxx-yyyyy-zzzzz-aaaaa-bbbbb-cc',
			'host_mysql_3308:3306',
			'host_mysql_3308',
			'mysql',
			'7a6a803d-c50f-11eb-ad79-0242ac120006',
			'5.7.34-log',
			'',
			NULL,
			NULL,
			'',
			'',
			NULL,
			NULL,
			'2021-07-10 04:21:17',
			'2021-07-10 04:21:17',
			'2021-07-10 04:21:17',1,
			'host_mysql_3308',
			3306,
			'[]',
			0,
			NULL,
			1,
			NULL,
			0,
			0,
			0,
			1,
			0,
			1
		),
		(
			185446,
			'host_mysql_3312',
			3306,
			'xxxxx-yyyyy-zzzzz-aaaaa-bbbbb-cc',
			'host_mysql_3308:3306',
			'host_mysql_3308',
			'mysql',
			'0b8f1d71-dd6a-11eb-8418-0242ac120007',
			'5.7.34-log',
			'',
			NULL,
			NULL,
			'',
			'',
			NULL,
			NULL,
			'2021-07-10 04:21:17',
			'2021-07-10 04:21:17',
			'2021-07-10 04:21:17',
			1,
			'host_mysql_3310',
			3306,
			'[]',
			0,
			NULL,
			2,
			NULL,
			0,
			0,
			0,
			1,
			0,
			1
		)
`)
	tests.S(t).ExpectNil(err)
	cnt, _ := res.RowsAffected()
	tests.S(t).ExpectTrue(cnt == 5)

	res, err = db.ExecSQL("insert into ham_database_instance_downtime values (?,?,?,?,?,?,?)", sqlutil.Args("host_mysql_3308", 3306, 1, time.Now(), time.Now().Add(time.Duration(constant.DowntimeSecond)), "abc", constant.DowntimeReasonLostInRecovery)...)
	tests.S(t).ExpectNil(err)
	cnt, _ = res.RowsAffected()
	tests.S(t).ExpectTrue(cnt == 1)

	renewLostInRecoveryDowntime()

	//expireLostInRecoveryDowntime()

	ExpireDowntime(constant.TestDB)
}
