/*
   Copyright 2016 Simon J Mudd

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

package base

import (
	"fmt"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"

	"gitee.com/opengauss/ham4db/go/config"
)

// RegisterCandidateInstance markes a given instance as suggested for successoring a master in the event of failover.
func RegisterCandidateInstance(candidate *dtstruct.CandidateDatabaseInstance) error {
	if candidate.LastSuggestedString == "" {
		WithCurrentTime(candidate)
	}
	args := sqlutil.Args(candidate.Hostname, candidate.Port, candidate.DBType, candidate.ClusterId, string(candidate.PromotionRule), candidate.LastSuggestedString)
	query := fmt.Sprintf(`
			insert into ham_database_instance_candidate (
					hostname,
					port,
					db_type,
					cluster_id,
					promotion_rule,
					last_suggested_timestamp
				) values (
					?, ?, ?, ?, ?, ?
				) on duplicate key update
					last_suggested_timestamp=values(last_suggested_timestamp),
					promotion_rule=values(promotion_rule)
			`)
	writeFunc := func() error {
		_, err := db.ExecSQL(query, args...)
		AuditOperation(constant.AuditRegisterCandidate, candidate.Key(), "", string(candidate.PromotionRule))
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}

// ExpireCandidateInstances removes stale master candidate suggestions.
func ExpireCandidateInstances() error {
	writeFunc := func() error {
		_, err := db.ExecSQL(`
				delete from ham_database_instance_candidate
				where last_suggested_timestamp < NOW() - INTERVAL ? MINUTE
				`, config.Config.CandidateInstanceExpireMinutes,
		)
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}

// BulkReadCandidateDatabaseInstance returns a slice of
// CandidateDatabaseInstance converted to JSON.
/*
root@my [ham4db]> select * from ham_database_instance_candidate;
+-------------------+------+---------------------+----------+----------------+
| hostname          | port | last_suggested_timestamp      | priority | promotion_rule |
+-------------------+------+---------------------+----------+----------------+
| host1.example.com | 3306 | 2016-11-22 17:41:06 |        1 | prefer         |
| host2.example.com | 3306 | 2016-11-22 17:40:24 |        1 | prefer         |
+-------------------+------+---------------------+----------+----------------+
2 rows in set (0.00 sec)
*/
func BulkReadCandidateDatabaseInstance() ([]dtstruct.CandidateDatabaseInstance, error) {
	var candidateDatabaseInstances []dtstruct.CandidateDatabaseInstance

	// Read all promotion rules from the table
	query := `
		SELECT
			hostname,
			port,
			db_type,
			cluster_id,
			promotion_rule,
			last_suggested_timestamp,
			last_suggested_timestamp + INTERVAL ? MINUTE AS promotion_rule_expiry
		FROM
			ham_database_instance_candidate
	`
	err := db.Query(query, sqlutil.Args(config.Config.CandidateInstanceExpireMinutes), func(m sqlutil.RowMap) error {
		cdi := dtstruct.CandidateDatabaseInstance{
			InstanceKey: dtstruct.InstanceKey{
				Hostname:  m.GetString("hostname"),
				Port:      m.GetInt("port"),
				DBType:    m.GetString("db_type"),
				ClusterId: m.GetString("cluster_id"),
			},
			PromotionRule:       dtstruct.CandidatePromotionRule(m.GetString("promotion_rule")),
			LastSuggestedString: m.GetString("last_suggested_timestamp"),
			PromotionRuleExpiry: m.GetString("promotion_rule_expiry"),
		}
		// add to end of candidateDatabaseInstances
		candidateDatabaseInstances = append(candidateDatabaseInstances, cdi)

		return nil
	})
	return candidateDatabaseInstances, err
}
