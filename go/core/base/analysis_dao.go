/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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
	"gitee.com/opengauss/ham4db/go/core/cache"

	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"github.com/rcrowley/go-metrics"
)

var analysisChangeWriteAttemptCounter = metrics.NewCounter()
var analysisChangeWriteCounter = metrics.NewCounter()

func init() {
	metrics.Register(constant.MetricAnalysisChangeWriteAttempt, analysisChangeWriteAttemptCounter)
	metrics.Register(constant.MetricAnalysisChangeWrite, analysisChangeWriteCounter)
}

// AuditInstanceAnalysisInChangelog will write down an instance's analysis in the database_instance_analysis_changelog table.
// To not repeat recurring analysis code, the database_instance_last_analysis table is used, so that only changes to
// analysis codes are written.
func AuditInstanceAnalysisInChangelog(instanceKey *dtstruct.InstanceKey, analysisCode dtstruct.AnalysisCode) error {

	// get from cache
	if lastWrittenAnalysis, found := cache.GetAnalysis(instanceKey.DisplayString()); found {
		// Surely nothing new. let's expand the timeout
		if lastWrittenAnalysis == analysisCode {
			cache.SetAnalysis(instanceKey.DisplayString(), analysisCode)
			return nil
		}
	}
	// Passed the cache; but does database agree that there's a change? Here's a persistent cache; this comes here
	// to verify no two ham4db services are doing this without coordinating (namely, one dies, the other taking its place
	// and has no familiarity of the former's cache)
	analysisChangeWriteAttemptCounter.Inc(1)

	// update analysis in database
	lastAnalysisChanged := false
	sqlResult, err := db.ExecSQL(`
		update ham_database_instance_last_analysis 
		set analysis = ?, analysis_timestamp = now()
		where
			hostname = ? and port = ? and analysis != ?
		`,
		string(analysisCode), instanceKey.Hostname, instanceKey.Port, string(analysisCode),
	)
	if err != nil {
		return err
	}
	rows, err := sqlResult.RowsAffected()
	if err != nil {
		return log.Errore(err)
	}
	lastAnalysisChanged = rows > 0

	// not exist in database, insert a new record and put it to cache
	if !lastAnalysisChanged {
		if _, err = db.ExecSQL(`
				insert ignore into ham_database_instance_last_analysis (
					hostname, port, analysis_timestamp, analysis
				) values (
					?, ?, now(), ?
				)
			`,
			instanceKey.Hostname, instanceKey.Port, string(analysisCode),
		); err != nil {
			return err
		}
	}
	cache.SetAnalysis(instanceKey.DisplayString(), analysisCode)
	if !lastAnalysisChanged {
		return nil
	}

	// new record to table change log
	if _, err = db.ExecSQL(`
		insert into ham_database_instance_analysis_changelog (
			hostname, port, analysis_timestamp, analysis
		) values (
			?, ?, now(), ?
		)
		`,
		instanceKey.Hostname, instanceKey.Port, string(analysisCode),
	); err == nil {
		analysisChangeWriteCounter.Inc(1)
	}
	return err
}

// ExpireInstanceAnalysisChangelog removes old-enough analysis entries from the changelog
func ExpireInstanceAnalysisChangelog() (err error) {
	_, err = db.ExecSQL(
		`delete from ham_database_instance_analysis_changelog where analysis_timestamp < current_timestamp - interval ? hour`,
		config.Config.UnseenInstanceForgetHours,
	)
	return
}

// ReadReplicationAnalysisChangelog query all analysis change log and group by instance
func ReadReplicationAnalysisChangelog() (res []*dtstruct.ReplicationAnalysisChangelog, err error) {
	analysisChangelog := &dtstruct.ReplicationAnalysisChangelog{}
	err = db.Query(
		`
			select 
				hostname, port, analysis_timestamp, analysis
			from 
				ham_database_instance_analysis_changelog
			order by 
				hostname, port, changelog_id
		`,
		nil,
		func(m sqlutil.RowMap) error {
			key := dtstruct.InstanceKey{Hostname: m.GetString("hostname"), Port: m.GetInt("port")}
			if !analysisChangelog.AnalyzedInstanceKey.Equals(&key) {
				analysisChangelog = &dtstruct.ReplicationAnalysisChangelog{AnalyzedInstanceKey: key, Changelog: []string{}}
				res = append(res, analysisChangelog)
			}
			analysisChangelog.Changelog = append(analysisChangelog.Changelog, fmt.Sprintf("%s;%s,", m.GetString("analysis_timestamp"), m.GetString("analysis")))
			return nil
		})
	return
}
