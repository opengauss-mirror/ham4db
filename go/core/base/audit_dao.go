/*
   Copyright 2014 Outbrain Inc.

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
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"log/syslog"
	"os"
	"time"

	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"github.com/rcrowley/go-metrics"
)

// syslogWriter is optional, and defaults to nil (disabled)
var syslogWriter *syslog.Writer

// auditCounter count audit times
var auditCounter = metrics.NewCounter()

func init() {
	metrics.Register(constant.MetricAuditOpt, auditCounter)
}

// EnableSyslog enables, if possible, writes to syslog. These will execute in addition to normal logging
func EnableSyslog() (err error) {
	if syslogWriter, err = syslog.New(syslog.LOG_ERR, "ham4db"); err != nil {
		syslogWriter = nil
	}
	return log.Errore(err)
}

// AuditOperation creates and writes a new audit entry by given params
func AuditOperation(auditType string, instanceKey *dtstruct.InstanceKey, clusterName string, message string) {

	// check instance key, use a empty instance if nil
	if instanceKey == nil {
		instanceKey = &dtstruct.InstanceKey{}
	}

	// if write to file
	isWriteToFile := false
	if config.Config.AuditLogFile != "" {
		isWriteToFile = true
		go func() {
			f, err := os.OpenFile(config.Config.AuditLogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0640)
			if err != nil {
				log.Erroref(err)
			}
			defer f.Close()
			if _, err = f.WriteString(fmt.Sprintf("%s\t%s\t%s\t%d\t[%s]\t%s\t\n", time.Now().Format(constant.DateFormatLog), auditType, instanceKey.Hostname, instanceKey.Port, clusterName, message)); err != nil {
				log.Erroref(err)
			}
		}()
	}

	// if write to database
	if config.Config.AuditToBackendDB {
		_, _ = db.ExecSQL(`
				insert into 
					ham_audit (audit_timestamp, audit_type, hostname, port, cluster_id, cluster_name, message) 
				values (
					current_timestamp, ?, ?, ?, ?, ?, ?
				)
			`,
			auditType, instanceKey.Hostname, instanceKey.Port, instanceKey.ClusterId, clusterName, message,
		)
	}

	// if write to system log
	logMessage := fmt.Sprintf("auditType:%s instance:%s cluster:%s message:%s", auditType, instanceKey.DisplayString(), clusterName, message)
	if syslogWriter != nil {
		isWriteToFile = true
		go func() {
			_ = syslogWriter.Info(logMessage)
		}()
	}

	// if write to log file
	if !isWriteToFile {
		log.Infof(logMessage)
	}

	auditCounter.Inc(1)
	return
}

//////////////////////////////////////////

// ReadRecentAudit returns a list of audit entries order chronologically descending, using page number.
func ReadRecentAudit(instanceKey *dtstruct.InstanceKey, page int) ([]dtstruct.Audit, error) {
	res := []dtstruct.Audit{}
	args := sqlutil.Args()
	whereCondition := ``
	if instanceKey != nil {
		whereCondition = `where hostname=? and port=?`
		args = append(args, instanceKey.Hostname, instanceKey.Port)
	}
	query := fmt.Sprintf(`
		select
			audit_id,
			audit_timestamp,
			audit_type,
			hostname,
			port,
			cluster_id,
			message
		from
			ham_audit
		%s
		order by
			audit_timestamp desc
		limit ?
		offset ?
		`, whereCondition)
	args = append(args, config.AuditPageSize, page*config.AuditPageSize)
	err := db.Query(query, args, func(m sqlutil.RowMap) error {
		audit := dtstruct.Audit{}
		audit.AuditId = m.GetInt64("audit_id")
		audit.AuditTimestamp = m.GetString("audit_timestamp")
		audit.AuditType = m.GetString("audit_type")
		audit.AuditInstanceKey.Hostname = m.GetString("hostname")
		audit.AuditInstanceKey.Port = m.GetInt("port")
		audit.AuditInstanceKey.ClusterId = m.GetString("cluster_id")
		audit.Message = m.GetString("message")

		res = append(res, audit)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err

}

// ExpireAudit removes old rows from the audit table
func ExpireAudit() error {
	return db.ExecDBWrite(func() error {
		_, err := db.ExecSQL("delete from ham_audit where audit_timestamp < NOW() - INTERVAL ? DAY", config.Config.AuditPurgeDays)
		return err
	})
}
