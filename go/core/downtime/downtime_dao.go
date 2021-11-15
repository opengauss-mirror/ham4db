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

package downtime

import (
	"database/sql"
	"fmt"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/instance"
	"gitee.com/opengauss/ham4db/go/dtstruct"

	"time"
)

// ExpireDowntime will remove the maintenance flag on old downtime
func ExpireDowntime(dbt string) (err error) {
	if err = renewLostInRecoveryDowntime(); err != nil {
		return
	}
	if err = expireLostInRecoveryDowntime(dbt); err != nil {
		return
	}
	var res sql.Result
	if res, err = db.ExecSQL(`delete from ham_database_instance_downtime where end_timestamp < current_timestamp`); err == nil {
		if rowsAffected, _ := res.RowsAffected(); rowsAffected > 0 {
			base.AuditOperation(constant.AuditDowntimeExpire, nil, "", fmt.Sprintf("expired %d entries", rowsAffected))
		}
	}
	return
}

// renewLostInRecoveryDowntime renews hosts who are made as downtime due to being lost in recovery, such that
// their downtime never expire.
func renewLostInRecoveryDowntime() error {
	_, err := db.ExecSQL(`update ham_database_instance_downtime set end_timestamp = current_timestamp + ?
			where end_timestamp > current_timestamp and reason = ?
		`, constant.DowntimeSecond, constant.DowntimeReasonLostInRecovery,
	)
	return err
}

// expireLostInRecoveryDowntime expires downtime for servers who have been lost in recovery in the last,
// but are now replicating.
func expireLostInRecoveryDowntime(dbt string) (err error) {

	// get instance list that need to end downtime
	var instanceList []dtstruct.InstanceAdaptor
	if instanceList, err = instance.ReadInstanceLostInRecovery(dbt, ""); err != nil || len(instanceList) == 0 {
		return
	}

	// get cluster alias for instance
	var unambiguousAlias map[string]dtstruct.InstanceKey
	if unambiguousAlias, err = base.ReadUnambiguousSuggestedClusterAlias(); err != nil {
		return
	}

	// check instance and end its downtime
	for _, inst := range instanceList {
		// We _may_ expire this downtime, but only after a minute
		// This is a graceful period, during which other servers can claim ownership of the alias,
		// or can update their own cluster name to match a new master's name
		if inst.GetInstance().ElapsedDowntime < time.Minute || !inst.GetInstance().IsLastCheckValid {
			continue
		}
		endDowntime := false
		if inst.ReplicaRunning() {
			// back, alive, replicating in some topology
			endDowntime = true
		} else if inst.GetInstance().ReplicationDepth == 0 {
			// instance makes the appearance of a master
			if unambiguousKey, ok := unambiguousAlias[inst.GetInstance().SuggestedClusterAlias]; ok {
				if unambiguousKey.Equals(&inst.GetInstance().Key) {
					// This instance seems to be a master, which is valid, and has a suggested alias,
					// and is the _only_ one to have this suggested alias (i.e. no one took its place)
					endDowntime = true
				}
			}
		}
		if endDowntime {
			if _, err = base.EndDowntime(&inst.GetInstance().Key); err != nil {
				return
			}
		}
	}
	return
}
