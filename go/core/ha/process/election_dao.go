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

package process

import (
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/consensus/raft"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/core/system/osp"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
)

// AttemptElection tries to grab leadership (become active node)
func AttemptElection() (bool, error) {
	//multiSQL := &dtstruct.MultiSQL{}
	//multiSQL.Query = append(multiSQL.Query, `
	//	insert ignore into ham_node_active (
	//		anchor, hostname, token, first_seen_active_timestamp, last_seen_active_timestamp
	//	) values (
	//		1, ?, ?, now(), now()
	//	)
	//`)
	//multiSQL.Args = append(multiSQL.Args, []interface{}{osp.GetHostname(), dtstruct.ProcessToken.Hash})
	//db.ExecMultiSQL(multiSQL)

	{
		sqlResult, err := db.ExecSQL(`
		insert ignore into ham_node_active (
				anchor, hostname, token, first_seen_active_timestamp, last_seen_active_timestamp
			) values (
				1, ?, ?, now(), now()
			)
		`,
			osp.GetHostname(), dtstruct.ProcessToken.Hash,
		)
		if err != nil {
			return false, log.Errore(err)
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			return false, log.Errore(err)
		}
		if rows > 0 {
			// We managed to insert a row
			return true, nil
		}
	}
	{
		// takeover from a node that has been inactive
		sqlResult, err := db.ExecSQL(`
			update ham_node_active set
				hostname = ?,
				token = ?,
				first_seen_active_timestamp=now(),
				last_seen_active_timestamp=now()
			where
				anchor = 1 and 
				last_seen_active_timestamp < (now() - interval ? second)
		`,
			osp.GetHostname(), dtstruct.ProcessToken.Hash, config.ActiveNodeExpireSeconds,
		)
		if err != nil {
			return false, log.Errore(err)
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			return false, log.Errore(err)
		}
		if rows > 0 {
			// We managed to update a row: overtaking a previous leader
			return true, nil
		}
	}
	{
		// Update last_seen_active is this very node is already the active node
		sqlResult, err := db.ExecSQL(`
			update ham_node_active set
				last_seen_active_timestamp=now()
			where
				anchor = 1
				and hostname = ?
				and token = ?
		`,
			osp.GetHostname(), dtstruct.ProcessToken.Hash,
		)
		if err != nil {
			return false, log.Errore(err)
		}
		rows, err := sqlResult.RowsAffected()
		if err != nil {
			return false, log.Errore(err)
		}
		if rows > 0 {
			// Reaffirmed our own leadership
			return true, nil
		}
	}
	return false, nil
}

// GrabElection forcibly grabs leadership. Use with care!!
func GrabElection() error {
	if orcraft.IsRaftEnabled() {
		return log.Errorf("Cannot GrabElection on raft setup")
	}
	_, err := db.ExecSQL(`
			replace into ham_node_active (
					anchor, hostname, token, first_seen_active_timestamp, last_seen_active_timestamp
				) values (
					1, ?, ?, now(), now()
				)
			`,
		osp.GetHostname(), dtstruct.ProcessToken.Hash,
	)
	return log.Errore(err)
}

// Reelect clears the way for re-elections. Active node is immediately demoted.
func Reelect() error {
	if orcraft.IsRaftEnabled() {
		orcraft.StepDown()
	}
	_, err := db.ExecSQL(`delete from ham_node_active where anchor = 1`)
	return log.Errore(err)
}

// ElectedNode returns the details of the elected node, as well as answering the question "is this process the elected one"?
func ElectedNode() (node NodeHealth, isElected bool, err error) {
	query := `
		select
			hostname,
			token,
			first_seen_active_timestamp,
			last_seen_Active_timestamp
		from
			ham_node_active
		where
			anchor = 1
		`
	err = db.Query(query, nil, func(m sqlutil.RowMap) error {
		node.Hostname = m.GetString("hostname")
		node.Token = m.GetString("token")
		node.FirstSeenActive = m.GetString("first_seen_active_timestamp")
		node.LastSeenActive = m.GetString("last_seen_active_timestamp")

		return nil
	})

	isElected = node.Hostname == osp.GetHostname() && node.Token == dtstruct.ProcessToken.Hash
	return node, isElected, log.Errore(err)
}
