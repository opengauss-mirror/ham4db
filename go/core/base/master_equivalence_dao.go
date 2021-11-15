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
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
)

func WriteMasterPositionEquivalence(master1Key *dtstruct.InstanceKey, master1BinlogCoordinates *dtstruct.LogCoordinates,
	master2Key *dtstruct.InstanceKey, master2BinlogCoordinates *dtstruct.LogCoordinates) error {
	if master1Key.Equals(master2Key) {
		// Not interesting
		return nil
	}
	writeFunc := func() error {
		_, err := db.ExecSQL(`
        	insert into mysql_master_position_equivalence (
        			master1_hostname, master1_port, master1_binary_log_file, master1_binary_log_pos,
        			master2_hostname, master2_port, master2_binary_log_file, master2_binary_log_pos,
        			last_suggested_timestamp)
        		values (?, ?, ?, ?, ?, ?, ?, ?, NOW()) 
        		on duplicate key update last_suggested_timestamp=values(last_suggested_timestamp)
				
				`, master1Key.Hostname, master1Key.Port, master1BinlogCoordinates.LogFile, master1BinlogCoordinates.LogPos,
			master2Key.Hostname, master2Key.Port, master2BinlogCoordinates.LogFile, master2BinlogCoordinates.LogPos,
		)
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}

// ExpireMasterPositionEquivalence expires old mysql_master_position_equivalence
func ExpireMasterPositionEquivalence() error {
	writeFunc := func() error {
		_, err := db.ExecSQL(`
        	delete from mysql_master_position_equivalence 
				where last_suggested_timestamp < NOW() - INTERVAL ? HOUR
				`, config.Config.UnseenInstanceForgetHours,
		)
		return log.Errore(err)
	}
	return db.ExecDBWrite(writeFunc)
}
