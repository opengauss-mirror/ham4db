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

package token

import (
	"database/sql"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
)

// TODO field `is_reentrant` never be updated, so maybe remove this field in future version

// GenerateAccessToken attempts to generate a new access token and returns the public part of the token
func GenerateAccessToken(owner string) (publicToken string, err error) {
	publicToken = dtstruct.NewToken().Hash
	if _, err = db.ExecSQL(`
				insert into ham_access_token (
					public_token, secret_token, generate_timestamp, generate_by, is_acquired, is_reentrant
				) values (
					?, ?, current_timestamp, ?, 0, 0
				)
			`, publicToken, dtstruct.NewToken().Hash, owner,
	); err != nil {
		return "", log.Errore(err)
	}
	return
}

// AcquireAccessToken attempts to acquire a hopefully free token; returning in such case
// the secretToken as proof of ownership.
func AcquireAccessToken(publicToken string) (secretToken string, err error) {

	// update public token acquired value
	var sqlResult sql.Result
	if sqlResult, err = db.ExecSQL(`
				update 
					ham_access_token
				set
					is_acquired = 1,
					acquired_timestamp = current_timestamp
				where
					public_token = ? and
					(
						(
							is_acquired = 0 and current_timestamp - generate_timestamp < ?
						) or 
						is_reentrant = 1
					)
			`,
		publicToken, config.Config.AccessTokenUseExpirySeconds,
	); err != nil {
		return secretToken, log.Errore(err)
	}
	var rows int64
	if rows, err = sqlResult.RowsAffected(); err != nil {
		return secretToken, log.Errore(err)
	}
	if rows == 0 {
		return secretToken, log.Errorf("cannot acquire token:%s", publicToken)
	}

	// get secret token according to this public
	query := `select secret_token from ham_access_token where public_token = ?`
	err = db.Query(query, sqlutil.Args(publicToken), func(m sqlutil.RowMap) error {
		secretToken = m.GetString("secret_token")
		return nil
	})
	return secretToken, log.Errore(err)
}

// IsTokenValid checks to see whether a given token exists and is not outdated.
func IsTokenValid(publicToken string, secretToken string) (result bool, err error) {
	query := `
			select
				count(*) as cnt
			from
				ham_access_token
			where
				public_token = ? and
				secret_token = ? and 
				(
					(current_timestamp - generate_timestamp <= ? * 60) or is_reentrant = 1
				)
		`
	err = db.Query(query, sqlutil.Args(publicToken, secretToken, config.Config.AccessTokenExpiryMinutes), func(m sqlutil.RowMap) error {
		result = m.GetInt("cnt") > 0
		return nil
	})
	return result, log.Errore(err)
}

// ExpireAccessToken removes old, known to be ineligible tokens
func ExpireAccessToken() error {
	_, err := db.ExecSQL(`
				delete
					from ham_access_token
				where
					(current_timestamp - generate_timestamp <= ? * 60) and is_reentrant = 0
			`,
		config.Config.AccessTokenExpiryMinutes,
	)
	return log.Errore(err)
}
