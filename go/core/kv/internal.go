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

package kv

import (
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
)

// Internal key-value store, based on relational backend
type internalKVStore struct {
}

func NewInternalKVStore() dtstruct.KVStore {
	return &internalKVStore{}
}

func (this *internalKVStore) PutKeyValue(key string, value string) (err error) {
	_, err = db.ExecSQL(`
		replace
			into ham_kv_store (
        store_key, store_value, last_updated_timestamp
			) values (
				?, ?, now()
			)
		`, key, value,
	)
	return log.Errore(err)
}

func (this *internalKVStore) GetKeyValue(key string) (value string, found bool, err error) {
	query := `
		select
			store_value
		from
			ham_kv_store
		where
      store_key = ?
		`

	err = db.Query(query, sqlutil.Args(key), func(m sqlutil.RowMap) error {
		value = m.GetString("store_value")
		found = true
		return nil
	})

	return value, found, log.Errore(err)
}

func (this *internalKVStore) PutKVPairs(kvPairs []*dtstruct.KVPair) (err error) {
	for _, pair := range kvPairs {
		if err := this.PutKeyValue(pair.Key, pair.Value); err != nil {
			return err
		}
	}
	return nil
}

func (this *internalKVStore) DistributePairs(kvPairs []*dtstruct.KVPair) (err error) {
	return nil
}
