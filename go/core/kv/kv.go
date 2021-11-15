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
	"gitee.com/opengauss/ham4db/go/core/kv/consul"
	"gitee.com/opengauss/ham4db/go/core/kv/zk"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"sync"

	"gitee.com/opengauss/ham4db/go/config"
)

var kvMutex sync.Mutex
var kvInitOnce sync.Once
var kvStores = []dtstruct.KVStore{}

// InitKVStores initializes the KV stores (duh), once in the lifetime of this app.
// Configuration reload does not affect a running instance.
func InitKVStores() {
	kvMutex.Lock()
	defer kvMutex.Unlock()

	kvInitOnce.Do(func() {
		kvStores = []dtstruct.KVStore{
			NewInternalKVStore(),
			zk.NewZkStore(),
		}
		switch config.Config.ConsulKVStoreProvider {
		case "consul-txn", "consul_txn":
			kvStores = append(kvStores, consul.NewConsulTxnStore())
		default:
			kvStores = append(kvStores, consul.NewConsulStore())
		}
	})
}

func getKVStores() (stores []dtstruct.KVStore) {
	kvMutex.Lock()
	defer kvMutex.Unlock()

	stores = kvStores
	return stores
}

func GetValue(key string) (value string, found bool, err error) {
	for _, store := range getKVStores() {
		// It's really only the first (internal) that matters here
		return store.GetKeyValue(key)
	}
	return value, found, err
}

func PutValue(key string, value string) (err error) {
	for _, store := range getKVStores() {
		if err := store.PutKeyValue(key, value); err != nil {
			return err
		}
	}
	return nil
}

func PutKVPairs(kvPairs []*dtstruct.KVPair) (err error) {
	if len(kvPairs) < 1 {
		return nil
	}
	for _, store := range getKVStores() {
		if err := store.PutKVPairs(kvPairs); err != nil {
			return err
		}
	}
	return nil
}

func DistributePairs(kvPairs [](*dtstruct.KVPair)) (err error) {
	for _, store := range getKVStores() {
		if err := store.DistributePairs(kvPairs); err != nil {
			return err
		}
	}
	return nil
}
