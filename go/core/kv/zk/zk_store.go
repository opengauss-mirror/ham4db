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

package zk

import (
	"fmt"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"math/rand"
	"strings"
	"time"

	"gitee.com/opengauss/ham4db/go/config"
	zkconstants "github.com/samuel/go-zookeeper/zk"
)

// Internal key-value store, based on relational backend
type zkStore struct {
	zook *ZooKeeper
}

func normalizeKey(key string) (normalizedKey string) {
	normalizedKey = strings.TrimLeft(key, "/")
	normalizedKey = fmt.Sprintf("/%s", normalizedKey)
	return normalizedKey
}

func NewZkStore() dtstruct.KVStore {
	store := &zkStore{}

	if config.Config.ZkAddress != "" {
		rand.Seed(time.Now().UnixNano())

		serversArray := strings.Split(config.Config.ZkAddress, ",")
		zook := NewZooKeeper()
		zook.SetServers(serversArray)
		store.zook = zook
	}
	return store
}

func (this *zkStore) PutKeyValue(key string, value string) (err error) {
	if this.zook == nil {
		return nil
	}

	if _, err = this.zook.Set(normalizeKey(key), []byte(value)); err == zkconstants.ErrNoNode {
		aclstr := ""
		_, err = this.zook.Create(normalizeKey(key), []byte(value), aclstr, true)
	}
	return err
}

func (this *zkStore) GetKeyValue(key string) (value string, found bool, err error) {
	if this.zook == nil {
		return value, false, nil
	}
	result, err := this.zook.Get(normalizeKey(key))
	if err != nil {
		return value, false, err
	}
	return string(result), true, nil
}

func (this *zkStore) PutKVPairs(kvPairs []*dtstruct.KVPair) (err error) {
	if this.zook == nil {
		return nil
	}
	for _, pair := range kvPairs {
		if err := this.PutKeyValue(pair.Key, pair.Value); err != nil {
			return err
		}
	}
	return nil
}

func (this *zkStore) DistributePairs(kvPairs []*dtstruct.KVPair) (err error) {
	return nil
}
