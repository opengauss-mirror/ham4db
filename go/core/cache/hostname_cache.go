/*
	Copyright 2021 SANGFOR TECHNOLOGIES

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
package cache

import (
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"github.com/patrickmn/go-cache"
	"strings"
	"time"
)

// key: hostname or ip, value: resolved hostname
var hostnameCache *dtstruct.Cache

func init() {
	hostnameCache = NewCache(constant.CacheHostnameName, time.Duration(config.Config.ExpiryHostnameResolvesMinutes)*time.Minute, constant.CacheHostnameCleanupInterval*time.Second)
}

// GetHostname get hostname from cache
func GetHostname(hostname string, valFunc func() (interface{}, error)) (string, bool) {

	// get from cache, if value is nil or empty, should be get from valFunc
	if val, err := hostnameCache.GetVal(hostname,
		func(i interface{}) bool {
			return i != nil && i.(string) != ""
		},
		valFunc,
	); err == nil {
		return val.(string), true
	}

	// just return hostname
	return hostname, false
}

// SetHostname put hostname to cache
func SetHostname(hostname string, resolvedHost string, expire time.Duration) {
	hostnameCache.SetVal(hostname, resolvedHost, expire)
}

// FlushHostname flush hostname cache
func FlushHostname() {
	hostnameCache.Flush()
}

// ItemHostname get item in cache
func ItemHostname() map[string]cache.Item {
	itemMap := make(map[string]cache.Item)
	for key, value := range hostnameCache.Items() {
		if !strings.HasPrefix(key, constant.CacheAtomPrefix) {
			itemMap[key] = value
		}
	}
	return itemMap
}
