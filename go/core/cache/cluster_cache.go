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
	"time"
)

// clusterCache is a non-authoritative cache; used for auditing or general purpose.
var clusterCache *dtstruct.Cache

func init() {
	clusterCache = NewCache(constant.CacheClusterName, time.Duration(config.Config.InstancePollSeconds/2)*time.Second, constant.CacheClusterCleanupInterval*time.Second)
}

// GetCluster get cluster from cache for given instance key
func GetCluster(key string) (string, bool) {
	if val, exist := clusterCache.Get(key); exist {
		return val.(string), true
	}
	return "", false
}

// SetCluster set key->cluster name to cache
func SetCluster(key string, clusterName string, expire time.Duration) {
	clusterCache.Set(key, clusterName, expire)
}
