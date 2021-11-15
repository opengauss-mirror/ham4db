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
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"time"
)

var cacheList []*dtstruct.Cache

// NewCache create a new cache and append to cache list
func NewCache(name string, expiration time.Duration, cleanupInterval time.Duration) *dtstruct.Cache {
	cache := dtstruct.NewCache(name, expiration, cleanupInterval)
	cacheList = append(cacheList, cache)
	return cache
}

// ClearCache exec delete expired key on all cache in list
func ClearCache() {
	for _, c := range cacheList {
		c.DeleteExpired()
	}
}

// ShowCacheHit show all cache hit rate
func ShowCacheHit() map[string]float64 {
	hitMap := make(map[string]float64, len(cacheList))
	for _, cache := range cacheList {
		hitMap[cache.Name] = cache.HitRate()
	}
	return hitMap
}
