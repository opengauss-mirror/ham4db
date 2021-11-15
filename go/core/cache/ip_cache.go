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
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"net"
	"time"
)

var ipCache *dtstruct.Cache

func init() {
	ipCache = NewCache(constant.CacheIPName, constant.CacheIPExpire*time.Second, constant.CacheIPCleanupInterval*time.Second)
}

// GetIP get ip from cache, if not exist, get new and put it to cache
func GetIP(hostname string, valFunc func() (interface{}, error)) ([]net.IP, error) {
	val, err := ipCache.GetVal(hostname, nil, valFunc)
	if err == nil {
		return val.([]net.IP), nil
	}
	return []net.IP{}, err
}
