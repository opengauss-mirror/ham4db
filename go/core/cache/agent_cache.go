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
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util"
	"time"
)

var agentCache *dtstruct.Cache

func init() {
	agentCache = NewCache(constant.CacheAgentName, constant.CacheAgentExpire, constant.CacheAgentCleanupInterval*time.Second)
}

// GetAgent using default expire when get from cache.
func GetAgent(addr string, valFunc func() (interface{}, error)) (*dtstruct.Agent, error) {
	return GetAgentWithExpire(util.RandomExpirationSecond(constant.CacheAgentExpire), addr, valFunc)
}

// GetAgentWithExpire return agent in cache, if not, get from function valFunc() and add to cache.
func GetAgentWithExpire(expire time.Duration, addr string, valFunc func() (interface{}, error)) (agt *dtstruct.Agent, err error) {
	val, err := agentCache.GetWithExpire(
		expire,
		addr,
		func(val interface{}) bool {
			return !IsExpire(val.(*dtstruct.Agent))
		},
		valFunc,
	)
	if err == nil {
		return val.(*dtstruct.Agent), nil
	}
	return nil, log.Errorf("can not get agent for key: %s, %s", addr, err)
}

// IsExpire true if too long since last health check.
func IsExpire(agt *dtstruct.Agent) bool {
	return time.Now().Sub(agt.LastUpdate).Seconds() > float64(agt.Interval)
}
