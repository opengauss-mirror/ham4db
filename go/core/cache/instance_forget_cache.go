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

var forgetCache *dtstruct.Cache

func init() {
	forgetCache = NewCache(constant.CacheInstanceForgetName, time.Duration(config.Config.InstancePollSeconds*3)*time.Second, constant.CacheInstanceForgetCleanupInterval*time.Second)
}

// ForgetInstance set instance key to cache
func ForgetInstance(key string, forget bool) {
	forgetCache.SetDefault(key, forget)
}

// IsExistInstanceForget check if instance is in cache
func IsExistInstanceForget(key string) bool {
	return forgetCache.IsExist(key)
}
