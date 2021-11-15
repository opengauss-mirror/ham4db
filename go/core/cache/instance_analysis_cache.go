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

var analysisCache *dtstruct.Cache

func init() {
	analysisCache = NewCache(constant.CacheInstanceAnalysisName, time.Duration(config.RecoveryPollSeconds*2)*time.Second, constant.CacheInstanceAnalysisCleanupInterval*time.Second)
}

// GetAnalysis get analysis from cache
func GetAnalysis(key string) (dtstruct.AnalysisCode, bool) {
	if val, exist := analysisCache.Get(key); exist {
		return val.(dtstruct.AnalysisCode), true
	}
	return "", false
}

// SetAnalysis put instance analysis code to cache
func SetAnalysis(key string, analysisCode dtstruct.AnalysisCode) {
	analysisCache.Set(key, analysisCode, constant.CacheNotExpire)
}
