/*
   Copyright 2014 Outbrain Inc.

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
	"fmt"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"github.com/patrickmn/go-cache"
	"strings"
	"time"
)

var logCache *dtstruct.Cache

func init() {
	logCache = NewCache(constant.CacheLogName, constant.CacheLogExpire, constant.CacheLogCleanupInterval*time.Second)
}

// ClearToLog add topic with key to cache
func ClearToLog(topic string, key string) bool {
	return logCache.Add(fmt.Sprintf("%s:%s", topic, key), true, cache.DefaultExpiration) == nil
}

// LogReadTopologyInstanceError logs an error, if applicable, for a ReadTopologyInstance operation,
// providing context and hint as for the source of the error. If there's no hint just provide the original error.
func LogReadTopologyInstanceError(instanceKey *dtstruct.InstanceKey, hint string, err error) error {
	if err == nil {
		return nil
	}
	if !ClearToLog("ReadTopologyInstance", instanceKey.StringCode()) {
		return err
	}
	var msg string
	if hint == "" {
		msg = fmt.Sprintf("ReadTopologyInstance(%+v): %+v", *instanceKey, err)
	} else {
		msg = fmt.Sprintf("ReadTopologyInstance(%+v) %+v: %+v", *instanceKey, strings.Replace(hint, "%", "%%", -1), err)
	}
	return log.Errorf(msg)
}
