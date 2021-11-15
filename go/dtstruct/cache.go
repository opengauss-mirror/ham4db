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
package dtstruct

import (
	"fmt"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/util"
	"github.com/patrickmn/go-cache"
	"strconv"
	"sync"
	"time"
)

type Cache struct {
	sync.Mutex
	Name string
	*cache.Cache
	access float64
	miss   float64
}

// NewCache create new cache with expiration and clean up interval
func NewCache(name string, expiration time.Duration, cleanupInterval time.Duration) *Cache {
	return &Cache{Name: constant.CacheNamePrefix + name, Cache: cache.New(expiration, cleanupInterval)}
}

// GetVal return value in cache, if not, get from function `valFunc` and add to cache with default expire.
func (c *Cache) GetVal(key string, checkFunc func(interface{}) bool, valFunc func() (interface{}, error)) (interface{}, error) {
	return c.GetWithFunc(util.RandomExpirationSecond(constant.CacheExpireDefault), key, checkFunc, valFunc, func(atomC chan struct{}) { <-atomC })
}

// GetWithExpire return value in cache, if not, get from function `valFunc` and add to cache.
func (c *Cache) GetWithExpire(expire time.Duration, key string, checkFunc func(interface{}) bool, valFunc func() (interface{}, error)) (interface{}, error) {
	return c.GetWithFunc(expire, key, checkFunc, valFunc, func(atomC chan struct{}) { <-atomC })
}

// GetWithFunc return value in cache, if not, get from function `valFunc` and add to cache.
// 1. validCheck, function to check if value in cache is valid, true means valid.
// 2. valFunc, function to get value for key.
// 3. blockFunc, function for read goroutine to decide what to do when blocked by atomic channel.
func (c *Cache) GetWithFunc(expire time.Duration, key string, checkFunc func(interface{}) bool, valFunc func() (interface{}, error), blockFunc func(chan struct{})) (val interface{}, err error) {
	var atomC chan struct{}
	var write, ok bool

	defer func() {
		// reset expire time in cache
		if val != nil {
			c.SetVal(key, val, expire)
		}
		// close channel to unblock read
		if write {
			close(atomC)
		}
	}()

	// read from cache
	c.access++
	if val, ok = c.Get(key); ok && (checkFunc == nil || checkFunc(val)) {
		return
	}
	c.miss++

	// read&write concurrently
	if atomC, write = c.atomOpt(key, expire); !write {
		if blockFunc != nil {
			blockFunc(atomC)
		}

		// exist and val is valid, return
		if val, ok = c.Get(key); ok && (checkFunc == nil || checkFunc(val)) {
			return
		}

		// not exist
		if !ok {
			return nil, log.Errorf("%s: can not get value from cache for key:%s", c.Name, key)
		}

		// key is invalid
		return nil, log.Warningf("%s: key:%s is not valid", c.Name, key)
	}

	// renew one, delete old to trigger evict function
	if valFunc == nil {
		return nil, log.Errorf("%s: no value function for key:%s", c.Name, key)
	}
	if val, err = valFunc(); err == nil && (checkFunc == nil || checkFunc(val)) {
		c.Delete(key)
		c.SetValDefault(key, val)
		return
	}

	return nil, log.Errorf("%s: can not get value for %s, %s", c.Name, key, err)
}

// SetVal set key/value to cache and reset atom key expire time
func (c *Cache) SetVal(key string, value interface{}, d time.Duration) {
	c.Set(key, value, d)
	atomKey := constant.CacheAtomPrefix + key
	if val, ok := c.Get(atomKey); ok {
		c.Set(atomKey, val, atomExpire(d))
	}
}

// SetVal set key/value to cache with default expire time
func (c *Cache) SetValDefault(key string, value interface{}) {
	c.SetVal(key, value, cache.DefaultExpiration)
}

// IsExist check if key is exist in cache
func (c *Cache) IsExist(key string) (exist bool) {
	_, exist = c.Get(key)
	return
}

// HitRate get hit rate
func (c *Cache) HitRate() float64 {
	if c.access < 1 {
		return 0
	}
	rate, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", (c.access-c.miss)/c.access), 64)
	return rate
}

// atomOpt get atom chan to disable write to same key concurrently
// (chan, true) mean this is the first time to write
// (chan, false) mean someone is writing to this key, so using the chan to block until write finish, then read from cache again
func (c *Cache) atomOpt(key string, expire time.Duration) (chan struct{}, bool) {
	c.Lock()
	defer c.Unlock()
	atomKey := constant.CacheAtomPrefix + key
	var atomC interface{}
	var exist bool
	if atomC, exist = c.Get(atomKey); !exist {
		log.Infof("%s: key:%s is not exist in cache", c.Name, atomKey)
		atomC = make(chan struct{})
		c.Set(atomKey, atomC, atomExpire(expire))
	}
	return atomC.(chan struct{}), !exist
}

// atomExpire get expire time for atom key, max value is {constant.CacheExpireAtomKey}
func atomExpire(expire time.Duration) time.Duration {
	if expire.Seconds() < constant.CacheExpireAtomKey {
		return expire
	}
	return constant.CacheExpireAtomKey * time.Second
}
