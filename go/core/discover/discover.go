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
package discover

import (
	"context"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/cache"
	cinstance "gitee.com/opengauss/ham4db/go/core/instance"
	"gitee.com/opengauss/ham4db/go/core/log"
	ometrics "gitee.com/opengauss/ham4db/go/core/metric"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util"
	"github.com/opentracing/opentracing-go"
	"github.com/rcrowley/go-metrics"
	"github.com/sjmudd/stopwatch"
	"sync"
	"time"
)

// discoveryQueue is a channel of deduplicated instanceKey-s
// that were requested for discovery.  It can be continuously updated
// as discovery process progresses.
var DiscoveryQueue *dtstruct.Queue

var instancePollSecondsExceededCounter = metrics.NewCounter()
var discoveriesCounter = metrics.NewCounter()
var failedDiscoveriesCounter = metrics.NewCounter()
var discoveryQueueLengthGauge = metrics.NewGauge()

var snapshotDiscoveryKeysMutex sync.Mutex
var SnapshotDiscoveryKeys chan dtstruct.InstanceKey

func init() {
	DiscoveryQueue = dtstruct.CreateOrReturnQueue("DEFAULT", config.Config.DiscoveryQueueCapacity, config.Config.DiscoveryQueueMaxStatisticsSize, config.Config.InstancePollSeconds)
	SnapshotDiscoveryKeys = make(chan dtstruct.InstanceKey, 10)

	metrics.Register(constant.MetricDiscoverQueueLength, discoveryQueueLengthGauge)
	metrics.Register(constant.MetricDiscoverInstancePollSecondExceed, instancePollSecondsExceededCounter)
	metrics.Register(constant.MetricDiscoverAttempt, discoveriesCounter)
	metrics.Register(constant.MetricDiscoverFail, failedDiscoveriesCounter)
	ometrics.OnMetricTick(func() {
		discoveryQueueLengthGauge.Update(int64(DiscoveryQueue.QueueLen()))
	})

	go func() {
		healthTick := time.Tick(config.HealthPollSeconds * time.Second)
		for {
			select {
			case <-healthTick:
				go func() {
					onHealthTick()
				}()
			}
		}
	}()
}

// DiscoverInstance will attempt to discover (poll) an instance (unless
// it is already up to date) and will also ensure that its master and
// replicas (if any) are also checked.
func DiscoverInstance(ctx context.Context, instanceKey dtstruct.InstanceKey) {

	span, ctx := opentracing.StartSpanFromContext(ctx, "Discover Instance")
	defer span.Finish()

	// check if instance has been set to forget in cache or ignore in config
	if cache.IsExistInstanceForget(instanceKey.StringCode()) {
		log.Infof("discoverInstance: skipping discovery of %+v because it is set to be forgotten", instanceKey)
		return
	}
	if util.RegexpMatchPattern(instanceKey.StringCode(), config.Config.DiscoveryIgnoreHostnameFilters) {
		log.Debugf("discoverInstance: skipping discovery of %+v because it matches DiscoveryIgnoreHostnameFilters", instanceKey)
		return
	}

	// create stopwatch entry
	latency := stopwatch.NewNamedStopwatch()
	if err := latency.AddMany([]string{"backend", "instance", "total", "grpc"}); err != nil {
		log.Erroref(err)
	}

	// metric discover
	latency.Start("total")
	defer func() {
		latency.Stop("total")
		discoveryTime := latency.Elapsed("total")
		if discoveryTime > InstancePollSecondsDuration() {
			instancePollSecondsExceededCounter.Inc(1)
			log.Warning("discoverInstance exceeded InstancePollSeconds for %+v, took %.4fs", instanceKey, discoveryTime.Seconds())
		}
	}()

	base.ResolveHostname(instanceKey.Hostname)
	//instanceKey.ResolveHostname()
	if !instanceKey.IsValid() {
		log.Warning("invalid instance key:%s", instanceKey)
		return
	}

	// Calculate the expiry period each time as InstancePollSeconds
	// _may_ change during the run of the process (via SIGHUP) and
	// it is not possible to change the cache's default expiry..
	if existsInCacheError := cache.SetOperationKey(instanceKey.DisplayString(), true, InstancePollSecondsDuration()); existsInCacheError != nil {
		// Just recently attempted
		return
	}

	// metric read instance
	latency.Start("backend")
	instance, found, err := cinstance.ReadInstance(&instanceKey)
	latency.Stop("backend")
	if instance != nil && found && instance.GetInstance().IsUpToDate && instance.GetInstance().IsLastCheckValid {
		// we've already discovered this one. Skip!
		return
	}

	discoveriesCounter.Inc(1)

	// First we've ever heard of this instance. Continue investigation:
	instance, err = cinstance.GetInfoFromInstance(context.TODO(), &instanceKey, "", latency)

	// panic can occur (IO stuff). Therefore it may happen
	// that instance is nil. Check it, but first get the timing metric.
	totalLatency := latency.Elapsed("total")
	backendLatency := latency.Elapsed("backend")
	instanceLatency := latency.Elapsed("instance")

	//interface的空值需要同时判断动态类型和动态值都为空
	if util.IsNil(instance) {
		failedDiscoveriesCounter.Inc(1)
		if cache.ClearToLog("discoverInstance", instanceKey.StringCode()) {
			log.Warning("DiscoverInstance(%+v) instance is nil in %.3fs (Backend: %.3fs, Instance: %.3fs), error=%+v",
				instanceKey,
				totalLatency.Seconds(),
				backendLatency.Seconds(),
				instanceLatency.Seconds(),
				err)
		}
		return
	}

	if !base.IsLeaderOrActive() {
		// Maybe this node was elected before, but isn't elected anymore.
		// If not elected, stop drilling up/down the topology
		return
	}
	if config.Config.AutoDiscover && instance != nil {
		// investigate upstream and downstream instance
		for _, instKey := range instance.GetAssociateInstance() {
			if IsDiscover(instKey) {
				DiscoveryQueue.Push(instKey)
			} else {
				log.Infof("discovery ignore instance:%s", instKey.String())
			}
		}
	}
}

// IsDiscover check if key can be discover
func IsDiscover(instanceKey dtstruct.InstanceKey) bool {

	// check if key is valid
	if !instanceKey.IsValid() {
		return false
	}

	// avoid noticing some hosts we would otherwise discover
	if util.RegexpMatchPattern(instanceKey.StringCode(), config.Config.DiscoveryIgnoreReplicaHostnameFilters) {
		return false
	}

	// ignore key in forgotten cache
	if cache.IsExistInstanceForget(instanceKey.StringCode()) {
		return false
	}

	return true
}

// InstancePollSecondsDuration
func InstancePollSecondsDuration() time.Duration {
	return time.Duration(config.Config.InstancePollSeconds) * time.Second
}

// onHealthTick handles the actions to take to discover/poll instances
func onHealthTick() {
	if !base.IsLeaderOrActive() {
		return
	}
	instanceKeys, err := cinstance.ReadInstanceKeyOutDate()
	if err != nil {
		log.Erroref(err)
	}
	func() {
		// Normally onHealthTick() shouldn't run concurrently. It is kicked by a ticker.
		// However it _is_ invoked inside a goroutine. I like to be safe here.
		snapshotDiscoveryKeysMutex.Lock()
		defer snapshotDiscoveryKeysMutex.Unlock()

		countSnapshotKeys := len(SnapshotDiscoveryKeys)
		for i := 0; i < countSnapshotKeys; i++ {
			instanceKeys = append(instanceKeys, <-SnapshotDiscoveryKeys)
		}
	}()
	// avoid any logging unless there's something to be done
	if len(instanceKeys) > 0 {
		for _, instanceKey := range instanceKeys {
			if instanceKey.IsValid() {
				DiscoveryQueue.Push(instanceKey)
			}
		}
	}
}
