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

package logic

import (
	"context"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/cache"
	"gitee.com/opengauss/ham4db/go/core/consensus"
	"gitee.com/opengauss/ham4db/go/core/discover"
	"gitee.com/opengauss/ham4db/go/core/downtime"
	instance2 "gitee.com/opengauss/ham4db/go/core/instance"
	"gitee.com/opengauss/ham4db/go/core/limiter"
	"gitee.com/opengauss/ham4db/go/core/metric/monitor"
	"gitee.com/opengauss/ham4db/go/core/security/token"
	os2 "gitee.com/opengauss/ham4db/go/core/system/osp"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/consensus/raft"
	"gitee.com/opengauss/ham4db/go/core/ha/process"
	"gitee.com/opengauss/ham4db/go/core/kv"
	"gitee.com/opengauss/ham4db/go/core/log"
	ometrics "gitee.com/opengauss/ham4db/go/core/metric"
	"github.com/rcrowley/go-metrics"
)

const (
	discoveryMetricsName = "DISCOVERY_METRICS"
)

var discoveryRecentCountGauge = metrics.NewGauge()
var isHealthyGauge = metrics.NewGauge()
var discoveryMetrics = ometrics.CreateOrReturnCollection(discoveryMetricsName)

func init() {

	metrics.Register(constant.MetricDiscoverRecentCount, discoveryRecentCountGauge)
	metrics.Register(constant.MetricHealthIsHealthy, isHealthyGauge)

	ometrics.OnMetricTick(func() {
		isHealthyGauge.Update(atomic.LoadInt64(&process.LastContinousCheckHealthy))
	})
}

// ContinuousDiscovery starts an asynchronous infinite discovery process where instances are
// periodically investigated and their status captured, and long since unseen instances are
// purged and forgotten.
func ContinuousDiscovery() {
	log.Infof("continuous discovery: setting up")
	continuousDiscoveryStartTime := time.Now()
	checkAndRecoverWaitPeriod := 3 * discover.InstancePollSecondsDuration()

	base.LoadHostnameResolveCache()
	go handleDiscoveryRequests()

	instancePollTick := time.Tick(discover.InstancePollSecondsDuration())
	caretakingTick := time.Tick(time.Minute)
	raftCaretakingTick := time.Tick(10 * time.Minute)
	recoveryTick := time.Tick(time.Duration(config.RecoveryPollSeconds) * time.Second)

	var recoveryEntrance = make([]int64, len(config.Config.EnableAdaptor))
	var snapshotTopologiesTick <-chan time.Time
	if config.Config.SnapshotTopologiesIntervalHours > 0 {
		snapshotTopologiesTick = time.Tick(time.Duration(config.Config.SnapshotTopologiesIntervalHours) * time.Hour)
	}

	runCheckAndRecoverOperationsTimeRipe := func() bool {
		return time.Since(continuousDiscoveryStartTime) >= checkAndRecoverWaitPeriod
	}

	var seedOnce sync.Once

	go ometrics.RunAllMetricCallback(time.Duration(config.DebugMetricsIntervalSeconds) * time.Second)
	monitor.Start()
	go acceptSignals()
	go kv.InitKVStores()
	if config.Config.RaftEnabled {
		if err := orcraft.Setup(consensus.NewCommandApplier(), os2.GetHostname()); err != nil {
			log.Fatale(err)
		}
		go orcraft.Monitor()
	}

	if *dtstruct.RuntimeCLIFlags.GrabElection {
		process.GrabElection()
	}

	log.Infof("continuous discovery: starting")
	for {
		select {
		case <-instancePollTick:
			go func() {
				// This tick does NOT do instance poll (these are handled by the oversampling discoveryTick)
				// But rather should invoke such routinely operations that need to be as (or roughly as) frequent
				// as instance poll
				if base.IsLeaderOrActive() {
					go base.UpdateClusterAliases()
					// TODO why need database type
					for _, dbt := range config.Config.EnableAdaptor {
						go downtime.ExpireDowntime(dbt)
					}
					go injectSeeds(&seedOnce)
				}
			}()
		case <-caretakingTick:
			// Various periodic internal maintenance tasks
			go func() {
				if base.IsLeaderOrActive() {
					//go inst.RecordInstanceCoordinatesHistory()

					// TODO why need database type
					for _, dbt := range config.Config.EnableAdaptor {
						go instance2.ReviewInstanceUnSeen(dbt)
					}
					go instance2.InjectUnseenMaster()

					go base.ForgetLongUnseenInstances()
					go base.ForgetLongUnseenClusterAliases()
					go base.ForgetUnseenInstancesDifferentlyResolved()
					go base.ForgetExpiredHostnameResolves()
					go base.DeleteInvalidHostnameResolves()
					go base.ResolveUnknownMasterHostnameResolves()
					go base.ExpireMaintenance()
					go base.ExpireCandidateInstances()
					go base.ExpireHostnameUnresolve()
					go base.ExpireClusterDomainName()
					go base.ExpireAudit()
					go base.ExpireMasterPositionEquivalence()
					go base.ExpirePoolInstance()
					go base.FlushNontrivialResolveCacheToDatabase()
					go limiter.FlushInstanceInBuffer()
					//go core.ExpireInjectedPseudoGTID()
					//go base.ExpireStaleInstanceBinlogCoordinates()
					go process.ExpireNodesHistory()
					go token.ExpireAccessToken()
					go process.ExpireAvailableNodes()
					go base.ExpireFailureDetectionHistory()
					go base.ExpireTopologyRecoveryHistory()
					go base.ExpireTopologyRecoveryStepsHistory()

					if runCheckAndRecoverOperationsTimeRipe() && base.IsLeader() {
						go SubmitMastersToKvStores("", false)
					}
				} else {
					// Take this opportunity to refresh yourself
					go base.LoadHostnameResolveCache()
				}
			}()
		case <-raftCaretakingTick:
			if orcraft.IsRaftEnabled() && orcraft.IsLeader() {
				go publishDiscoverMasters()
			}
		case <-recoveryTick:
			go func() {
				if base.IsLeaderOrActive() {
					go base.ClearActiveFailureDetections()
					go base.ClearActiveRecoveries()
					go base.ExpireBlockedRecoveries()
					go base.AcknowledgeCrashedRecoveries()
					go base.ExpireInstanceAnalysisChangelog()

					// run failure detection/recovery for all adaptor enabled
					for i, adp := range config.Config.EnableAdaptor {
						recoveryAdaptor := adp
						idx := i
						go func() {
							// This function is non re-entrant (it can only be running once at any point in time)
							if atomic.CompareAndSwapInt64(&recoveryEntrance[idx], 0, 1) {
								defer atomic.StoreInt64(&recoveryEntrance[idx], 0)
							} else {
								return
							}
							if runCheckAndRecoverOperationsTimeRipe() {
								log.Infof("Run failure detection/recovery for %s", recoveryAdaptor)
								_, _, _ = base.CheckAndRecover(dtstruct.GetHamHandler(dtstruct.GetDatabaseType(recoveryAdaptor)), nil, nil, false)
							} else {
								log.Infof("Waiting for %+v seconds to pass before running failure detection/recovery", checkAndRecoverWaitPeriod.Seconds())
							}
						}()
					}
				}
			}()
		case <-snapshotTopologiesTick:
			go func() {
				if base.IsLeaderOrActive() {
					go base.SnapshotTopologies()
				}
			}()
		}
	}
}

// handleDiscoveryRequests iterates the discoveryQueue channel and calls upon
// instance discovery per entry.
func handleDiscoveryRequests() {

	// create a pool of discovery workers
	for i := uint(0); i < config.Config.DiscoveryMaxConcurrency; i++ {
		go func() {
			for {
				instanceKey := discover.DiscoveryQueue.Consume()
				// Possibly this used to be the elected node, but has
				// been demoted, while still the queue is full.
				if !base.IsLeaderOrActive() {
					log.Debugf("Node apparently demoted. Skipping discovery of %+v. Remaining queue size: %+v", instanceKey, discover.DiscoveryQueue.QueueLen())
					discover.DiscoveryQueue.Release(instanceKey)
					continue
				}
				if discover.IsDiscover(instanceKey) {
					discover.DiscoverInstance(context.TODO(), instanceKey)
				}
				discover.DiscoveryQueue.Release(instanceKey)
			}
		}()
	}
}

//TODO==========================

// acceptSignals registers for OS signals
func acceptSignals() {
	c := make(chan os.Signal, 1)

	signal.Notify(c, syscall.SIGHUP)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		for sig := range c {
			switch sig {
			case syscall.SIGHUP:
				log.Infof("Received SIGHUP. Reloading configuration")
				base.AuditOperation("reload-configuration", nil, "", "Triggered via SIGHUP")
				config.Reload()
				discoveryMetrics.SetExpirePeriod(time.Duration(config.Config.DiscoveryCollectionRetentionSeconds) * time.Second)
			case syscall.SIGTERM:
				log.Infof("Received SIGTERM. Shutting down ham4db")
				discoveryMetrics.StopAutoExpiration()
				// probably should poke other go routines to stop cleanly here ...
				base.AuditOperation("shutdown", nil, "", "Triggered via SIGTERM")
				os.Exit(0)
			}
		}
	}()
}

// publishDiscoverMasters will publish to raft a discovery request for all known masters.
// This makes for a best-effort keep-in-sync between raft nodes, where some may have
// inconsistent data due to hosts being forgotten, for example.
func publishDiscoverMasters() error {

	// TODO why need database type
	instances, err := instance2.ReadMasterWriteable()
	if err == nil {
		for _, instance := range instances {
			key := instance.Key
			go orcraft.PublishCommand("discover", key)
		}
	}
	return log.Errore(err)
}

// Write a cluster's master (or all clusters masters) to kv stores.
// This should generally only happen once in a lifetime of a cluster. Otherwise KV
// stores are updated via failovers.
func SubmitMastersToKvStores(clusterName string, force bool) (kvPairs []*dtstruct.KVPair, submittedCount int, err error) {

	// TODO why need database type
	kvPairs, err = instance2.GetMastersKVPair("", clusterName)
	log.Debugf("kv.SubmitMastersToKvStores, clusterName: %s, force: %+v: numPairs: %+v", clusterName, force, len(kvPairs))
	if err != nil {
		return kvPairs, submittedCount, log.Errore(err)
	}
	var selectedError error
	var submitKvPairs []*dtstruct.KVPair
	for _, kvPair := range kvPairs {
		if !force {
			// !force: Called periodically to auto-populate KV
			// We'd like to avoid some overhead.
			if _, found := cache.GetKV(kvPair.Key); found {
				// Let's not overload database with queries. Let's not overload raft with events.
				continue
			}
			v, found, err := kv.GetValue(kvPair.Key)
			if err == nil && found && v == kvPair.Value {
				// Already has the right value.
				cache.SetKV(kvPair.Key, true, constant.CacheExpireDefault)
				continue
			}
		}
		submitKvPairs = append(submitKvPairs, kvPair)
	}
	log.Debugf("kv.SubmitMastersToKvStores: submitKvPairs: %+v", len(submitKvPairs))
	if orcraft.IsRaftEnabled() {
		for _, kvPair := range submitKvPairs {
			_, err := orcraft.PublishCommand("put-key-value", kvPair)
			if err == nil {
				submittedCount++
			} else {
				selectedError = err
			}
		}
	} else {
		err := kv.PutKVPairs(submitKvPairs)
		if err == nil {
			submittedCount += len(submitKvPairs)
		} else {
			selectedError = err
		}
	}
	if err := kv.DistributePairs(kvPairs); err != nil {
		log.Errore(err)
	}
	return kvPairs, submittedCount, log.Errore(selectedError)
}

func injectSeeds(seedOnce *sync.Once) {
	seedOnce.Do(func() {
		for _, seed := range config.Config.DiscoverySeeds {
			// TODO better to check seed pattern
			ds := strings.Split(seed, ":")
			instanceKey, err := base.ParseRawInstanceKey(ds[0], strings.Join(ds[1:], ":"))
			if err == nil {
				instance2.InjectSeed(instanceKey)
			} else {
				log.Errorf("Error parsing seed %s: %+v", seed, err)
			}
		}
	})
}
