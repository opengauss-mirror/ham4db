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
package base

import (
	"fmt"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	orcraft "gitee.com/opengauss/ham4db/go/core/consensus/raft"
	"gitee.com/opengauss/ham4db/go/core/ha/process"
	"gitee.com/opengauss/ham4db/go/core/log"
	ometrics "gitee.com/opengauss/ham4db/go/core/metric"
	"github.com/rcrowley/go-metrics"
	"sync/atomic"
	"time"
)

const (
	yieldAfterUnhealthyDuration = 5 * config.HealthPollSeconds * time.Second
	fatalAfterUnhealthyDuration = 30 * config.HealthPollSeconds * time.Second
)

var isElectedGauge = metrics.NewGauge()
var isRaftLeaderGauge = metrics.NewGauge()
var isRaftHealthyGauge = metrics.NewGauge()

var isElectedNode int64 = 0

func init() {

	metrics.Register(constant.MetricElectIsElected, isElectedGauge)
	metrics.Register(constant.MetricRaftIsLeader, isRaftLeaderGauge)
	metrics.Register(constant.MetricRaftIsHealthy, isRaftHealthyGauge)

	ometrics.OnMetricTick(func() {
		isElectedGauge.Update(atomic.LoadInt64(&isElectedNode))
	})
	ometrics.OnMetricTick(func() {
		isRaftLeaderGauge.Update(atomic.LoadInt64(&isElectedNode))
	})
	ometrics.OnMetricTick(func() {
		var healthy int64
		if orcraft.IsHealthy() {
			healthy = 1
		}
		isRaftHealthyGauge.Update(healthy)
	})

	go func() {
		config.WaitForConfigurationToBeLoaded()
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

func IsLeader() bool {
	if orcraft.IsRaftEnabled() {
		return orcraft.IsLeader()
	}
	return atomic.LoadInt64(&isElectedNode) == 1
}

func IsLeaderOrActive() bool {
	if orcraft.IsRaftEnabled() {
		return orcraft.IsPartOfQuorum()
	}
	return atomic.LoadInt64(&isElectedNode) == 1
}

// onHealthTick handles the actions to take to discover/poll instances
func onHealthTick() {
	wasAlreadyElected := IsLeader()

	if orcraft.IsRaftEnabled() {
		if orcraft.IsLeader() {
			atomic.StoreInt64(&isElectedNode, 1)
		} else {
			atomic.StoreInt64(&isElectedNode, 0)
		}
		if process.SinceLastGoodHealthCheck() > yieldAfterUnhealthyDuration {
			log.Errorf("Health test is failing for over %+v seconds. raft yielding", yieldAfterUnhealthyDuration.Seconds())
			orcraft.Yield()
		}
		if process.SinceLastGoodHealthCheck() > fatalAfterUnhealthyDuration {
			orcraft.FatalRaftError(fmt.Errorf("node is unable to register health. Please check database connnectivity and/or time synchronisation."))
		}
	}
	if !orcraft.IsRaftEnabled() {
		myIsElectedNode, err := process.AttemptElection()
		if err != nil {
			log.Errore(err)
		}
		if myIsElectedNode {
			atomic.StoreInt64(&isElectedNode, 1)
		} else {
			atomic.StoreInt64(&isElectedNode, 0)
		}
		if !myIsElectedNode {
			if electedNode, _, err := process.ElectedNode(); err == nil {
				log.Infof("Not elected as active node; active node: %v; polling", electedNode.Hostname)
			} else {
				log.Infof("Not elected as active node; active node: Unable to determine: %v; polling", err)
			}
		}
	}
	if !IsLeaderOrActive() {
		return
	}
	if !wasAlreadyElected {
		// Just turned to be leader!
		go process.RegisterNode(process.ThisNodeHealth)
		go ExpireMaintenance()
	}
}
