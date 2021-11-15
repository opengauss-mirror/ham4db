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
package ham

import (
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/dtstruct"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/base"
	cache2 "gitee.com/opengauss/ham4db/go/core/cache"
	orcraft "gitee.com/opengauss/ham4db/go/core/consensus/raft"
	"gitee.com/opengauss/ham4db/go/core/log"
	"github.com/patrickmn/go-cache"
	"math/rand"
	"time"
)

var pseudoGTIDPublishCache = cache.New(time.Minute, time.Second)

func ContinuousDiscovery() {

	autoPseudoGTIDTick := time.Tick(time.Duration(config.PseudoGTIDIntervalSeconds) * time.Second)
	caretakingTick := time.Tick(time.Minute)

	for {
		select {

		case <-autoPseudoGTIDTick:
			go func() {
				if config.Config.AutoPseudoGTID && base.IsLeader() {
					go InjectPseudoGTIDOnWriters()
				}
			}()

		case <-caretakingTick:
			// Various periodic internal maintenance tasks
			go func() {
				if base.IsLeaderOrActive() {
					go ExpireInjectedPseudoGTID()
				}
			}()
		}
	}
}

// InjectPseudoGTIDOnWriters will inject a PseudoGTID entry on all writable, accessible,
// supported writers.
func InjectPseudoGTIDOnWriters() error {
	instances, err := ReadWriteableClustersMasters()
	if err != nil {
		return log.Errore(err)
	}
	for i := range rand.Perm(len(instances)) {
		instance := instances[i]
		go func() {
			if injected, _ := CheckAndInjectPseudoGTIDOnWriter(instance); injected {
				clusterName := instance.ClusterName
				if orcraft.IsRaftEnabled() {
					// We prefer not saturating our raft communication. Pseudo-GTID information is
					// OK to be cached for a while.
					if _, found := pseudoGTIDPublishCache.Get(clusterName); !found {
						pseudoGTIDPublishCache.Set(clusterName, true, cache.DefaultExpiration)
						orcraft.PublishCommand("injected-pseudo-gtid", clusterName)
					}
				} else {
					RegisterInjectedPseudoGTID(clusterName)
				}
			}
		}()
	}
	return nil
}

// CheckAndInjectPseudoGTIDOnWriter checks whether pseudo-GTID can and
// should be injected on given instance, and if so, attempts to inject.
func CheckAndInjectPseudoGTIDOnWriter(instance *dtstruct.MysqlInstance) (injected bool, err error) {
	if instance == nil {
		return injected, log.Errorf("CheckAndInjectPseudoGTIDOnWriter: instance is nil")
	}
	if instance.ReadOnly {
		return injected, log.Errorf("CheckAndInjectPseudoGTIDOnWriter: instance is read-only: %+v", instance.Key)
	}
	if !instance.IsLastCheckValid {
		return injected, nil
	}
	canInject, err := canInjectPseudoGTID(&instance.Key)
	if err != nil {
		return injected, log.Errore(err)
	}
	if !canInject {
		if cache2.ClearToLog("CheckAndInjectPseudoGTIDOnWriter", instance.Key.StringCode()) {
			log.Warningf("AutoPseudoGTID enabled, but ham4db has no priviliges on %+v to inject pseudo-gtid", instance.Key)
		}

		return injected, nil
	}
	if _, err := injectPseudoGTID(instance); err != nil {
		return injected, log.Errore(err)
	}
	injected = true
	if err := RegisterInjectedPseudoGTID(instance.ClusterName); err != nil {
		return injected, log.Errore(err)
	}
	return injected, nil
}
