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
package limiter

import (
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/instance"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"github.com/rcrowley/go-metrics"
	"sort"
	"time"
)

var topologyConcurrencyChan = make(chan bool, constant.ConcurrencyTopologyOperation)
var readInstanceCounter = metrics.NewCounter()
var writeInstanceCounter = metrics.NewCounter()

func init() {
	metrics.Register(constant.MetricInstanceRead, readInstanceCounter)
	metrics.Register(constant.MetricInstanceWrite, writeInstanceCounter)
}

// ExecuteOnTopology will execute given function while maintaining concurrency limit
// on topology servers. It is safe in the sense that we will not leak tokens.
func ExecuteOnTopology(f func()) {
	topologyConcurrencyChan <- true
	defer func() { recover(); <-topologyConcurrencyChan }()
	f()
}

type InstanceUpdateObject struct {
	Instance                 dtstruct.InstanceAdaptor
	InstanceWasActuallyFound bool
	LastError                error
}

var InstanceWriteBuffer = make(chan InstanceUpdateObject, config.Config.InstanceWriteBufferSize)
var ForceFlushInstanceWriteBuffer = make(chan bool)

// EnqueueInstanceWrite limit write concurrently
func EnqueueInstanceWrite(instance dtstruct.InstanceAdaptor, instanceWasActuallyFound bool, lastError error) {
	if len(InstanceWriteBuffer) == config.Config.InstanceWriteBufferSize {
		// Signal the "flushing" goroutine that there's work.
		// We prefer doing all bulk flushes from one goroutine.
		// Non blocking send to avoid blocking goroutines on sending a flush,
		// if the "flushing" goroutine is not able read is because a flushing is ongoing.
		select {
		case ForceFlushInstanceWriteBuffer <- true:
		default:
		}
	}
	InstanceWriteBuffer <- InstanceUpdateObject{instance, instanceWasActuallyFound, lastError}
}

// FlushInstanceInBuffer flush all instance in write buffer periodically or force
func FlushInstanceInBuffer() {
	config.WaitForConfigurationToBeLoaded()
	flushTick := time.Tick(time.Duration(config.Config.InstanceFlushIntervalMilliseconds) * time.Millisecond)
	for {
		select {
		case <-flushTick:
			flushInstanceWriteBuffer()
		case <-ForceFlushInstanceWriteBuffer:
			flushInstanceWriteBuffer()
		}
	}
}

// flushInstanceWriteBuffer saves enqueued instances to backend database
func flushInstanceWriteBuffer() {

	// check channel length
	if len(InstanceWriteBuffer) == 0 {
		return
	}

	// There are `DiscoveryMaxConcurrency` many goroutines trying to enqueue an instance into the buffer
	// when one instance is flushed from the buffer then one discovery goroutine is ready to enqueue a new instance
	// this is why we want to flush all errInstList in the buffer until a max of `InstanceWriteBufferSize`.
	// Otherwise we can flush way more errInstList than what's expected.
	var errInstList []dtstruct.InstanceAdaptor
	var seenInstList []dtstruct.InstanceAdaptor // update with last_seen_timestamp field
	for i := 0; i < config.Config.InstanceWriteBufferSize && len(InstanceWriteBuffer) > 0; i++ {
		upd := <-InstanceWriteBuffer
		if upd.InstanceWasActuallyFound && upd.LastError == nil {
			seenInstList = append(seenInstList, upd.Instance)
		} else {
			errInstList = append(errInstList, upd.Instance)
		}
	}

	// write all instance to backend database, sort instance list by instanceKey (table pk) to make locking predictable
	_ = db.ExecDBWrite(
		func() error {

			// instance with error
			sort.Sort(byInstanceKey(errInstList))
			if err := instance.WriteInstance(errInstList, true, false, nil); err != nil {
				return log.Errorf("flush instance without update last_seen_timestamp, error: %v", err)
			}

			// instance without error
			sort.Sort(byInstanceKey(seenInstList))
			if err := instance.WriteInstance(seenInstList, true, true, nil); err != nil {
				return log.Errorf("flush instance with update last_seen_timestamp, error: %v", err)
			}
			writeInstanceCounter.Inc(int64(len(errInstList) + len(seenInstList)))
			return nil
		},
	)
}

// instances sorter by instanceKey
type byInstanceKey []dtstruct.InstanceAdaptor

func (a byInstanceKey) Len() int      { return len(a) }
func (a byInstanceKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byInstanceKey) Less(i, j int) bool {
	return a[i].GetInstance().Key.SmallerThan(&a[j].GetInstance().Key)
}
