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

/*

package discovery manages a queue of discovery requests: an ordered
queue with no duplicates.

push() operation never blocks while pop() blocks on an empty queue.

*/

package dtstruct

import (
	"github.com/montanaflynn/stats"
	"sync"
	"time"

	"gitee.com/opengauss/ham4db/go/core/log"
)

// TODO need more refactor for this
// discoveryQueueMap contains the discovery queue which can then be accessed via an API call for monitoring.
// Currently this is accessed by ContinuousDiscovery() but also from http api calls.
// I may need to protect this better?
var discoveryQueueMap map[string]*Queue
var dcLock sync.Mutex

func init() {
	discoveryQueueMap = make(map[string]*Queue)
}

// Queue contains information for managing discovery requests
type Queue struct {
	sync.Mutex

	name         string
	done         chan struct{}
	queue        chan InstanceKey
	queuedKeys   map[InstanceKey]time.Time
	consumedKeys map[InstanceKey]time.Time
	metrics      []MetricQueue

	maxMetricSize int
	timeToWarn    time.Duration // second
}

// start to monitor queue sizes until we are told to stop, hard-coded at every second to do the periodic expiry
func (q *Queue) start() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			q.collectMetric()
		case <-q.done:
			return
		}
	}
}

// stop monitoring the queue
func (q *Queue) stop() {
	q.done <- struct{}{}
}

// collectMetric do a check of the entries in the queue, both those active and queued
func (q *Queue) collectMetric() {
	q.Lock()
	defer q.Unlock()

	// append new queue metric
	q.metrics = append(q.metrics, MetricQueue{Queued: len(q.queuedKeys), Active: len(q.consumedKeys)})

	// remove old entries if we get too big
	if len(q.metrics) > q.maxMetricSize {
		q.metrics = q.metrics[len(q.metrics)-q.maxMetricSize:]
	}
}

// QueueLen returns the length of the queue (channel size + queued size)
func (q *Queue) QueueLen() int {
	q.Lock()
	defer q.Unlock()

	return len(q.queue) + len(q.queuedKeys)
}

// Push enqueues a key if it is not on a queue and is not being
// processed; silently returns otherwise.
func (q *Queue) Push(key InstanceKey) {
	q.Lock()
	defer q.Unlock()

	// is it enqueued already?
	if _, found := q.queuedKeys[key]; found {
		return
	}

	// is it being processed now?
	if _, found := q.consumedKeys[key]; found {
		return
	}

	q.queuedKeys[key] = time.Now()
	q.queue <- key // TODO maybe block
}

// Consume fetches a key to process; blocks if queue is empty.
// Release must be called once after Consume.
func (q *Queue) Consume() InstanceKey {
	q.Lock()
	queue := q.queue
	q.Unlock()

	key := <-queue

	q.Lock()
	defer q.Unlock()

	// alarm if have been waiting for too long
	timeOnQueue := time.Since(q.queuedKeys[key])
	if timeOnQueue > time.Duration(q.timeToWarn)*time.Second {
		log.Warning("key %v spent %.4fs waiting on a discoveryQueueMap", key, timeOnQueue.Seconds())
	}

	q.consumedKeys[key] = q.queuedKeys[key]

	delete(q.queuedKeys, key)

	return key
}

// Release removes a key from a list of being processed keys
// which allows that key to be pushed into the queue again.
func (q *Queue) Release(key InstanceKey) {
	q.Lock()
	defer q.Unlock()

	delete(q.consumedKeys, key)
}

// DiscoveryQueueMetrics returns some raw queue metric based on the
// period (last N entries) requested.
func (q *Queue) DiscoveryQueueMetrics(period int) []MetricQueue {
	q.Lock()
	defer q.Unlock()

	// adjust period in case we ask for something that's too long
	if period > len(q.metrics) {
		log.Debugf("DiscoveryQueueMetrics: wanted: %d, adjusting period to %d", period, len(q.metrics))
		period = len(q.metrics)
	}

	a := q.metrics[len(q.metrics)-period:]
	log.Debugf("DiscoveryQueueMetrics: returning values: %+v", a)
	return a
}

// AggregatedDiscoveryQueueMetrics Returns some aggregate statistics
// based on the period (last N entries) requested.  We store up to
// config.Config.DiscoveryQueueMaxStatisticsSize values and collect once
// a second so we expect period to be a smaller value.
func (q *Queue) AggregatedDiscoveryQueueMetrics(period int) *MetricAggregatedQueue {
	wanted := q.DiscoveryQueueMetrics(period)

	var activeEntries, queuedEntries []int
	// fill vars
	for i := range wanted {
		activeEntries = append(activeEntries, wanted[i].Active)
		queuedEntries = append(queuedEntries, wanted[i].Queued)
	}

	metricAgg := MetricAggregate{}
	a := &MetricAggregatedQueue{
		ActiveMinEntries:    metricAgg.Min(intSliceToFloat64Slice(activeEntries)),
		ActiveMeanEntries:   metricAgg.Mean(intSliceToFloat64Slice(activeEntries)),
		ActiveMedianEntries: metricAgg.Median(intSliceToFloat64Slice(activeEntries)),
		ActiveP95Entries:    metricAgg.Percentile(intSliceToFloat64Slice(activeEntries), 95),
		ActiveMaxEntries:    metricAgg.Max(intSliceToFloat64Slice(activeEntries)),
		QueuedMinEntries:    metricAgg.Min(intSliceToFloat64Slice(queuedEntries)),
		QueuedMeanEntries:   metricAgg.Mean(intSliceToFloat64Slice(queuedEntries)),
		QueuedMedianEntries: metricAgg.Median(intSliceToFloat64Slice(queuedEntries)),
		QueuedP95Entries:    metricAgg.Percentile(intSliceToFloat64Slice(queuedEntries), 95),
		QueuedMaxEntries:    metricAgg.Max(intSliceToFloat64Slice(queuedEntries)),
	}
	log.Debugf("AggregatedDiscoveryQueueMetrics: returning values: %+v", a)
	return a
}

// we pull out values in ints so convert to float64 for metric calculations
func intSliceToFloat64Slice(someInts []int) stats.Float64Data {
	var slice stats.Float64Data

	for _, v := range someInts {
		slice = append(slice, float64(v))
	}

	return slice
}

// CreateOrReturnQueue allows for creation of a new discovery queue or
// returning a pointer to an existing one given the name.
func CreateOrReturnQueue(name string, capacity uint, maxMetricSize int, timeToWarn uint) *Queue {
	dcLock.Lock()
	defer dcLock.Unlock()

	// check if queue with name is exist
	if q, found := discoveryQueueMap[name]; found {
		return q
	}

	// create new one and put to discovery queue map
	q := &Queue{
		name:          name,
		queuedKeys:    make(map[InstanceKey]time.Time),
		consumedKeys:  make(map[InstanceKey]time.Time),
		queue:         make(chan InstanceKey, capacity),
		maxMetricSize: maxMetricSize,
		timeToWarn:    time.Duration(timeToWarn) * time.Second,
	}
	discoveryQueueMap[name] = q
	go q.start()

	return q
}
