/*
   Copyright 2017 Simon J Mudd

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

Package collection holds routines for collecting "high frequency"
metric and handling their auto-expiry based on a configured retention
time. This becomes more interesting as the number of MySQL servers
monitored by ham4db increases.

Most monitoring systems look at different metric over a period
like 1, 10, 30 or 60 seconds but even at second resolution ham4db
may have polled a number of servers.

It can be helpful to collect the raw values, and then allow external
monitoring to pull via an http api call either pre-cooked aggregate
data or the raw data for custom analysis over the period requested.

This is expected to be used for the following types of metric:

* discovery metric (time to poll a MySQL server and collect status)
* queue metric (statistics within the discovery queue itself)
* query metric (statistics on the number of queries made to the
  backend MySQL database)

This code can just add a new metric without worrying about
removing it later, and other code which serves API requests can
pull out the data when needed for the requested time period.

For current metric two api urls have been provided: one provides
the raw data and the other one provides a single set of aggregate
data which is suitable for easy collection by monitoring systems.

Expiry is triggered by default if the collection is created via
CreateOrReturnCollection() and uses an expiry period of
DiscoveryCollectionRetentionSeconds. It can also be enabled by
calling StartAutoExpiration() after setting the required expire
period with SetExpirePeriod().

This will trigger periodic calls (every second) to ensure the removal
of metric which have passed the time specified. Not enabling expiry
will mean data is collected but never freed which will make
ham4db run out of memory eventually.

Current code uses DiscoveryCollectionRetentionSeconds as the
time to keep metric data.

*/
package dtstruct

import (
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/log"
	"sync"
	"time"
)

// Collection contains a collection of Metrics
type Collection struct {
	sync.Mutex                     // for locking the structure
	MetricList   []MetricInterface // metrics of this collection
	Done         chan struct{}     // to indicate that we are finishing expiry processing
	ExpirePeriod time.Duration     // time to keep the MetricList information for

	monitoring bool // am I monitoring the queue size?
}

// SetExpirePeriod determines after how long the collected data should be removed
func (c *Collection) SetExpirePeriod(duration time.Duration) {
	if c == nil {
		log.Error("collection is nil")
		return
	}
	c.Lock()
	defer c.Unlock()
	c.ExpirePeriod = duration
}

// ExpirePeriod returns the currently configured expiration period
func (c *Collection) GetExpirePeriod() time.Duration {
	if c == nil {
		log.Error("collection is nil")
		return constant.CollectionDefaultExpire * time.Second
	}
	c.Lock()
	defer c.Unlock()
	return c.ExpirePeriod
}

// StopAutoExpiration prepares to stop by terminating the auto-expiration process
func (c *Collection) StopAutoExpiration() {
	if c == nil {
		return
	}
	c.Lock()
	if !c.monitoring {
		c.Unlock()
		return
	}
	c.monitoring = false
	c.Unlock()

	// no locking here deliberately
	c.Done <- struct{}{}
}

// StartAutoExpiration initiates the auto expiry procedure which
// periodically checks for metric in the collection which need to
// be expired according to bc.ExpirePeriod.
func (c *Collection) StartAutoExpiration() {
	func() {
		if c == nil {
			return
		}
		c.Lock()
		defer c.Unlock()
		if c.monitoring {
			return
		}
		c.monitoring = true
	}()

	// do the periodic expiry
	ticker := time.NewTicker(constant.CollectionDefaultExpire * time.Second)
	for {
		select {
		case <-ticker.C:
			_ = c.removeBefore(time.Now().Add(-c.ExpirePeriod))
		case <-c.Done:
			ticker.Stop()
			return
		}
	}
}

// Metrics returns a slice containing all the metric values
func (c *Collection) Metrics() []MetricInterface {
	if c == nil {
		return nil
	}
	c.Lock()
	defer c.Unlock()
	return c.MetricList
}

// Since returns the Metrics on or after the given time. We assume
// the metric are stored in ascending time.
// Iterate backwards until we reach the first value before the given time
// or the end of the array.
func (c *Collection) Since(t time.Time) ([]MetricInterface, error) {

	// check if collection is valid
	if c == nil {
		return nil, log.Errorf("collection is nil, so cannot remove collection data")
	}
	c.Lock()
	defer c.Unlock()

	// check if has metric
	if c.MetricList == nil || len(c.MetricList) == 0 {
		return nil, nil
	}

	// find first metric that after time
	first := -1
	for i, metric := range c.MetricList {
		if !metric.When().Before(t) {
			first = i
			break
		}
	}
	if first == -1 {
		return nil, nil
	}

	return c.MetricList[first:], nil
}

// removeBefore is called by StartAutoExpiration and removes Collection values
// before the given time.
func (c *Collection) removeBefore(t time.Time) error {

	// check if collection is valid
	if c == nil {
		return log.Errorf("collection is nil, so cannot remove collection data")
	}
	c.Lock()
	defer c.Unlock()

	// check if has metric
	if c.MetricList == nil || len(c.MetricList) == 0 {
		return nil
	}

	// find first metric that after time
	first := 0
	for i, metric := range c.MetricList {
		if !metric.When().Before(t) {
			first = i
			break
		}
	}
	c.MetricList = c.MetricList[first:]
	return nil
}

// Append a new MetricInterface to the existing collection
func (c *Collection) Append(m MetricInterface) error {

	// check if collection is valid
	if c == nil {
		return log.Errorf("collection is nil, so cannot append metric")
	}
	c.Lock()
	defer c.Unlock()

	// append to metric list
	c.MetricList = append(c.MetricList, m)
	return nil
}
