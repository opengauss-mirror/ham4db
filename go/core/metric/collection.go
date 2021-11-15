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
package metric

import (
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"sync"
	"time"

	"gitee.com/opengauss/ham4db/go/config"
)

// backendMetricCollection contains the last N backend "channelled"
// metric which can then be accessed via an API call for monitoring.
var (
	collectionMap map[string]*dtstruct.Collection
	mapLock       sync.Mutex
)

func init() {
	collectionMap = make(map[string]*dtstruct.Collection)
}

// CreateOrReturnCollection allows for creation of a new collection or
// returning a pointer to an existing one given the name. This allows access
// to the data structure from the api interface (http/api.go) and also when writing (inst).
func CreateOrReturnCollection(name string) *dtstruct.Collection {
	mapLock.Lock()
	defer mapLock.Unlock()

	// if exist, return it
	if collection, found := collectionMap[name]; found {
		return collection
	}

	// create new one and set to map, TODO WARNING: use a different configuration name
	collection := &dtstruct.Collection{
		MetricList:   nil,
		Done:         make(chan struct{}),
		ExpirePeriod: time.Duration(config.Config.DiscoveryCollectionRetentionSeconds) * time.Second,
	}
	collectionMap[name] = collection

	// auto expire metric
	go collection.StartAutoExpiration()

	return collection
}
