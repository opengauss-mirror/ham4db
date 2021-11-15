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

package dtstruct

import (
	"time"
)

// MetricAggregatedDiscovery contains aggregated metric for instance discovery.
// Called from api/discovery-metric-aggregated/:seconds
type MetricAggregatedDiscovery struct {
	FirstSeen                       time.Time // timestamp of the first data seen
	LastSeen                        time.Time // timestamp of the last data seen
	CountDistinctInstanceKeys       int       // number of distinct Instances seen (note: this may not be true: distinct = succeeded + failed)
	CountDistinctOkInstanceKeys     int       // number of distinct Instances which succeeded
	CountDistinctFailedInstanceKeys int       // number of distinct Instances which failed
	FailedDiscoveries               uint64    // number of failed discoveries
	SuccessfulDiscoveries           uint64    // number of successful discoveries
	MeanTotalSeconds                float64
	MeanBackendSeconds              float64
	MeanInstanceSeconds             float64
	FailedMeanTotalSeconds          float64
	FailedMeanBackendSeconds        float64
	FailedMeanInstanceSeconds       float64
	MaxTotalSeconds                 float64
	MaxBackendSeconds               float64
	MaxInstanceSeconds              float64
	FailedMaxTotalSeconds           float64
	FailedMaxBackendSeconds         float64
	FailedMaxInstanceSeconds        float64
	MedianTotalSeconds              float64
	MedianBackendSeconds            float64
	MedianInstanceSeconds           float64
	FailedMedianTotalSeconds        float64
	FailedMedianBackendSeconds      float64
	FailedMedianInstanceSeconds     float64
	P95TotalSeconds                 float64
	P95BackendSeconds               float64
	P95InstanceSeconds              float64
	FailedP95TotalSeconds           float64
	FailedP95BackendSeconds         float64
	FailedP95InstanceSeconds        float64
}
