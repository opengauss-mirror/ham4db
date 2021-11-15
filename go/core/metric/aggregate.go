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
// Package query provides query metric with this file providing
// aggregated metric based on the underlying values.
package metric

import (
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"time"

	"github.com/montanaflynn/stats"
)

// AggregatedQuerySince returns the aggregated query metric for the period given from the values provided.
func AggregatedQuerySince(c *dtstruct.Collection, t time.Time) dtstruct.MetricAggregatedQuery {

	// retrieve metricList since the time specified
	metricList, err := c.Since(t)
	if err != nil {
		return dtstruct.MetricAggregatedQuery{}
	}

	// generate the metric
	var waitList []float64
	var execList []float64
	for _, v := range metricList {
		metric := v.(*dtstruct.Metric)
		waitList = append(waitList, metric.WaitLatency.Seconds())
		execList = append(execList, metric.ExecuteLatency.Seconds())
	}

	// create aggregate query metrics
	aqm := dtstruct.MetricAggregatedQuery{}
	aqm.Count = len(waitList)

	// generate aggregate metricList
	var aggVal float64
	if aggVal, err = stats.Max(waitList); err == nil {
		aqm.MaxWaitSeconds = aggVal
	}
	if aggVal, err = stats.Mean(waitList); err == nil {
		aqm.MeanWaitSeconds = aggVal
	}
	if aggVal, err = stats.Median(waitList); err == nil {
		aqm.MedianWaitSeconds = aggVal
	}
	if aggVal, err = stats.Percentile(waitList, 95); err == nil {
		aqm.P95WaitSeconds = aggVal
	}
	if aggVal, err = stats.Max(execList); err == nil {
		aqm.MaxExecSeconds = aggVal
	}
	if aggVal, err = stats.Mean(execList); err == nil {
		aqm.MeanExecSeconds = aggVal
	}
	if aggVal, err = stats.Median(execList); err == nil {
		aqm.MedianExecSeconds = aggVal
	}
	if aggVal, err = stats.Percentile(execList, 95); err == nil {
		aqm.P95ExecSeconds = aggVal
	}

	return aqm
}

// AggregatedDiscoverSince returns a large number of aggregated metric
// based on the raw metric collected since the given time.
func AggregatedDiscoverSince(c *dtstruct.Collection, t time.Time) (dtstruct.MetricAggregatedDiscovery, error) {

	// get all metric since time t
	results, err := c.Since(t)
	if err != nil || len(results) == 0 {
		return dtstruct.MetricAggregatedDiscovery{}, err
	}

	// init maps
	counterMap := make(map[constant.CounterKey]uint64)         // map of string based counterMap
	nameMap := make(map[constant.HostKey]map[string]int)       // map of string based nameMap (using a map)
	timingMap := make(map[constant.TimerKey]stats.Float64Data) // map of string based float64 values
	for _, v := range []constant.CounterKey{constant.FailedDiscoveries, constant.Discoveries} {
		counterMap[v] = 0
	}
	for _, v := range []constant.HostKey{constant.InstanceKeys, constant.FailedInstanceKeys, constant.OkInstanceKeys} {
		nameMap[v] = make(map[string]int)
	}
	for _, v := range []constant.TimerKey{constant.TotalSeconds, constant.BackendSeconds, constant.InstanceSeconds, constant.FailedTotalSeconds, constant.FailedBackendSeconds, constant.FailedInstanceSeconds} {
		timingMap[v] = nil
	}

	var (
		first time.Time
		last  time.Time
	)
	// iterate over results storing required values
	for _, v2 := range results {

		// convert to the right type
		v := v2.(*dtstruct.MetricDiscover)

		// first and last
		if first.IsZero() || first.After(v.Timestamp) {
			first = v.Timestamp
		}
		if last.Before(v.Timestamp) {
			last = v.Timestamp
		}

		// different nameMap, value doesn't matter
		x := nameMap[constant.InstanceKeys]
		x[v.InstanceKey.String()] = 1
		nameMap[constant.InstanceKeys] = x

		// ok nameMap, value doesn't matter
		if v.Err == nil {
			x = nameMap[constant.OkInstanceKeys]
			x[v.InstanceKey.String()] = 1
			nameMap[constant.OkInstanceKeys] = x

			// failed nameMap, value doesn't matter
		} else {
			x = nameMap[constant.FailedInstanceKeys]
			x[v.InstanceKey.String()] = 1
			nameMap[constant.FailedInstanceKeys] = x
		}

		// discoveries
		counterMap[constant.Discoveries]++
		if v.Err != nil {
			counterMap[constant.FailedDiscoveries]++
		}

		// All timingMap
		timingMap[constant.TotalSeconds] = append(timingMap[constant.TotalSeconds], v.TotalLatency.Seconds())
		timingMap[constant.BackendSeconds] = append(timingMap[constant.BackendSeconds], v.BackendLatency.Seconds())
		timingMap[constant.InstanceSeconds] = append(timingMap[constant.InstanceSeconds], v.InstanceLatency.Seconds())

		// Failed timingMap
		if v.Err != nil {
			timingMap[constant.FailedTotalSeconds] = append(timingMap[constant.FailedTotalSeconds], v.TotalLatency.Seconds())
			timingMap[constant.FailedBackendSeconds] = append(timingMap[constant.FailedBackendSeconds], v.BackendLatency.Seconds())
			timingMap[constant.FailedInstanceSeconds] = append(timingMap[constant.FailedInstanceSeconds], v.InstanceLatency.Seconds())
		}
	}
	metricAgg := dtstruct.MetricAggregate{}
	return dtstruct.MetricAggregatedDiscovery{
		FirstSeen:                       first,
		LastSeen:                        last,
		CountDistinctInstanceKeys:       len(nameMap[constant.InstanceKeys]),
		CountDistinctOkInstanceKeys:     len(nameMap[constant.OkInstanceKeys]),
		CountDistinctFailedInstanceKeys: len(nameMap[constant.FailedInstanceKeys]),
		FailedDiscoveries:               counterMap[constant.FailedDiscoveries],
		SuccessfulDiscoveries:           counterMap[constant.Discoveries],
		MeanTotalSeconds:                metricAgg.Mean(timingMap[constant.TotalSeconds]),
		MeanBackendSeconds:              metricAgg.Mean(timingMap[constant.BackendSeconds]),
		MeanInstanceSeconds:             metricAgg.Mean(timingMap[constant.InstanceSeconds]),
		FailedMeanTotalSeconds:          metricAgg.Mean(timingMap[constant.FailedTotalSeconds]),
		FailedMeanBackendSeconds:        metricAgg.Mean(timingMap[constant.FailedBackendSeconds]),
		FailedMeanInstanceSeconds:       metricAgg.Mean(timingMap[constant.FailedInstanceSeconds]),
		MaxTotalSeconds:                 metricAgg.Max(timingMap[constant.TotalSeconds]),
		MaxBackendSeconds:               metricAgg.Max(timingMap[constant.BackendSeconds]),
		MaxInstanceSeconds:              metricAgg.Max(timingMap[constant.InstanceSeconds]),
		FailedMaxTotalSeconds:           metricAgg.Max(timingMap[constant.FailedTotalSeconds]),
		FailedMaxBackendSeconds:         metricAgg.Max(timingMap[constant.FailedBackendSeconds]),
		FailedMaxInstanceSeconds:        metricAgg.Max(timingMap[constant.FailedInstanceSeconds]),
		MedianTotalSeconds:              metricAgg.Median(timingMap[constant.TotalSeconds]),
		MedianBackendSeconds:            metricAgg.Median(timingMap[constant.BackendSeconds]),
		MedianInstanceSeconds:           metricAgg.Median(timingMap[constant.InstanceSeconds]),
		FailedMedianTotalSeconds:        metricAgg.Median(timingMap[constant.FailedTotalSeconds]),
		FailedMedianBackendSeconds:      metricAgg.Median(timingMap[constant.FailedBackendSeconds]),
		FailedMedianInstanceSeconds:     metricAgg.Median(timingMap[constant.FailedInstanceSeconds]),
		P95TotalSeconds:                 metricAgg.Percentile(timingMap[constant.TotalSeconds], 95),
		P95BackendSeconds:               metricAgg.Percentile(timingMap[constant.BackendSeconds], 95),
		P95InstanceSeconds:              metricAgg.Percentile(timingMap[constant.InstanceSeconds], 95),
		FailedP95TotalSeconds:           metricAgg.Percentile(timingMap[constant.FailedTotalSeconds], 95),
		FailedP95BackendSeconds:         metricAgg.Percentile(timingMap[constant.FailedBackendSeconds], 95),
		FailedP95InstanceSeconds:        metricAgg.Percentile(timingMap[constant.FailedInstanceSeconds], 95),
	}, nil
}
