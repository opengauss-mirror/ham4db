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
package metric

import (
	"errors"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/tests"
	"strconv"
	"testing"
	"time"
)

func TestAggregatedQuerySince(t *testing.T) {

	// normal case 1: create collection for test with 10 metrics
	collection := &dtstruct.Collection{
		MetricList:   nil,
		Done:         make(chan struct{}),
		ExpirePeriod: 100 * time.Minute,
	}

	metricCount := 10

	// append metric to collection with wait and execute latency
	for i := 0; i < metricCount; i++ {
		tests.S(t).ExpectTrue(collection.Append(
			&dtstruct.Metric{
				WaitLatency:    time.Duration(i) * time.Second,
				ExecuteLatency: time.Duration(i) * time.Second,
				Timestamp:      time.Now(),
			},
		) == nil)
	}

	// get aggregate metric
	aqm := AggregatedQuerySince(collection, time.Now().Add(-100*time.Minute))
	tests.S(t).ExpectTrue(aqm.Count == metricCount)
	tests.S(t).ExpectTrue(aqm.MaxWaitSeconds == float64(metricCount-1))
	tests.S(t).ExpectTrue(aqm.MeanWaitSeconds*2 == float64(metricCount-1))
	tests.S(t).ExpectTrue(aqm.MedianWaitSeconds*2 == float64(metricCount-1))
	tests.S(t).ExpectTrue(aqm.P95WaitSeconds == float64(metricCount-1))
	tests.S(t).ExpectTrue(aqm.MaxExecSeconds == float64(metricCount-1))
	tests.S(t).ExpectTrue(aqm.MeanExecSeconds*2 == float64(metricCount-1))
	tests.S(t).ExpectTrue(aqm.MedianExecSeconds*2 == float64(metricCount-1))
	tests.S(t).ExpectTrue(aqm.P95ExecSeconds == float64(metricCount-1))

	// failed case 1: collection is nil
	tests.S(t).ExpectTrue(AggregatedQuerySince(nil, time.Now()).Count == 0)
}

func TestAggregatedDiscoverSince(t *testing.T) {

	// normal case 1: create collection for test with 10 metrics
	collection := &dtstruct.Collection{
		MetricList:   nil,
		Done:         make(chan struct{}),
		ExpirePeriod: 100 * time.Minute,
	}

	metricCount := 10

	// append metric to collection with wait and execute latency
	for i := 0; i < metricCount; i++ {
		hostname := "test_host_" + strconv.Itoa(i/2)
		var err error
		if i%3 == 0 {
			err = errors.New("simulate error")
		}
		tests.S(t).ExpectTrue(collection.Append(
			&dtstruct.MetricDiscover{
				InstanceKey:     dtstruct.InstanceKey{Hostname: hostname, Port: 10000},
				BackendLatency:  time.Duration(i) * time.Second,
				InstanceLatency: time.Duration(i) * time.Second,
				TotalLatency:    time.Duration(i*2) * time.Second,
				Err:             err,
				Timestamp:       time.Now(),
			},
		) == nil)
	}

	mad, err := AggregatedDiscoverSince(collection, time.Now().Add(-10*time.Minute))
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(mad.CountDistinctInstanceKeys == 5)
	tests.S(t).ExpectTrue(mad.CountDistinctOkInstanceKeys == 5)
	tests.S(t).ExpectTrue(mad.CountDistinctFailedInstanceKeys == 4)
	tests.S(t).ExpectTrue(mad.FailedDiscoveries == 4)
	tests.S(t).ExpectTrue(mad.SuccessfulDiscoveries == 10)
	tests.S(t).ExpectTrue(mad.MeanTotalSeconds == 9)
	tests.S(t).ExpectTrue(mad.MeanBackendSeconds == 4.5)
	tests.S(t).ExpectTrue(mad.MeanInstanceSeconds == 4.5)
	tests.S(t).ExpectTrue(mad.FailedMeanTotalSeconds == 9)
	tests.S(t).ExpectTrue(mad.FailedMeanBackendSeconds == 4.5)
	tests.S(t).ExpectTrue(mad.FailedMeanInstanceSeconds == 4.5)
	tests.S(t).ExpectTrue(mad.MaxTotalSeconds == 18)
	tests.S(t).ExpectTrue(mad.MaxBackendSeconds == 9)
	tests.S(t).ExpectTrue(mad.MaxInstanceSeconds == 9)
	tests.S(t).ExpectTrue(mad.FailedMaxTotalSeconds == 18)
	tests.S(t).ExpectTrue(mad.FailedMaxBackendSeconds == 9)
	tests.S(t).ExpectTrue(mad.FailedMaxInstanceSeconds == 9)
	tests.S(t).ExpectTrue(mad.MedianTotalSeconds == 9)
	tests.S(t).ExpectTrue(mad.MedianBackendSeconds == 4.5)
	tests.S(t).ExpectTrue(mad.MedianInstanceSeconds == 4.5)
	tests.S(t).ExpectTrue(mad.FailedMedianTotalSeconds == 9)
	tests.S(t).ExpectTrue(mad.FailedMedianBackendSeconds == 4.5)
	tests.S(t).ExpectTrue(mad.FailedMedianInstanceSeconds == 4.5)
	tests.S(t).ExpectTrue(mad.P95TotalSeconds == 18)
	tests.S(t).ExpectTrue(mad.P95BackendSeconds == 9)
	tests.S(t).ExpectTrue(mad.P95InstanceSeconds == 9)
	tests.S(t).ExpectTrue(mad.FailedP95TotalSeconds == 18)

	// normal case 2: no metric after time
	mad, err = AggregatedDiscoverSince(collection, time.Now().Add(10*time.Minute))
	tests.S(t).ExpectTrue(err == nil)
	tests.S(t).ExpectTrue(mad.CountDistinctInstanceKeys == 0)
}
