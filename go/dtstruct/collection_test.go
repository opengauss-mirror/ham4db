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
package dtstruct

import (
	"testing"
	"time"
)

// some random base timestamp
var ts = time.Date(2016, 12, 27, 13, 36, 40, 0, time.Local)

// TestExpirePeriod checks that the set expire period is returned
func TestExpirePeriod(t *testing.T) {
	oneSecond := time.Second
	twoSeconds := 2 * oneSecond

	// create a new collection
	c := &Collection{}

	// check if we change it we get back the value we provided
	c.SetExpirePeriod(oneSecond)
	if c.GetExpirePeriod() != oneSecond {
		t.Errorf("TestExpirePeriod: did not get back oneSecond")
	}

	// change the period and check again
	c.SetExpirePeriod(twoSeconds)
	if c.GetExpirePeriod() != twoSeconds {
		t.Errorf("TestExpirePeriod: did not get back twoSeconds")
	}
}

// dummy structure for testing
type testMetric struct {
}

func (tm *testMetric) When() time.Time {
	return ts
}

// check that Append() works as expected
func TestAppend(t *testing.T) {
	c := &Collection{}

	if len(c.Metrics()) != 0 {
		t.Errorf("TestAppend: len(Metrics) = %d, expecting %d", len(c.Metrics()), 0)
	}
	for _, v := range []int{1, 2, 3} {
		tm := &testMetric{}
		c.Append(tm)
		if len(c.Metrics()) != v {
			t.Errorf("TestExpirePeriod: len(Metrics) = %d, expecting %d", len(c.Metrics()), v)
		}
	}
}
