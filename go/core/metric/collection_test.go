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

package metric

import (
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
	"time"
)

var randomString = []string{
	"RANDOM_STRING",
	"SOME_OTHER_STRING",
}

func TestCreateOrReturnCollection(t *testing.T) {

	// first one
	name := randomString[0]

	// normal case 1: not exist
	c1 := CreateOrReturnCollection(name)
	tests.S(t).ExpectTrue(c1 != nil)
	tests.S(t).ExpectTrue(c1.Append(dtstruct.NewMetric()) == nil)
	tests.S(t).ExpectTrue(len(c1.Metrics()) == 1)

	// normal case 2: exist
	c2 := CreateOrReturnCollection(name)
	tests.S(t).ExpectTrue(c2 == c1)

	// another different one
	name = randomString[1]

	// normal case 3: not exist and different one
	c3 := CreateOrReturnCollection(name)
	tests.S(t).ExpectTrue(c3 != nil && c3 != c1)

	// normal case 4: metric expired
	time.Sleep(1 * time.Second)
	tests.S(t).ExpectTrue(c1.Append(dtstruct.NewMetric()) == nil)
	tests.S(t).ExpectTrue(len(c1.Metrics()) == 2)
	metricList, err := c1.Since(time.Now().Add(-1 * time.Second))
	tests.S(t).ExpectTrue(err == nil)

	// relative time, so maybe take too long before this, so it will get nil or get one metric normally
	tests.S(t).ExpectTrue(metricList == nil || len(metricList) == 1)
}
