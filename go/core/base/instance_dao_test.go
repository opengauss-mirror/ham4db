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
package base

import (
	"gitee.com/opengauss/ham4db/go/dtstruct"
	test "gitee.com/opengauss/ham4db/go/util/tests"
	"regexp"
	"testing"
)

func TestWithCurrentTime(t *testing.T) {
	ci := &dtstruct.CandidateDatabaseInstance{}
	ci = WithCurrentTime(ci)
	if match, err := regexp.MatchString("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}", ci.LastSuggestedString); err == nil {
		test.S(t).ExpectTrue(match)
	} else {
		t.Fatalf("%s", err)
	}
}
