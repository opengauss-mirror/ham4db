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
package instance

import (
	"bytes"
	"fmt"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	ttest "gitee.com/opengauss/ham4db/go/test"
	"gitee.com/opengauss/ham4db/go/test/testdb/common/constant"
	tdtstruct "gitee.com/opengauss/ham4db/go/test/testdb/dtstruct"
	"gitee.com/opengauss/ham4db/go/util"
	"regexp"
	"strings"
	"testing"

	test "gitee.com/opengauss/ham4db/go/util/tests"
)

func init() {
	ttest.DBTestInit()
}

var clusterId = util.RandomHash32()

var (
	i710k = dtstruct.InstanceKey{Hostname: "i710", Port: 3306, DBType: constant.DBTTestDB, ClusterId: clusterId}
	i720k = dtstruct.InstanceKey{Hostname: "i720", Port: 3306, DBType: constant.DBTTestDB, ClusterId: clusterId}
	i730k = dtstruct.InstanceKey{Hostname: "i730", Port: 3306, DBType: constant.DBTTestDB, ClusterId: clusterId}
)

var (
	spacesRegexp = regexp.MustCompile(`[ \t\n\r]+`)
)

func mkTestInstances() []dtstruct.InstanceAdaptor {
	i710 := &tdtstruct.TestDBInstance{Instance: &dtstruct.Instance{Key: i710k, InstanceId: "710", Version: "20.1.0"}}
	i720 := &tdtstruct.TestDBInstance{Instance: &dtstruct.Instance{Key: i720k, InstanceId: "720", Version: "20.1.0"}}
	i730 := &tdtstruct.TestDBInstance{Instance: &dtstruct.Instance{Key: i730k, InstanceId: "730", Version: "20.1.0"}}
	instances := []dtstruct.InstanceAdaptor{i710, i720, i730}
	return instances
}
func normalizeQuery(name string) string {
	name = strings.Replace(name, "`", "", -1)
	name = spacesRegexp.ReplaceAllString(name, " ")
	name = strings.TrimSpace(name)
	return name
}

func stripSpaces(s string) string {
	s = spacesRegexp.ReplaceAllString(s, "")
	return s
}

func TestMkInsertOdkuSingle(t *testing.T) {
	instances := mkTestInstances()

	err := WriteInstance(nil, true, true, nil)
	test.S(t).ExpectNil(err)

	err = WriteInstance(instances[:1], false, true, nil)
	test.S(t).ExpectNil(err)
}

func TestMkInsertOdkuThree(t *testing.T) {
	instances := mkTestInstances()
	err := WriteInstance(instances[:3], true, true, nil)
	test.S(t).ExpectNil(err)
}

func fmtArgs(args []interface{}) string {
	b := &bytes.Buffer{}
	for _, a := range args {
		fmt.Fprint(b, a)
		fmt.Fprint(b, ", ")
	}
	return b.String()
}
