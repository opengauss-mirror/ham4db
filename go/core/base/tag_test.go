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
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"testing"

	test "gitee.com/opengauss/ham4db/go/util/tests"
)

func init() {
	config.Config.AuditToBackendDB = true
}

func TestPutInstanceTag(t *testing.T) {

	// normal case 1: put new tag
	// put new tag
	instKey := &dtstruct.InstanceKey{DBType: constant.TestDB, Hostname: "abc.local", Port: 10000, ClusterId: util.RandomHash32()}
	tag := &dtstruct.Tag{TagName: "tag1", TagValue: "test-tag-1"}
	test.S(t).ExpectNil(PutInstanceTag(instKey, tag))

	// function for check
	checkTag := func(tag *dtstruct.Tag) {
		var tagVal string
		rowCount := 0
		db.Query("select tag_name, tag_value from ham_database_instance_tag where tag_name = ? and cluster_id = ?", sqlutil.Args(tag.TagName, instKey.ClusterId), func(rowMap sqlutil.RowMap) error {
			tagVal = rowMap.GetString("tag_value")
			rowCount++
			return nil
		})
		test.S(t).ExpectEquals(rowCount, 1)
		test.S(t).ExpectEquals(tagVal, tag.TagValue)
	}

	// check for first put
	checkTag(tag)

	// normal case 2: update exist tag
	tag.TagValue = "test-tag-2"
	test.S(t).ExpectNil(PutInstanceTag(instKey, tag))
	checkTag(tag)
}

func TestUntag(t *testing.T) {

	clusterIdUntag := util.RandomHash32()

	// init instance and tag info
	instList := []*dtstruct.InstanceKey{
		{DBType: constant.TestDB, Hostname: "abc-1.local", Port: 11111, ClusterId: clusterIdUntag},
		{DBType: constant.TestDB, Hostname: "abc-2.local", Port: 11111, ClusterId: clusterIdUntag},
		{DBType: constant.TestDB, Hostname: "abc-3.local", Port: 11111, ClusterId: clusterIdUntag},
	}
	tagList := []*dtstruct.Tag{
		{TagName: "untag-1", TagValue: "un-1", HasValue: true, Negate: false},
		{TagName: "untag-2", TagValue: "un-2", HasValue: true, Negate: false},
		{TagName: "untag-3", TagValue: "un-3", HasValue: true, Negate: true}, // negate
		{TagName: "untag-4", HasValue: false, Negate: false},                 // without value
	}

	// insert tag for instance
	for _, inst := range instList {
		for _, tag := range tagList {
			test.S(t).ExpectNil(PutInstanceTag(inst, tag))
		}
	}

	// normal case 1: untag on given instance
	tagged, err := Untag(instList[0], tagList[0])
	test.S(t).ExpectNil(err)
	test.S(t).ExpectTrue(tagged.HasKey(*instList[0]))

	// normal case 2: untag on all instance with this tag
	tagged, err = Untag(nil, tagList[1])
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(len(tagged.GetInstanceKeys()), 3)

	// normal case 3: untag tag without value on given instance
	tagged, err = Untag(instList[1], tagList[3])
	test.S(t).ExpectNil(err)
	test.S(t).ExpectEquals(len(tagged.GetInstanceKeys()), 1)

	// failed case 1: tag is nil
	tagged, err = Untag(nil, nil)
	test.S(t).ExpectNotNil(err)
	test.S(t).ExpectTrue(tagged == nil)

	// failed case 2: untag negative tag
	tagged, err = Untag(nil, tagList[2])
	test.S(t).ExpectNotNil(err)
	test.S(t).ExpectTrue(tagged == nil)

	// failed case 2: untag tag without value on all instance
	tagged, err = Untag(nil, tagList[3])
	test.S(t).ExpectNotNil(err)
	test.S(t).ExpectTrue(tagged == nil)

	// check audit log
	var count int
	db.Query("select count(*) as cnt from ham_audit where audit_type = 'delete-instance-tag' and cluster_id = ? and message like 'untag-%'", sqlutil.Args(clusterIdUntag), func(rowMap sqlutil.RowMap) error {
		count = rowMap.GetInt("cnt")
		return nil
	})
	test.S(t).ExpectEquals(count, 2) // only can get call Untag with specified instance key
}

func TestReadInstanceTag(t *testing.T) {

	// put new tag
	instKey := &dtstruct.InstanceKey{DBType: constant.TestDB, Hostname: "abc-rit.local", Port: 10000}
	tag := &dtstruct.Tag{TagName: "tag-rit", TagValue: "test-tag-1"}
	test.S(t).ExpectNil(PutInstanceTag(instKey, tag))

	// normal case 1: exist tag
	tag1 := &dtstruct.Tag{TagName: "tag-rit"}
	exist, err := ReadInstanceTag(instKey, tag1)
	test.S(t).ExpectNil(err)
	test.S(t).ExpectTrue(exist)
	test.S(t).ExpectEquals(tag1.TagValue, "test-tag-1")
}

func TestParseTag(t *testing.T) {
	{
		tag, err := dtstruct.ParseTag("")
		test.S(t).ExpectTrue(tag == nil)
		test.S(t).ExpectNotNil(err)
	}
	{
		tag, err := dtstruct.ParseTag("=")
		test.S(t).ExpectTrue(tag == nil)
		test.S(t).ExpectNotNil(err)
	}
	{
		tag, err := dtstruct.ParseTag("=backup")
		test.S(t).ExpectTrue(tag == nil)
		test.S(t).ExpectNotNil(err)
	}
	{
		tag, err := dtstruct.ParseTag("  =backup")
		test.S(t).ExpectTrue(tag == nil)
		test.S(t).ExpectNotNil(err)
	}
	{
		tag, err := dtstruct.ParseTag("role")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(tag != nil)
		test.S(t).ExpectEquals(tag.TagName, "role")
		test.S(t).ExpectEquals(tag.TagValue, "")
		test.S(t).ExpectFalse(tag.Negate)
		test.S(t).ExpectFalse(tag.HasValue)

		test.S(t).ExpectEquals(tag.String(), "role=")
	}
	{
		tag, err := dtstruct.ParseTag("role=")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(tag != nil)
		test.S(t).ExpectEquals(tag.TagName, "role")
		test.S(t).ExpectEquals(tag.TagValue, "")
		test.S(t).ExpectFalse(tag.Negate)
		test.S(t).ExpectTrue(tag.HasValue)

		test.S(t).ExpectEquals(tag.String(), "role=")

	}
	{
		tag, err := dtstruct.ParseTag("role=backup")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(tag != nil)
		test.S(t).ExpectEquals(tag.TagName, "role")
		test.S(t).ExpectEquals(tag.TagValue, "backup")
		test.S(t).ExpectFalse(tag.Negate)
		test.S(t).ExpectTrue(tag.HasValue)

		test.S(t).ExpectEquals(tag.String(), "role=backup")
	}
	{
		tag, err := dtstruct.ParseTag("!role")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(tag != nil)
		test.S(t).ExpectEquals(tag.TagName, "role")
		test.S(t).ExpectTrue(tag.Negate)
		test.S(t).ExpectFalse(tag.HasValue)
	}
	{
		tag, err := dtstruct.ParseTag("~role=backup")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectTrue(tag != nil)
		test.S(t).ExpectEquals(tag.TagName, "role")
		test.S(t).ExpectEquals(tag.TagValue, "backup")
		test.S(t).ExpectTrue(tag.Negate)
		test.S(t).ExpectTrue(tag.HasValue)
	}
}

func TestParseIntersectTags(t *testing.T) {
	{
		_, err := dtstruct.ParseIntersectTags("")
		test.S(t).ExpectNotNil(err)
	}
	{
		_, err := dtstruct.ParseIntersectTags(",")
		test.S(t).ExpectNotNil(err)
	}
	{
		_, err := dtstruct.ParseIntersectTags(",,,")
		test.S(t).ExpectNotNil(err)
	}
	{
		_, err := dtstruct.ParseIntersectTags("role,")
		test.S(t).ExpectNotNil(err)
	}
	{
		tags, err := dtstruct.ParseIntersectTags("role")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(len(tags), 1)

		test.S(t).ExpectEquals(tags[0].TagName, "role")
		test.S(t).ExpectEquals(tags[0].TagValue, "")
		test.S(t).ExpectFalse(tags[0].Negate)
		test.S(t).ExpectFalse(tags[0].HasValue)
	}
	{
		tags, err := dtstruct.ParseIntersectTags("role,dc")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(len(tags), 2)

		test.S(t).ExpectEquals(tags[0].TagName, "role")
		test.S(t).ExpectEquals(tags[0].TagValue, "")
		test.S(t).ExpectFalse(tags[0].Negate)
		test.S(t).ExpectFalse(tags[0].HasValue)

		test.S(t).ExpectEquals(tags[1].TagName, "dc")
		test.S(t).ExpectEquals(tags[1].TagValue, "")
		test.S(t).ExpectFalse(tags[1].Negate)
		test.S(t).ExpectFalse(tags[1].HasValue)
	}
	{
		tags, err := dtstruct.ParseIntersectTags("role=backup, !dc=ny")
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(len(tags), 2)

		test.S(t).ExpectEquals(tags[0].TagName, "role")
		test.S(t).ExpectEquals(tags[0].TagValue, "backup")
		test.S(t).ExpectFalse(tags[0].Negate)
		test.S(t).ExpectTrue(tags[0].HasValue)

		test.S(t).ExpectEquals(tags[1].TagName, "dc")
		test.S(t).ExpectEquals(tags[1].TagValue, "ny")
		test.S(t).ExpectTrue(tags[1].Negate)
		test.S(t).ExpectTrue(tags[1].HasValue)
	}
}
