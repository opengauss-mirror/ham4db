/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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
	"fmt"
	"gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
)

// PutInstanceTag put new tag on given instance, if tag is exist, update its value and timestamp
func PutInstanceTag(instanceKey *dtstruct.InstanceKey, tag *dtstruct.Tag) (err error) {
	_, err = db.ExecSQL(`
			insert into 
				ham_database_instance_tag (hostname, port, db_type, cluster_id, tag_name, tag_value, last_updated_timestamp
			) values (
				?, ?, ?, ?, ?, ?, current_timestamp
			)
			on duplicate key update
			tag_value=values(tag_value),
			last_updated_timestamp=values(last_updated_timestamp)
		`,
		instanceKey.Hostname, instanceKey.Port, instanceKey.DBType, instanceKey.ClusterId, tag.TagName, tag.TagValue,
	)
	return err
}

// Untag delete tag from given instance, return all instance with tag
func Untag(instanceKey *dtstruct.InstanceKey, tag *dtstruct.Tag) (tagged *dtstruct.InstanceKeyMap, err error) {

	// check tag and instance
	if tag == nil {
		return nil, log.Errorf("tag should be not nil")
	}
	if tag.Negate {
		return nil, log.Errorf("does not support negation")
	}
	if instanceKey == nil && !tag.HasValue {
		return nil, log.Errorf("either indicate an instance or a tag value. will not delete on-valued tag across instances")
	}

	// get where clause
	clause := ``
	args := sqlutil.Args()
	if tag.HasValue {
		clause = `tag_name = ? and tag_value = ?`
		args = append(args, tag.TagName, tag.TagValue)
	} else {
		clause = `tag_name = ?`
		args = append(args, tag.TagName)
	}
	if instanceKey != nil {
		clause = fmt.Sprintf("%s and hostname = ? and port = ? and cluster_id = ?", clause)
		args = append(args, instanceKey.Hostname, instanceKey.Port, instanceKey.ClusterId)
	}

	// query to get all instance with tag
	tagged = dtstruct.NewInstanceKeyMap()
	query := fmt.Sprintf(`
			select
				db_type, hostname, port, cluster_id
			from
				ham_database_instance_tag
			where
				%s
			order by hostname, port
		`, clause,
	)
	if err = db.Query(query, args, func(m sqlutil.RowMap) error {
		instKey, rerr := NewResolveInstanceKey(m.GetString("db_type"), m.GetString("hostname"), m.GetInt("port"))
		if rerr != nil {
			return rerr
		}
		instKey.ClusterId = m.GetString("cluster_id")
		tagged.AddKey(*instKey)
		return nil
	}); err != nil {
		return nil, err
	}

	// delete all tags
	if _, err = db.ExecSQL(fmt.Sprintf(`delete from ham_database_instance_tag where %s`, clause), args...); err != nil {
		return nil, log.Errore(err)
	}

	// audit this operation
	AuditOperation("delete-instance-tag", instanceKey, "", tag.String())

	return tagged, nil
}

// ReadInstanceTag check if tag is exist on given instance and update tag param value when exist
func ReadInstanceTag(instanceKey *dtstruct.InstanceKey, tag *dtstruct.Tag) (tagExist bool, err error) {
	query := `
		select
			tag_value
		from
			ham_database_instance_tag
		where
			hostname = ? and port = ? and cluster_id = ? and tag_name = ?
	`
	err = db.Query(query, sqlutil.Args(instanceKey.Hostname, instanceKey.Port, instanceKey.ClusterId, tag.TagName), func(m sqlutil.RowMap) error {
		tag.TagValue = m.GetString("tag_value")
		tagExist = true
		return nil
	})
	return tagExist, log.Errore(err)
}

func InstanceTagExists(instanceKey *dtstruct.InstanceKey, tag *dtstruct.Tag) (tagExists bool, err error) {
	return ReadInstanceTag(instanceKey, &dtstruct.Tag{TagName: tag.TagName})
}

func ReadInstanceTags(instanceKey *dtstruct.InstanceKey) (tags []*dtstruct.Tag, err error) {
	tags = []*dtstruct.Tag{}
	query := `
		select
			tag_name, tag_value
		from
			ham_database_instance_tag
		where
			hostname = ?
			and port = ?
		order by tag_name
			`
	args := sqlutil.Args(instanceKey.Hostname, instanceKey.Port)
	err = db.Query(query, args, func(m sqlutil.RowMap) error {
		tag := &dtstruct.Tag{
			TagName:  m.GetString("tag_name"),
			TagValue: m.GetString("tag_value"),
		}
		tags = append(tags, tag)
		return nil
	})

	return tags, log.Errore(err)
}

func GetInstanceKeysByTag(tag *dtstruct.Tag) (tagged *dtstruct.InstanceKeyMap, err error) {
	if tag == nil {
		return nil, log.Errorf("GetInstanceKeysByTag: tag is nil")
	}
	clause := ``
	args := sqlutil.Args()
	if tag.HasValue && !tag.Negate {
		// exists and equals
		clause = `tag_name=? and tag_value=?`
		args = append(args, tag.TagName, tag.TagValue)
	} else if !tag.HasValue && !tag.Negate {
		// exists
		clause = `tag_name=?`
		args = append(args, tag.TagName)
	} else if tag.HasValue && tag.Negate {
		// exists and not equal
		clause = `tag_name=? and tag_value!=?`
		args = append(args, tag.TagName, tag.TagValue)
	} else if !tag.HasValue && tag.Negate {
		// does not exist
		clause = `1=1 group by hostname, port, db_type, cluster_id having sum(tag_name=?)=0`
		args = append(args, tag.TagName)
	}
	tagged = dtstruct.NewInstanceKeyMap()
	query := fmt.Sprintf(`
		select
			db_type,
			hostname,
			port,
			cluster_id
		from
			ham_database_instance_tag
		where
			%s
		order by hostname, port
		`, clause)
	err = db.Query(query, args, func(m sqlutil.RowMap) error {
		key, _ := NewResolveInstanceKey(dtstruct.GetDatabaseType(m.GetString("db_type")), m.GetString("hostname"), m.GetInt("port"))
		key.ClusterId = m.GetString("cluster_id")
		tagged.AddKey(*key)
		return nil
	})
	return tagged, log.Errore(err)
}

func GetInstanceKeysByTags(tagsString string) (tagged *dtstruct.InstanceKeyMap, err error) {
	tags, err := dtstruct.ParseIntersectTags(tagsString)
	if err != nil {
		return tagged, err
	}
	for i, tag := range tags {
		taggedByTag, err := GetInstanceKeysByTag(tag)
		if err != nil {
			return tagged, err
		}
		if i == 0 {
			tagged = taggedByTag
		} else {
			tagged = tagged.Intersect(taggedByTag)
		}
	}
	return tagged, nil
}
