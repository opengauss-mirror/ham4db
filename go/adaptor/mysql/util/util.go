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
package util

import (
	"fmt"
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/common/constant"
	mdtstruct "gitee.com/opengauss/ham4db/go/adaptor/mysql/dtstruct"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"regexp"
	"strings"
)

// FilterInstancesByPattern will filter given array of instances according to regular expression pattern
func FilterInstancesByPattern(instances []*mdtstruct.MysqlInstance, pattern string) []*mdtstruct.MysqlInstance {
	if pattern == "" {
		return instances
	}
	filtered := []*mdtstruct.MysqlInstance{}
	for _, instance := range instances {
		if matched, _ := regexp.MatchString(pattern, instance.GetInstance().Key.DisplayString()); matched {
			filtered = append(filtered, instance)
		}
	}
	return filtered
}

// removeInstance will remove an instance from a list of instances
func RemoveInstance(instances []*mdtstruct.MysqlInstance, instanceKey *dtstruct.InstanceKey) []*mdtstruct.MysqlInstance {
	if instanceKey == nil {
		return instances
	}
	for i := len(instances) - 1; i >= 0; i-- {
		if instances[i].GetInstance().Key.Equals(instanceKey) {
			instances = append(instances[:i], instances[i+1:]...)
		}
	}
	return instances
}

// AddInstances adds keys of all given instances to this map
func AddInstances(instMap *dtstruct.InstanceKeyMap, instances []*mdtstruct.MysqlInstance) {
	for _, instance := range instances {
		instMap.AddKey(instance.GetInstance().Key)
	}
}

// removeNilInstances
func RemoveNilInstances(instances []*mdtstruct.MysqlInstance) []*mdtstruct.MysqlInstance {
	for i := len(instances) - 1; i >= 0; i-- {
		if instances[i] == nil {
			instances = append(instances[:i], instances[i+1:]...)
		}
	}
	return instances
}

func ToInstanceHander(mis []*mdtstruct.MysqlInstance) []dtstruct.InstanceAdaptor {
	var ih []dtstruct.InstanceAdaptor
	for _, mi := range mis {
		ih = append(ih, mi)
	}
	return ih
}

func ToMysqlInstance(ihs []dtstruct.InstanceAdaptor) []*mdtstruct.MysqlInstance {
	ih := make([]*mdtstruct.MysqlInstance, len(ihs))
	for _, mi := range ihs {
		ih = append(ih, mi.(*mdtstruct.MysqlInstance))
	}
	return ih
}

// check if the event is one we want to skip.
func SpecialEventToSkip(event *dtstruct.BinlogEvent) bool {
	if event != nil && strings.Index(event.Info, constant.AnonymousGTIDNextEvent) >= 0 {
		return true
	}
	return false
}

func GetInstanceBinlogEntryKey(instanceKey *dtstruct.InstanceKey, entry string) string {
	return fmt.Sprintf("%s;%s", instanceKey.DisplayString(), entry)
}

// Is this an error which means that we shouldn't try going more queries for this discovery attempt?
func UnrecoverableError(err error) bool {
	contains := []string{
		constant.Error1045AccessDenied,
		constant.ErrorConnectionRefused,
		constant.ErrorIOTimeout,
		constant.ErrorNoSuchHost,
	}
	for _, k := range contains {
		if strings.Contains(err.Error(), k) {
			return true
		}
	}
	return false
}
