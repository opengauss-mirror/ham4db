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
	"gitee.com/opengauss/ham4db/go/adaptor/opengauss/common/constant"
	odtstruct "gitee.com/opengauss/ham4db/go/adaptor/opengauss/dtstruct"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"reflect"
	"regexp"
)

// ToInstanceHandler cast opengauss instance to instance adaptor
func ToInstanceHandler(mis []*odtstruct.OpenGaussInstance) []dtstruct.InstanceAdaptor {

	// check if nil or empty
	if mis == nil || len(mis) == 0 {
		return []dtstruct.InstanceAdaptor{}
	}

	// copy and cast to
	var ih []dtstruct.InstanceAdaptor
	for _, mi := range mis {
		ih = append(ih, mi)
	}
	return ih
}

// UpdatePhysicalLocation update instance physical location using pattern in config
func UpdatePhysicalLocation(instance *odtstruct.OpenGaussInstance, configPattern string, fieldName string) {
	if configPattern != "" {
		if pattern, err := regexp.Compile(configPattern); err == nil {
			match := pattern.FindStringSubmatch(instance.Key.Hostname)
			if len(match) != 0 {
				field := reflect.ValueOf(instance).Elem().FieldByName(fieldName)
				if field.IsValid() {
					field.SetString(match[1])
				} else {
					log.Error("invalid field name:%s", fieldName)
				}
			}
		}
	}
}

// NeedRecover check if state is not normal and need to recover
func NeedRecover(state string) bool {
	switch state {
	case constant.DbAbnormal, constant.DbManually, constant.DBDown, constant.DbUnknown:
		return true
	default:
		return false
	}
}
