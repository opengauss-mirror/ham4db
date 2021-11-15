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
	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/common/constant"
	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/dtstruct"
	"gitee.com/opengauss/ham4db/go/core/log"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// UpdateHostAndPort replace ip in key hostname use real host name
func UpdateHostAndPort(kvs map[string]string, ip string, hostname string, port string) {
	if kvs["hostname"] == ip {
		kvs["hostname"] = hostname
		kvs["port"] = port
	}
}

// OutputToMap filter valid output, transfer it to key/value, check if result count is in count list
func OutputToMap(cmdOutput []string, countList []int, otype string) (map[string]string, error) {

	// transfer to key/value, all to lower case
	infoMap := make(map[string]string)
	for _, line := range cmdOutput {
		switch otype {
		case constant.OutputTypeCmd:
			idx := strings.Index(line, ":")
			if idx > 0 {
				infoMap[KeyToField(strings.TrimSpace(line[0:idx]))] = strings.ToLower(strings.TrimSpace(line[idx+1:]))
				continue
			}

			// TODO do more check
			if strings.TrimSpace(line) != "" && strings.ReplaceAll(line, "-", "") != "" {
				log.Warning("unprocessed line:%s", line)
			}
		case constant.OutputTypeEnv:
			idx := strings.Index(line, "=")
			if idx == -1 || idx == len(line)-1 {
				return nil, log.Errorf("wrong format:%s", line)
			}
			infoMap[KeyToField(strings.ToLower(line[0:idx]))] = line[idx+1:]
		default:
			log.Warning("output type:%s is not supported", otype)
		}
	}

	// check if output of command is as expected, TODO check key of command output not only by key count
	if countList != nil && len(countList) > 0 {
		match := false
		for _, count := range countList {
			if count == -1 || len(infoMap) == count {
				match = true
				break
			}
		}
		if !match {
			return nil, log.Errorf("output has been changed, expected:%v, please check:%s", countList, cmdOutput)
		}
	}

	return infoMap, nil
}

// KeyToField format key to struct field name, e.g. instance_id -> InstanceId
// TODO key demo: datanodePort or HA_state or instance_state, key is not unified, this is bad design
func KeyToField(key string) string {
	if strings.Contains(key, "_") {
		key = strings.ToLower(key)
	}
	idx, rep := 0, 1
	byteVal := []byte(key)
	if ok, _ := regexp.MatchString(constant.RegexPatternKey, key); ok {
		for i := range byteVal {
			if '_' == byteVal[i] {
				idx += 1
				rep = 1
				continue
			}
			if rep == 1 && 'a' <= byteVal[i] && byteVal[i] <= 'z' {
				byteVal[i-idx] = byteVal[i] - ('a' - 'A')
			} else {
				byteVal[i-idx] = byteVal[i]
			}
			rep = 0
		}
	}
	return string(byteVal[:len(key)-idx])
}

// ResetValue reset field of struct instance value using key/value in info map
func ResetValue(structInstance interface{}, infoMap map[string]string, ignoreMiss bool) error {

	// check if interface is pointer
	baseInfoType := reflect.TypeOf(structInstance)
	if reflect.ValueOf(structInstance).Kind() == reflect.Ptr {
		baseInfoType = reflect.ValueOf(structInstance).Elem().Type()
	}

	// reset instance field value using info map
	for i := 0; i < baseInfoType.NumField(); i++ {
		fieldName := baseInfoType.Field(i).Name
		fieldVal := reflect.ValueOf(structInstance).Elem().FieldByName(fieldName)
		if baseInfoType.Field(i).Tag != "" && fieldVal.IsValid() {
			if val, ok := infoMap[fieldName]; ok {
				switch fieldVal.Kind() {
				case reflect.String:
					fieldVal.SetString(val)
				case reflect.Int32:
					intVal, err := strconv.ParseInt(val, 10, 64)
					if err != nil {
						return err
					}
					fieldVal.SetInt(intVal)
				default:
					log.Error("miss type for field:%s", fieldName)
				}
				continue
			}
			if !ignoreMiss {
				return log.Errorf("miss value for field: %s", fieldName)
			}
		}
	}
	return nil
}

// ParseChannel parse channel info from gs_ctl query
func ParseChannel(topology string, ctype string, portMap map[string]string) (*dtstruct.BaseInfo, error) {

	// use regex to find match string
	stream := regexp.MustCompile(ctype+"[0-9]+[.][0-9]+[.][0-9]+[.][0-9]+:[0-9]+$").FindAllString(topology, 1)
	if len(stream) != 1 {
		return nil, log.Errorf("wrong format topology info:%s", topology)
	}

	// set ip/hostname/port to map
	infoMap := make(map[string]string)
	ipAndPort := strings.Split(stream[0][len(ctype):], ":")
	infoMap["NodeIp"] = ipAndPort[0]
	if val, ok := portMap[ipAndPort[0]]; ok {
		if idx := strings.Index(val, ":"); idx > -1 {
			infoMap["NodeName"] = val[0:idx]
			infoMap["DatanodePort"] = val[idx+1:]
		} else {
			return nil, log.Errorf("wrong format:%s", val)
		}
	} else {
		return nil, log.Errorf("miss port for ip:%s", ipAndPort[0])
	}

	// map to base info
	baseInfo := &dtstruct.BaseInfo{}
	if err := ResetValue(baseInfo, infoMap, true); err != nil {
		return nil, err
	}
	return baseInfo, nil
}
