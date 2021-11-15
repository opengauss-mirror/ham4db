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
	"fmt"
	"gitee.com/opengauss/ham4db/go/common"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"strconv"
	"strings"
)

// NewResolveInstanceKey
func NewResolveInstanceKey(dbt string, hostname string, port int) (*dtstruct.InstanceKey, error) {
	return newInstanceKey(dbt, hostname, port, true)
}

// NewResolveInstanceKeyStrings creates and resolves a new instance key based on string params
func NewResolveInstanceKeyStrings(dbt string, hostname string, port string) (*dtstruct.InstanceKey, error) {
	return newInstanceKeyStrings(dbt, hostname, port, true)
}

// NewRawInstanceKeyStrings creates a new instance key based on string params without resolve
func NewRawInstanceKeyStrings(dbt, hostname string, port string) (*dtstruct.InstanceKey, error) {
	return newInstanceKeyStrings(dbt, hostname, port, false)
}

func ParseResolveInstanceKey(dbt string, hostPort string) (instanceKey *dtstruct.InstanceKey, err error) {
	return parseRawInstanceKey(dbt, hostPort, true)
}

func ParseRawInstanceKey(dbt string, hostPort string) (instanceKey *dtstruct.InstanceKey, err error) {
	return parseRawInstanceKey(dbt, hostPort, false)
}

func ResolveHostnameForInstanceKey(instanceKey *dtstruct.InstanceKey) (*dtstruct.InstanceKey, error) {

	if !instanceKey.IsValid() {
		return instanceKey, nil
	}

	// TODO double check
	hostname, err := ResolveHostname(instanceKey.Hostname)
	if err == nil {
		instanceKey.Hostname = hostname
	}
	return instanceKey, nil
}

// ReadJson unmarshalls a json into this map
func ReadCommaDelimitedList(instKeyMap *dtstruct.InstanceKeyMap, dbt string, list string) error {
	tokens := strings.Split(list, ",")
	for _, token := range tokens {
		key, err := ParseResolveInstanceKey(dbt, token)
		if err != nil {
			return err
		}
		instKeyMap.AddKey(*key)
	}
	return nil
}

func parseRawInstanceKey(dbt string, hostPort string, resolve bool) (instanceKey *dtstruct.InstanceKey, err error) {
	hostname := ""
	port := ""
	if submatch := common.Ipv4HostPortRegexp.FindStringSubmatch(hostPort); len(submatch) > 0 {
		hostname = submatch[1]
		port = submatch[2]
	} else if submatch = common.Ipv4HostRegexp.FindStringSubmatch(hostPort); len(submatch) > 0 {
		hostname = submatch[1]
	} else if submatch = common.Ipv6HostPortRegexp.FindStringSubmatch(hostPort); len(submatch) > 0 {
		hostname = submatch[1]
		port = submatch[2]
	} else if submatch = common.Ipv6HostRegexp.FindStringSubmatch(hostPort); len(submatch) > 0 {
		hostname = submatch[1]
	} else {
		return &dtstruct.InstanceKey{DBType: dtstruct.GetDatabaseType(dbt)}, fmt.Errorf("Cannot parse address: %s", hostPort)
	}
	if port == "" {
		port = fmt.Sprintf("%d", dtstruct.GetDefaultPort(dbt))
	}
	return newInstanceKeyStrings(dbt, hostname, port, resolve)
}

// newInstanceKeyStrings
func newInstanceKeyStrings(dbt string, hostname string, port string, resolve bool) (*dtstruct.InstanceKey, error) {

	// check if
	portInt, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %s", port)
	}
	return newInstanceKey(dtstruct.GetDatabaseType(dbt), hostname, portInt, resolve)
}

func newInstanceKey(dbt string, hostname string, port int, resolve bool) (instanceKey *dtstruct.InstanceKey, err error) {

	if hostname == "" {
		return &dtstruct.InstanceKey{DBType: dbt}, log.Errorf("NewResolveInstanceKey: Empty hostname")
	}

	instanceKey = &dtstruct.InstanceKey{Hostname: hostname, Port: port, DBType: dbt}
	if resolve {
		instanceKey, err = ResolveHostnameForInstanceKey(instanceKey)
	}
	return instanceKey, err
}
