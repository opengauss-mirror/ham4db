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
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/log"
)

var InstanceAdaptorMap = make(map[string]InstanceAdaptor)
var HamHandlerMap = make(map[string]HamHandler)
var DBTypeMap = make(map[string]bool)
var DBTypeDefaultPortMap = make(map[string]int)

// GetInstanceAdaptor get instance handler for database type
func GetInstanceAdaptor(dbt string) InstanceAdaptor {
	if ihdl, ok := InstanceAdaptorMap[dbt]; ok {
		return ihdl
	}
	log.Error("unknown database type:%s, will use default testdb", dbt)
	return InstanceAdaptorMap[constant.TestDB]
}

// GetHamHandler get ham handler for database type
func GetHamHandler(dbt string) HamHandler {
	if hhdl, ok := HamHandlerMap[dbt]; ok {
		return hhdl
	}
	log.Error("unknown database type:%s, will use default testdb", dbt)
	return HamHandlerMap[constant.TestDB]
}

// GetDatabaseType check if database type is exist and enabled
func GetDatabaseType(dbt string) string {
	if _, ok := DBTypeMap[dbt]; ok {
		return dbt
	}
	// TODO maybe audit and trigger alert
	log.Error("unknown database type:%s, will use default testdb", dbt)
	return constant.TestDB
}

// IsTypeValid check if database type is valid
func IsTypeValid(dbt string) (ok bool) {
	_, ok = DBTypeMap[dbt]
	return
}

// GetDatabaseType check if database type is exist and enabled
func GetDefaultPort(dbt string) int {
	if port, ok := DBTypeDefaultPortMap[dbt]; ok {
		return port
	}
	log.Error("unknown database type:%s, will use default testdb default port", dbt)
	return DBTypeDefaultPortMap[constant.TestDB]
}
