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
package config

import (
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/test/testdb/handler"
)

// TestConfigLog config log
func TestConfigLog() {

	// disable output log
	log.DisableOutput(true)

	// set fatal function, do nothing in it
	log.SetFatalFunc(func() {
		// nothing to do here
	})
}

// TestConfigDB config db
func TestConfigDB() {

	// set log
	TestConfigLog()

	// set backend database to testdb
	config.Config.BackendDB = constant.BackendDBTestDB

	// config current version
	dtstruct.RuntimeCLIFlags.ConfiguredVersion = "20.1.0"

	// config database adaptor
	dtstruct.HamHandlerMap[constant.BackendDBTestDB] = &handler.TestDB{}
	dtstruct.DBTypeMap[constant.BackendDBTestDB] = true
}
