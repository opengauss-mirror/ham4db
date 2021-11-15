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
package test

import (
	"gitee.com/opengauss/ham4db/go/common/constant"
	dbinit "gitee.com/opengauss/ham4db/go/core/db/init"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/test/config"
	"os"
)

func init() {
	config.TestConfigDB()
}

func DBTestInit() {

	// create test db dir
	if err := os.Mkdir(constant.TestDBDir, 0755); err != nil {
		log.Fatal("cann't create test db dir:%s, error:%s", constant.TestDBDir, err)
	}

	_ = dbinit.BackendDBInit()
}
