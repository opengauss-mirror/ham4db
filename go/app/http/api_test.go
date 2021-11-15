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
package http

import (
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"testing"

	"github.com/go-martini/martini"

	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/log"
	test "gitee.com/opengauss/ham4db/go/util/tests"
)

func init() {
	config.Config.HostnameResolveMethod = "none"
	config.MarkConfigurationLoaded()
	log.SetLevel(log.ERROR)
}

func TestKnownPaths(t *testing.T) {
	m := martini.Classic()
	api := dtstruct.HttpAPI{}

	RegisterRequests(&api, m)

	pathsMap := make(map[string]bool)
	for _, path := range registerApiList {
		pathsMap[path] = true
	}
	test.S(t).ExpectTrue(pathsMap["/api/health,GET"])
	test.S(t).ExpectTrue(pathsMap["/api/lb-check,GET"])
	test.S(t).ExpectTrue(pathsMap["/api/:type/relocate-replicas/:host/:port/:belowHost/:belowPort,GET"])
}
