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
package cli

import (
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/config"
	dbtest "gitee.com/opengauss/ham4db/go/test"
	test "gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
)

func init() {
	config.Config.HostnameResolveMethod = "none"
	config.MarkConfigurationLoaded()
	dbtest.DBTestInit()
}

func TestHelp(t *testing.T) {
	Cli(constant.TestDB, "help", false, constant.TestDB, "", "localhost:9999", "localhost:9999", "orc", "no-reason", "1m", ".", "no-alias", "no-pool", "")
	test.S(t).ExpectTrue(len(commandMap) == 0)
}

func TestKnownCommands(t *testing.T) {
	Cli("", "help", false, "", "", "localhost:9999", "localhost:9999", "orc", "no-reason", "1m", ".", "no-alias", "no-pool", "")

	commandsMap := make(map[string]string)
	for _, command := range commandMap {
		commandsMap[command.Command] = command.Section
	}
	test.S(t).ExpectEquals(commandsMap["topology"], "topology")
	test.S(t).ExpectEquals(commandsMap["global-recovery-disable"], "global")
}
