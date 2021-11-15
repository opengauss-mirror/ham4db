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
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"sort"
	"strings"
)

// CommandUsage show all command usage detail
func CommandUsage(commandMap map[string]dtstruct.CommandDesc) string {

	// sort all command in map
	var commandList dtstruct.CommandList
	for _, cmdDesc := range commandMap {
		commandList = append(commandList, cmdDesc)
	}
	sort.Sort(commandList)

	// format output
	var outList []string
	for i, cmdDesc := range commandList {
		if i == 0 || commandList[i-1].Section != commandList[i].Section {
			outList = append(outList, fmt.Sprintf("%s:", cmdDesc.Section))
		}
		outList = append(outList, fmt.Sprintf("\t%-40s%s", cmdDesc.Command, cmdDesc.Description))
	}
	return fmt.Sprintf(`Command (-c): 
%+v 
Run 'help <command>' for detail on given command, e.g. 'ham4db help relocate'

Usage for most command:
       ham4db -c <command> [-i <instance.fqdn>[,<instance.fqdn>]* ] [-d <destination.fqdn>] [--verbose|--debug]
`, strings.Join(outList, "\n"))
}

// RegisterCliCommand register cli command to command map
func RegisterCliCommand(commandMap map[string]dtstruct.CommandDesc, command string, section string, description string, f func(cliParam *dtstruct.CliParam)) {
	commandMap[command] = dtstruct.CommandDesc{Command: command, Section: section, Description: description, Func: f}
}
