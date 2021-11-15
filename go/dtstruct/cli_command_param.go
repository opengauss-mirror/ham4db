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

// CliParam used to hold all cli command param
type CliParam struct {
	Command                     string
	Strict                      bool
	DatabaseType                string
	ClusterId                   string
	Instance                    string
	Destination                 string
	Owner                       string
	Reason                      string
	Duration                    string
	Pattern                     string
	ClusterAlias                string
	Pool                        string
	HostnameFlag                string
	RawInstanceKey              *InstanceKey
	InstanceKey                 *InstanceKey
	DestinationKey              *InstanceKey
	PostponedFunctionsContainer *PostponedFunctionsContainer
}

// CommandDesc describe command detail: command name, which section it belongs to and description for it, it also have
// func that will be called when this command be executed
type CommandDesc struct {
	Command     string
	Category    string
	Section     string
	Description string
	Func        func(cliPrm *CliParam)
}

// CommandList used to sort command when output or execute help command
type CommandList []CommandDesc

func (cl CommandList) Len() int      { return len(cl) }
func (cl CommandList) Swap(i, j int) { cl[i], cl[j] = cl[j], cl[i] }
func (cl CommandList) Less(i, j int) bool {
	return cl[i].Section < cl[j].Section || (cl[i].Section == cl[j].Section && cl[i].Command < cl[j].Command)
}

type CommandSlice []string

func (cs CommandSlice) Len() int           { return len(cs) }
func (cs CommandSlice) Swap(i, j int)      { cs[i], cs[j] = cs[j], cs[i] }
func (cs CommandSlice) Less(i, j int) bool { return cs[i] < cs[j] }
