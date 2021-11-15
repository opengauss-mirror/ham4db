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
package constant

const (

	// command
	CmdGSOMStatusLocal = "gs_om -t status -h `hostname`"
	CmdEnv             = "env | grep -E '^GS_CLUSTER_NAME=|^GAUSS_VERSION='"
	CmdGSOMView        = "gs_om -t view | grep -E '^nodeName|^datanodePort'"
	CmdGSOMViewALL     = "gs_om -t view | grep -E '^nodeName|^datanodePort|^datanodeListenIP'"

	// output type
	OutputTypeCmd = "cmd"
	OutputTypeEnv = "env"

	// worker name
	WorkerNameAgentRefreshBaseInfo = "AgentRefreshBaseInfo"

	// regex pattern
	RegexPatternKey = "^[a-zA-Z_]+$"

	// version, use d(dot) as delimiter
	Version2d0d1 = "2.0.1"
	Version2d1d0 = "2.1.0"

	// replication direct
	ReplDirectUpstream   = "-->"
	ReplDirectDownStream = "<--"
)
