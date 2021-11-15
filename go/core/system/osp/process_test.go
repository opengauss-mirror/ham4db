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
package osp

import (
	"gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
)

func TestCommandRun(t *testing.T) {

	// normal case 1: command run successfully
	tests.S(t).ExpectTrue(CommandRun("echo \"VAR1=$VAR1 VAR2=$VAR2\"", []string{"VAR1=a", "VAR2=b"}) == nil)

	// failed case 1: command run failed
	cmdErr := CommandRun("echo \"VAR1=$VAR1 VAR2=$VAR2\" && exit 11", []string{"VAR1=a", "VAR2=b"})
	tests.S(t).ExpectTrue(cmdErr != nil)
}
