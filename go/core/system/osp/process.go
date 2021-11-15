/*
   Copyright 2014 Outbrain Inc.

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
	"gitee.com/opengauss/ham4db/go/common/constant"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/log"
)

// CommandRun executes some text as a command. This is assumed to be text that will be run by a shell so we need to write out the
// command to a temporary file and then ask the shell to execute it, after which the temporary file is removed.
func CommandRun(command string, env []string, args ...string) error {
	log.Infof("command  input: %v, %+v, %+v", command, env, args)

	// generate shell script
	cmd, shellScript, err := generateScript(command, env, args...)
	defer os.Remove(shellScript)
	if err != nil {
		return log.Errore(err)
	}

	// exec shell script
	log.Infof("command script: %s", strings.Join(cmd.Args, " "))
	output, err := cmd.CombinedOutput()
	log.Infof("command output: %s", output)

	var waitStatus syscall.WaitStatus
	// command failed, did the command fail because of an unsuccessful exit code
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			waitStatus = exitError.Sys().(syscall.WaitStatus)
		}
		return log.Errorf("command failed, error:%s, exit status: %d, output:%s", err, waitStatus.ExitStatus(), output)
	}

	// command success
	waitStatus = cmd.ProcessState.Sys().(syscall.WaitStatus)
	log.Infof("command success. exit status %d", waitStatus.ExitStatus())

	return nil
}

// generateScript generates a temporary shell script based on
// the given command to be executed, writes the command to a temporary
// file and returns the exec.Command which can be executed together
// with the script name that was created.
func generateScript(commandText string, env []string, arguments ...string) (*exec.Cmd, string, error) {
	var err error

	// create tmp file
	var tmpFile *os.File
	if tmpFile, err = ioutil.TempFile("", constant.OSTempFilePatten); err != nil {
		return nil, "", log.Errorf("failed to create temp file: %s", err)
	}

	// write command to temporary file
	if err = ioutil.WriteFile(tmpFile.Name(), []byte(commandText), 0640); err != nil {
		return nil, "", log.Errorf("failed to write command to temp file: %s", err)
	}

	// generate command and init its env
	cmd := exec.Command(config.Config.ProcessesShellCommand, append([]string{tmpFile.Name()}, arguments...)...)
	cmd.Env = env

	return cmd, tmpFile.Name(), nil
}
