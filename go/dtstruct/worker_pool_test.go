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
	"gitee.com/opengauss/ham4db/go/util/tests"
	"syscall"
	"testing"
)

func TestNewWorkerPool(t *testing.T) {
	tests.S(t).ExpectEquals(1, len(NewWorkerPool().workerMu.workers))
}

func TestWorkerPool_AsyncRun(t *testing.T) {
	count := 0
	workerName := "Test1"
	wp := NewWorkerPool()

	// normal case 1: create and run worker async
	tests.S(t).ExpectNil(wp.AsyncRun(workerName, func(workerExit chan struct{}) {
		<-workerExit
		count++
	}))

	// should have 2 workers (include done checker worker)
	tests.S(t).ExpectEquals(2, len(wp.workerMu.workers))

	// normal case 2: run task instance many times for worker
	instCount := 3
	for i := 0; i < instCount; i++ {
		tests.S(t).ExpectNil(wp.GetWorker(workerName).AsyncRun())
	}

	// should have `instCount+1` instance for worker
	tests.S(t).ExpectTrue(uint32(instCount+1) == *wp.workerMu.running[workerName])

	// normal case 3: run multi workers
	tests.S(t).ExpectNil(wp.AsyncRun("AAAA", func(workerExit chan struct{}) {
		<-workerExit
	}))
	tests.S(t).ExpectNil(wp.AsyncRun("ZZZZ", func(workerExit chan struct{}) {
		<-workerExit
	}))

	// show running tasks
	tests.S(t).ExpectTrue(wp.showWorkerRun() == "AAAA:1,Test1:4,ZZZZ:1")

	// failed case 1: create worker with same name, should get error
	tests.S(t).ExpectNotNil(wp.AsyncRun(workerName, func(workerExit chan struct{}) {
		<-workerExit
		count++
	}))

	// should have `instCount+1` instance for worker
	tests.S(t).ExpectTrue(uint32(instCount+1) == *wp.workerMu.running[workerName])

	wp.Exit(syscall.SIGTERM)
	wp.WaitStop()

	// worker should be run `instCount+1` times
	tests.S(t).ExpectEquals(instCount+1, count)

	// failed case 2: run worker should get error after worker pool stopped
	tests.S(t).ExpectNotNil(wp.AsyncRun("XXXX", func(workerExit chan struct{}) {
		<-workerExit
		count++
	}))
}
