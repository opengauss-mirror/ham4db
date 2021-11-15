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
package core

import (
	"gitee.com/opengauss/ham4db/go/agent/common/constant"
	"gitee.com/opengauss/ham4db/go/agent/dtstruct"
	dtstruct2 "gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/tests"
	"net"
	"net/http"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestRunHealthCheck(t *testing.T) {

	// create worker pool
	wp := dtstruct2.NewWorkerPool()

	// normal case 1: will run task successfully, but get error response because of no server exist
	tests.S(t).ExpectNil(RunHealthCheck(wp, &dtstruct.Args{ServerAddr: "http://0.0.0.0:" + strconv.Itoa(HttpPort), CheckInterval: 1}))
	if worker := wp.GetWorker(constant.WorkerNameKeepAlive); worker != nil {
		wp.RemoveWorker(constant.WorkerNameKeepAlive)
	}
	// failed case 1: wrong server addr, will quit directly
	tests.S(t).ExpectNil(RunHealthCheck(wp, &dtstruct.Args{ServerAddr: "http://0.0.0.0:" + strconv.Itoa(HttpPort+1), CheckInterval: 1}))
	tests.S(t).ExpectNil(RunHealthCheck(wp, &dtstruct.Args{ServerAddr: ""}))

	// quit work pool
	time.Sleep(2 * time.Second)
	wp.Exit(syscall.SIGTERM)
	wp.WaitStop()
}

func TestRunGrpcServer(t *testing.T) {

	// create worker pool
	wp := dtstruct2.NewWorkerPool()

	// failed case 1: wrong ip or port
	tests.S(t).ExpectNotNil(RunGrpcServer(wp, &dtstruct.Args{AgtIp: ""}))
	tests.S(t).ExpectNotNil(RunGrpcServer(wp, &dtstruct.Args{AgtPort: 0}))

	// failed case 2: port has already been listened on by other process, will run task successfully but get error in task and will quit work pool
	lis, _ := net.Listen("tcp", "0.0.0.0:11111")
	defer func() {
		if lis != nil {
			lis.Close()
		}
	}()
	tests.S(t).ExpectNil(RunGrpcServer(wp, &dtstruct.Args{AgtIp: "127.0.0.1", AgtPort: 11111}))

	// failed case 3: already has grpc worker with same name
	tests.S(t).ExpectNotNil(RunGrpcServer(wp, &dtstruct.Args{AgtIp: "127.0.0.1", AgtPort: 11111}))

	// quit work pool
	time.Sleep(1 * time.Second)
	wp.WaitStop()
}

func TestRunHttpServer(t *testing.T) {

	// simulate cli command args
	args := &dtstruct.Args{AgtIp: "127.0.0.1", AgtPort: GrpcPort}

	// http server test url
	addr := "http://" + args.AgtIp + ":" + strconv.Itoa(args.AgtPort+1)

	// create worker pool
	wp := dtstruct2.NewWorkerPool()

	// normal case 1: disable http server
	tests.S(t).ExpectNil(RunHttpServer(wp, &dtstruct.Args{DisableHttp: true}))
	tests.S(t).ExpectTrue(wp.GetWorker(constant.WorkerNameHttpServer) == nil)
	tests.S(t).ExpectTrue(wp.GetWorker(constant.WorkerNameStopHttpServer) == nil)

	// failed case 1: wrong ip or port
	tests.S(t).ExpectNotNil(RunHttpServer(wp, &dtstruct.Args{AgtIp: ""}))
	tests.S(t).ExpectNotNil(RunHttpServer(wp, &dtstruct.Args{AgtPort: 0}))

	// failed case 2: run http server successfully but without grpc server, got error when exec request
	tests.S(t).ExpectNil(RunHttpServer(wp, args))
	time.Sleep(2 * time.Second)
	resp, err := http.Get(addr)
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectNotNil(resp)
	tests.S(t).ExpectTrue(resp.StatusCode == http.StatusInternalServerError)

	// normal case 2: start grpc server, send request
	tests.S(t).ExpectNil(RunGrpcServer(wp, args))

	// send http request
	resp, err = http.Get(addr)
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectNotNil(resp)
	// TODO should check response body
	tests.S(t).ExpectTrue(resp.StatusCode == http.StatusInternalServerError || resp.StatusCode == http.StatusOK)

	// simulate more request in 5 second, will get 429
	for i := 0; i < 5; i++ {
		resp, err = http.Get(addr)
		tests.S(t).ExpectNil(err)
		tests.S(t).ExpectNotNil(resp)
		tests.S(t).ExpectTrue(resp.StatusCode == http.StatusTooManyRequests)
	}

	// quit work pool
	time.Sleep(1 * time.Second)
	wp.Exit(syscall.SIGTERM)
	wp.WaitStop()
}
