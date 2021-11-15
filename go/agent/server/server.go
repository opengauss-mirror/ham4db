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
package main

import (
	"flag"
	"gitee.com/opengauss/ham4db/go/agent/common/constant"
	"gitee.com/opengauss/ham4db/go/agent/core"
	adtstruct "gitee.com/opengauss/ham4db/go/agent/dtstruct"
	"gitee.com/opengauss/ham4db/go/common"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	args := &adtstruct.Args{}

	// check server addr, must match url regex, just like example in usage
	flag.Func("addr-server", "server http addr with port, e.g. http://192.168.10.100:3000", func(serverAddr string) error {
		if common.HttpAddr.MatchString(serverAddr) {
			args.ServerAddr = serverAddr
			return nil
		}
		return log.Errorf("wrong server addr:%s, see usage for more detail", serverAddr)
	})
	flag.StringVar(&args.AgtIp, "agt-ip", "0.0.0.0", "agent ip")
	flag.IntVar(&args.AgtPort, "agt-port", 15000, "agent port, if http server enabled, it will listen on port: agt-port+1")
	flag.IntVar(&args.CheckInterval, "check-interval", constant.HealthCheckTick, "health check interval(second)")
	flag.BoolVar(&args.DisableHttp, "disable-http", false, "disable http api or not")
	flag.IntVar(&args.RefreshInterval, "refresh-interval", constant.RefreshTick, "refresh interval(second)")
	flag.Parse()

	run(args)
}

// run start all task for agent
func run(args *adtstruct.Args) {
	log.Infof("start agent")

	// create worker pool to process task
	wp := dtstruct.NewWorkerPool()

	// catch system exit signal
	signal.Notify(wp.GetExitChannel(), os.Kill, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGKILL)

	// start health check task if need to send heartbeat to server
	_ = core.RunHealthCheck(wp, args)

	// if get error when run grpc server, we should quit right now
	if core.RunGrpcServer(wp, args) != nil {
		wp.Exit(syscall.SIGTERM)
	}

	// start http server for test
	_ = core.RunHttpServer(wp, args)

	// run adaptor task
	core.RunAdaptorTask(wp, args)

	// log successful info
	log.Infof("agent has been started successfully")

	// notify by system exit signal
	wp.WaitStop()
}
