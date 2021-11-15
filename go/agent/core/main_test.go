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
	adtstruct "gitee.com/opengauss/ham4db/go/agent/dtstruct"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/test/config"
	"gitee.com/opengauss/ham4db/go/util"
	"golang.org/x/time/rate"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

// random http port
var HttpPort = util.RandomPort(40000, 45000)
var GrpcPort = util.RandomPort(45000, 50000)

// httpServer for health check test
var httpServer adtstruct.HttpServer

// init init http server, return ok for every request
func init() {

	// disable log output
	config.TestConfigLog()

	// init http server for test
	httpServer = adtstruct.HttpServer{Server: &http.Server{Addr: "0.0.0.0:" + strconv.Itoa(HttpPort), ReadTimeout: constant.HttpReadTimeout * time.Second}}
	httpServer.Limiter = rate.NewLimiter(constant.HttpLimit, constant.HttpLimitBurst)
	httpServer.Process = func(limiter *rate.Limiter, args *adtstruct.Args, writer http.ResponseWriter) {
		writer.WriteHeader(http.StatusOK)
		if _, errW := writer.Write([]byte("OK")); errW != nil {
			log.Error("write to response, error:%s", errW)
		}
	}
	httpServer.Server.Handler = httpServer
	go func() {
		if err := httpServer.Server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server listen and server, error:%s", err)
		}
	}()

	// waiting 5 second to server start
	doneCheck, serverCheck := time.Tick(5*time.Second), time.Tick(100*time.Millisecond)
	for {
		select {
		case <-doneCheck:
			log.Error("take too long to start http server")
			os.Exit(1)
		case <-serverCheck:
			if resp, err := http.Get("http://" + httpServer.Server.Addr); err == nil && resp.StatusCode == 200 {
				return
			}
		}
	}
}

func TestMain(m *testing.M) {

	// run test
	exitCode := m.Run()

	// close http server if exist
	if err := httpServer.Server.Close(); err != nil {
		log.Error("%s", err)
	}

	// exit
	os.Exit(exitCode)
}
