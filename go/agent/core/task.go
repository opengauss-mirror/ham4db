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
	"context"
	"gitee.com/opengauss/ham4db/go/agent/adaptor"
	dtstruct2 "gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/dtstruct"
	"gitee.com/opengauss/ham4db/go/agent/adaptor/opengauss/handler"
	aconstant "gitee.com/opengauss/ham4db/go/agent/common/constant"
	adtstruct "gitee.com/opengauss/ham4db/go/agent/dtstruct"
	"gitee.com/opengauss/ham4db/go/agent/util"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"net"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"
)

// RunHealthCheck run health check for this agent, will send heartbeat message periodically
func RunHealthCheck(wp *dtstruct.WorkerPool, args *adtstruct.Args) error {

	// check server address, TODO more check
	if args.ServerAddr == "" {
		log.Warning("no server address in args:%v, so will not do health check", args)
		return nil
	}

	// run health check async
	return wp.AsyncRun(aconstant.WorkerNameKeepAlive, func(workerExit chan struct{}) {
		healthCheck := time.Tick(time.Duration(args.CheckInterval) * time.Second)
		for {
			select {
			case <-healthCheck:
				hostname, err := os.Hostname()
				if err != nil {
					log.Error("got error when get hostname: %s", err)
				}
				_ = KeepAlive(args.ServerAddr, args.AgtIp, hostname, args.AgtPort, args.CheckInterval)
			case <-workerExit:
				return
			}
		}
	})
}

// RunGrpcServer run grpc server to handler grpc request from client
func RunGrpcServer(wp *dtstruct.WorkerPool, args *adtstruct.Args) error {

	// check ip and port
	if err := util.CheckIPAndPort(args.AgtIp, args.AgtPort); err != nil {
		return err
	}

	// create new grpc server and register opengauss server
	s := grpc.NewServer()
	dtstruct2.RegisterOpengaussAgentServer(s, &handler.OpenGaussServer{}) // TODO use adaptor

	// if got error, stop grpc server gracefully
	var err error
	defer func() {
		if err != nil {
			s.GracefulStop()
		}
	}()

	// run worker to start grpc server async
	if err = wp.AsyncRun(aconstant.WorkerNameGrpcServer, func(workerExit chan struct{}) {
		var lis net.Listener
		var errS error

		// if got error, trigger worker pool to exit and program exit also
		defer func() {
			if errS != nil {
				wp.Exit(syscall.SIGTERM)
			}
		}()
		if lis, errS = net.Listen("tcp", args.AgtIp+":"+strconv.Itoa(args.AgtPort)); errS != nil {
			log.Error("failed to listen, error:%s", errS)
			return
		}
		if errS = s.Serve(lis); errS != nil {
			log.Error("failed to serve, error:%s", errS)
		}
	}); err != nil {
		return err
	}

	// run worker to stop grpc server
	if err = wp.AsyncRun(aconstant.WorkerNameStopGrpcServer, func(workerExit chan struct{}) {
		<-workerExit
		s.GracefulStop()
	}); err != nil {
		return err
	}

	return err
}

// RunHttpServer run http server for agent test
func RunHttpServer(wp *dtstruct.WorkerPool, args *adtstruct.Args) error {

	// if disabled in args, just return
	if args.DisableHttp {
		log.Warning("http server has been disabled in args, so it will not be start")
		return nil
	}

	var err error

	// http server will listen on port with next +1
	listenPort := args.AgtPort + 1

	// check ip and port
	if err = util.CheckIPAndPort(args.AgtIp, listenPort); err != nil {
		return err
	}

	// create http server
	httpServer := adtstruct.HttpServer{Server: &http.Server{Addr: args.AgtIp + ":" + strconv.Itoa(listenPort), ReadTimeout: aconstant.HttpReadTimeout * time.Second}}
	httpServer.Limiter = rate.NewLimiter(aconstant.HttpLimit, aconstant.HttpLimitBurst)
	httpServer.Args = args
	httpServer.Process = func(limiter *rate.Limiter, args *adtstruct.Args, writer http.ResponseWriter) {

		// limit check
		if limiter.Allow() {

			// connect to agent server
			addr := args.AgtIp + ":" + strconv.Itoa(args.AgtPort)
			ctx, cancel := context.WithTimeout(context.TODO(), constant.GrpcTimeout*time.Second)
			defer cancel()
			conn, errH := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
			defer func() {
				if conn != nil {
					if errC := conn.Close(); errC != nil {
						log.Error("close conn, error:%s", errC)
					}
				}
			}()
			defer func() {
				if errH != nil {
					writer.WriteHeader(http.StatusInternalServerError)
					if _, errW := writer.Write([]byte(errH.Error())); errW != nil {
						log.Error("write to response, error:%s", errW)
					}
				}
			}()
			if errH != nil {
				log.Error("can not connect to %s, error:%s", addr, errH)
				return
			}

			// collect info by grpc
			oac := dtstruct2.NewOpengaussAgentClient(conn)
			dir, errH := oac.CollectInfo(context.TODO(), &dtstruct2.DatabaseInfoRequest{FromCache: false})
			if errH != nil {
				log.Error("collect info from %s, error:%s", addr, errH)
				return
			}
			writer.WriteHeader(http.StatusOK)
			if _, errW := writer.Write([]byte(dir.NodeInfo.BaseInfo.GsClusterName)); errW != nil {
				log.Error("write to response, error:%s", errW)
			}
		} else {
			log.Warning("traffic is restricted, max 1 request in 5 seconds")
			writer.WriteHeader(http.StatusTooManyRequests)
		}
	}
	httpServer.Server.Handler = httpServer

	// create http server and accept test request
	if err = wp.AsyncRun(aconstant.WorkerNameHttpServer, func(workerExit chan struct{}) {
		if errH := httpServer.Server.ListenAndServe(); errH != nil && errH != http.ErrServerClosed {
			log.Error("http server listen and server, error:%s", errH)
		}
	}); err != nil {
		return err
	}

	// create worker to stop http server when worker pool quit
	if err = wp.AsyncRun(aconstant.WorkerNameStopHttpServer, func(workerExit chan struct{}) {
		<-workerExit
		if httpServer.Server != nil {
			if errH := httpServer.Server.Shutdown(context.TODO()); errH != nil {
				log.Error("stop http server, error:%s", errH)
			}
		}
	}); err != nil {
		return err
	}

	return err
}

// RunAdaptorTask run adaptor task in worker pool
func RunAdaptorTask(wp *dtstruct.WorkerPool, args *adtstruct.Args) {
	for _, adp := range adaptor.GetAdaptorList() {
		adp.RunTask(wp, args)
	}
}
