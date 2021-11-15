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
package cache

import (
	"context"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"time"
)

var grpcCache *dtstruct.Cache

func init() {
	grpcCache = NewCache(constant.CacheGrpcName, constant.CacheGrpcExpire, constant.CacheGrpcCleanupInterval*time.Second)
	grpcCache.OnEvicted(func(target string, conn interface{}) {
		switch conn.(type) {
		case *grpc.ClientConn:
			if err := conn.(*grpc.ClientConn).Close(); err != nil {
				log.Error("close connection for %s, error:%s", target, err)
			}
		}
	})
}

// GetGRPC return grpc conn in cache, if not exist, create it with default conn expire time
func GetGRPC(ctx context.Context, target string, opts ...grpc.DialOption) (conn *grpc.ClientConn, err error) {
	return GetGRPCWithFunc(ctx, util.RandomExpirationSecond(constant.CacheGrpcExpire), target, func(atomC chan struct{}) { <-atomC }, opts...)
}

// GetGRPCWithExpire return grpc conn in cache and reset conn expire time, if not exist, create it with given expire time
func GetGRPCWithExpire(ctx context.Context, expire time.Duration, target string, opts ...grpc.DialOption) (conn *grpc.ClientConn, err error) {
	return GetGRPCWithFunc(ctx, expire, target, func(atomC chan struct{}) { <-atomC }, opts...)
}

// GetGRPCWithFunc return grpc conn in cache and reset conn expire time.
// if get read/write race, using function in param. if not exist in cache, create it with given expire time
func GetGRPCWithFunc(ctx context.Context, expire time.Duration, target string, blockFunc func(atomC chan struct{}), opts ...grpc.DialOption) (*grpc.ClientConn, error) {

	span, ctx := opentracing.StartSpanFromContext(ctx, "Grpc Connect")
	defer span.Finish()

	val, err := grpcCache.GetWithFunc(
		expire,
		target,
		func(val interface{}) (valid bool) {
			conn := val.(*grpc.ClientConn)
			// check if conn has been closed, TODO maybe need more check
			if conn.GetState() != connectivity.Shutdown {
				valid = true
			}
			return
		},
		func() (interface{}, error) {
			gctx, cancel := context.WithTimeout(ctx, constant.GrpcTimeout*time.Second)
			defer cancel()
			return grpc.DialContext(gctx, target, opts...)
		},
		blockFunc,
	)
	if err == nil {
		return val.(*grpc.ClientConn), nil
	}
	return nil, log.Errorf("can not get grpc connection for key: %s, %s", target, err)
}
