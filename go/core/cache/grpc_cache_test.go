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
	"gitee.com/opengauss/ham4db/go/util/tests"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestGetGRPC(t *testing.T) {
	var err error

	// simulate server addr
	addrf := func() string {
		return "127.0.0.1:" + strconv.Itoa(int(50000+rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(1000)))
	}

	// normal case 1: write concurrently
	wg := sync.WaitGroup{}
	concurrent := 100
	ccChan := make(chan *grpc.ClientConn, concurrent)
	addr1 := addrf()
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cc, errc := GetGRPC(context.TODO(), addr1, grpc.WithInsecure())
			tests.S(t).ExpectNil(errc)
			ccChan <- cc
		}()
	}
	wg.Wait()
	c1 := <-ccChan
	for i := 0; i < concurrent-1; i++ {
		tests.S(t).ExpectEquals(c1, <-ccChan)
	}

	// normal case 2: not exist, create and set to cache
	var conn *grpc.ClientConn
	addr2 := addrf()
	conn, err = GetGRPC(context.TODO(), addr2, grpc.WithInsecure())
	tests.S(t).ExpectNotNil(conn)
	tests.S(t).ExpectNil(err)

	// normal case 2: get from cache and reset expire time
	var cacheConn *grpc.ClientConn
	cacheConn, err = GetGRPCWithExpire(context.TODO(), 1*time.Microsecond, addr2, grpc.WithInsecure())
	tests.S(t).ExpectNotNil(cacheConn)
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectEquals(conn, cacheConn)

	// normal case 3: cache expire, will create new connect
	time.Sleep(1 * time.Microsecond)
	var newConn *grpc.ClientConn
	newConn, err = GetGRPCWithExpire(context.TODO(), 1*time.Microsecond, addr2, grpc.WithInsecure())
	tests.S(t).ExpectNotNil(newConn)
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectNotEquals(cacheConn, newConn)

	// normal case 4: cache expire, will close conn when clear cache, check conn state
	ClearCache()
	tests.S(t).ExpectEquals(cacheConn.GetState(), connectivity.Shutdown)
	tests.S(t).ExpectEquals(conn.GetState(), connectivity.Shutdown)
	tests.S(t).ExpectEquals(newConn.GetState(), connectivity.Shutdown)

	// failed case 1: with wrong server addr
	var failConn *grpc.ClientConn
	failConn, err = GetGRPCWithExpire(context.TODO(), 1*time.Microsecond, "", grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: time.Millisecond}),
		grpc.FailOnNonTempDialError(true),
	)
	tests.S(t).ExpectTrue(failConn == nil)
	tests.S(t).ExpectNotNil(err)

	//failed case 2: simulate slow read, and get from cache after key is expire
	addr3 := addrf()
	errChan := make(chan error, concurrent)
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errc := GetGRPCWithFunc(context.TODO(), 1*time.Millisecond, addr3, func(atomC chan struct{}) {
				<-atomC
				time.Sleep(2 * time.Millisecond)
			}, grpc.WithInsecure())
			errChan <- errc
		}()
	}
	wg.Wait()
	count := 0
	for i := 0; i < concurrent; i++ {
		if <-errChan == nil {
			count++
		}
	}
	tests.S(t).ExpectTrue(count <= concurrent)

	// failed case 3: get error when close
	failConn, err = GetGRPCWithExpire(context.TODO(), 1*time.Microsecond, addrf(), grpc.WithInsecure())
	tests.S(t).ExpectTrue(failConn != nil)
	tests.S(t).ExpectNil(err)
	_ = failConn.Close()
	// trigger close again
	ClearCache()
}
