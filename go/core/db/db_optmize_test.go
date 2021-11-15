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
package db

import (
	"fmt"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/metric"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/tests"
	"sync"
	"testing"
	"time"
)

func TestExecDBWriteFunc(t *testing.T) {

	// normal case 1: simulate write with concurrency 20, no write will be blocked
	count := 0
	wg := sync.WaitGroup{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = ExecDBWrite(
				func() (err error) {
					time.Sleep(25 * time.Millisecond)
					return
				},
			)
		}()
	}
	wg.Wait()
	mbw := metric.CreateOrReturnCollection(constant.CollectionBackendWrite)
	for _, mtrc := range mbw.Metrics() {
		if mtrc.(*dtstruct.Metric).WaitLatency.Milliseconds() > 20 {
			count++
		}
	}
	tests.S(t).ExpectEquals(count, 0)

	// clear old metric
	expire := mbw.GetExpirePeriod()
	mbw.SetExpirePeriod(0)
	go mbw.StartAutoExpiration()
	time.Sleep(2 * time.Second)
	mbw.StopAutoExpiration()

	// reset expire to default
	mbw.SetExpirePeriod(expire)

	// failed case 1: simulate write with concurrency 30, 10 writes will be blocked because max concurrency is 20
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = ExecDBWrite(
				func() (err error) {
					time.Sleep(25 * time.Millisecond)
					return
				},
			)
		}()
	}
	wg.Wait()
	for _, metric := range metric.CreateOrReturnCollection(constant.CollectionBackendWrite).Metrics() {
		if metric.(*dtstruct.Metric).WaitLatency.Milliseconds() > 20 {
			count++
		}
	}
	tests.S(t).ExpectEquals(count, 10)

	// failed case 2: panic
	tests.S(t).ExpectTrue(ExecDBWrite(func() (err error) { panic("simulate panic here") }) != nil)

	// failed case 3: runtime error
	tests.S(t).ExpectTrue(
		ExecDBWrite(func() (err error) {
			func(array []string, idx int) { fmt.Printf(array[idx]) }([]string{""}, 2)
			return
		}) != nil,
	)
}
