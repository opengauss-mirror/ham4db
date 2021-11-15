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
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/core/metric"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"runtime"
	"time"
)

// collect write metric
var backendWrite = metric.CreateOrReturnCollection(constant.CollectionBackendWrite)

// use to limit write rate
var writeChan = make(chan interface{}, constant.ConcurrencyBackendDBWrite)

// ExecDBWrite backpressure database write
func ExecDBWrite(f func() error) (err error) {

	// metric for new write
	m := dtstruct.NewMetric()

	// wait to exec
	writeChan <- struct{}{}
	m.WaitLatency = time.Since(m.Timestamp)

	// catch the exec time and error if there is one
	defer func() {
		if r := recover(); r != nil {
			m.Err = log.Errorf("%s", r)
			if _, ok := r.(runtime.Error); ok {
				// TODO should do more here, write to table or alert
				m.Err = log.Errorf("runtime error: %s", r)
			}
			err = m.Err
		}

		// execute time
		m.ExecuteLatency = time.Since(m.Timestamp.Add(m.WaitLatency))
		_ = backendWrite.Append(m)

		<-writeChan
	}()
	return f()
}
