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

package metric

import (
	"time"
)

var metricTickCallbackList []func()

// RunAllMetricCallback is called once in the lifetime of the app, after config has been loaded
func RunAllMetricCallback(tick time.Duration) {
	go func() {
		callbackTick := time.Tick(tick)
		for {
			select {
			case <-callbackTick:
				for _, f := range metricTickCallbackList {
					go f()
				}
			}
		}
	}()
}

// OnMetricTick append callback function to tick list, will be called periodically
func OnMetricTick(f func()) {
	metricTickCallbackList = append(metricTickCallbackList, f)
}
