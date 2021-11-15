/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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

package osp

import (
	"gitee.com/opengauss/ham4db/go/core/log"
	"os"
)

// GetHostname get current host name
func GetHostname() string {
	if hostname, err := os.Hostname(); err != nil {
		log.Fatal("cannot resolve self hostname, aborting. %+v", err)
		return ""
	} else {
		return hostname
	}
}
