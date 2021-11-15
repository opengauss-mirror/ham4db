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

package dtstruct

import (
	"time"
)

// PoolInstancesMap lists instance keys per pool name
type PoolInstancesMap map[string][]*InstanceKey

type PoolInstancesSubmission struct {
	CreatedAt          time.Time
	DatabaseType       string
	Pool               string
	DelimitedInstances string
	RegisteredAt       string
}

func NewPoolInstancesSubmission(databaseType string, pool string, instances string) *PoolInstancesSubmission {
	return &PoolInstancesSubmission{
		CreatedAt:          time.Now(),
		DatabaseType:       databaseType,
		Pool:               pool,
		DelimitedInstances: instances,
	}
}

// ClusterPoolInstance is an instance mapping a cluster, pool & instance
type ClusterPoolInstance struct {
	ClusterName  string
	ClusterAlias string
	Pool         string
	Hostname     string
	Port         int
}
