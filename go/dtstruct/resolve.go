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

package dtstruct

import (
	"fmt"
	"gitee.com/opengauss/ham4db/go/config"
	"time"
)

type HostnameResolve struct {
	Hostname         string
	ResolvedHostname string
}

func (this HostnameResolve) String() string {
	return fmt.Sprintf("%s %s", this.Hostname, this.ResolvedHostname)
}

type HostnameUnresolve struct {
	Hostname           string
	UnresolvedHostname string
}

func (this HostnameUnresolve) String() string {
	return fmt.Sprintf("%s %s", this.Hostname, this.UnresolvedHostname)
}

type HostnameRegistration struct {
	CreatedAt time.Time
	Key       InstanceKey
	Hostname  string
}

func NewHostnameRegistration(instanceKey *InstanceKey, hostname string) *HostnameRegistration {
	return &HostnameRegistration{
		CreatedAt: time.Now(),
		Key:       *instanceKey,
		Hostname:  hostname,
	}
}

func NewHostnameDeregistration(instanceKey *InstanceKey) *HostnameRegistration {
	return &HostnameRegistration{
		CreatedAt: time.Now(),
		Key:       *instanceKey,
		Hostname:  "",
	}
}

func init() {
	if config.Config.ExpiryHostnameResolvesMinutes < 1 {
		config.Config.ExpiryHostnameResolvesMinutes = 1
	}
}
