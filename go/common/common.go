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
package common

import (
	"fmt"
	"regexp"
)

var ReplicationNotRunningError = fmt.Errorf("Replication not running")

var (

	// for ip regex
	Ipv4Regexp         = regexp.MustCompile("^([0-9]+)[.]([0-9]+)[.]([0-9]+)[.]([0-9]+)$")
	Ipv4HostPortRegexp = regexp.MustCompile("^([^:]+):([0-9]+)$")
	Ipv4HostRegexp     = regexp.MustCompile("^([^:]+)$")
	Ipv6HostPortRegexp = regexp.MustCompile("^\\[([:0-9a-fA-F]+)\\]:([0-9]+)$") // e.g. [2001:db8:1f70::999:de8:7648:6e8]:3308
	Ipv6HostRegexp     = regexp.MustCompile("^([:0-9a-fA-F]+)$")                // e.g. 2001:db8:1f70::999:de8:7648:6e8

	// for http addr
	HttpAddr = regexp.MustCompile("^(https?)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]")
)
