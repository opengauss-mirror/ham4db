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
	"fmt"
	"gitee.com/opengauss/ham4db/go/common"

	"strings"
)

// InstanceKey is an instance indicator, identified by hostname and port
type InstanceKey struct {
	DBType    string
	Hostname  string
	Port      int
	ClusterId string
}

const detachHint = "//"

// Equals tests equality between this key and another key
func (ik *InstanceKey) Equals(other *InstanceKey) bool {
	if other == nil {
		return false
	}
	return ik.Hostname == other.Hostname && ik.Port == other.Port
}

// SmallerThan returns true if this key is dictionary-smaller than another.
// This is used for consistent sorting/ordering; there's nothing magical about it.
func (ik *InstanceKey) SmallerThan(other *InstanceKey) bool {
	if ik.Hostname < other.Hostname {
		return true
	}
	if ik.Hostname == other.Hostname && ik.Port < other.Port {
		return true
	}
	return false
}

// IsDetached returns 'true' when this hostname is logically "detached"
func (ik *InstanceKey) IsDetached() bool {
	return strings.HasPrefix(ik.Hostname, detachHint)
}

// IsValid uses simple heuristics to see whether this key represents an actual instance
func (ik *InstanceKey) IsValid() bool {
	if ik.Hostname == "_" {
		return false
	}
	if ik.IsDetached() {
		return false
	}
	return ik.Hostname != "" && ik.Port > 0 && ik.ClusterId != ""
}

// DetachedKey returns an instance key whose hostname is detahced: invalid, but recoverable
func (ik *InstanceKey) DetachedKey() *InstanceKey {
	if ik.IsDetached() {
		return ik
	}
	return &InstanceKey{Hostname: fmt.Sprintf("%s%s", detachHint, ik.Hostname), Port: ik.Port, DBType: ik.DBType, ClusterId: ik.ClusterId}
}

// ReattachedKey returns an instance key whose hostname is detahced: invalid, but recoverable
func (ik *InstanceKey) ReattachedKey() *InstanceKey {
	if !ik.IsDetached() {
		return ik
	}
	return &InstanceKey{Hostname: ik.Hostname[len(detachHint):], Port: ik.Port, DBType: ik.DBType, ClusterId: ik.ClusterId}
}

// StringCode returns an official string representation of this key
func (ik *InstanceKey) StringCode() string {
	return fmt.Sprintf("%s:%s:%s:%d", ik.ClusterId, ik.DBType, ik.Hostname, ik.Port)
}

// DisplayString returns a user-friendly string representation of this key
func (ik *InstanceKey) DisplayString() string {
	return ik.StringCode()
}

// String returns a user-friendly string representation of this key
func (ik InstanceKey) String() string {
	return ik.StringCode()
}

// IsValid uses simple heuristics to see whether this key represents an actual instance
func (ik *InstanceKey) IsIPv4() bool {
	return common.Ipv4Regexp.MatchString(ik.Hostname)
}

type ByNamePort []*InstanceKey

func (bnp ByNamePort) Len() int      { return len(bnp) }
func (bnp ByNamePort) Swap(i, j int) { bnp[i], bnp[j] = bnp[j], bnp[i] }
func (bnp ByNamePort) Less(i, j int) bool {
	return (bnp[i].Hostname < bnp[j].Hostname) || (bnp[i].Hostname == bnp[j].Hostname && bnp[i].Port < bnp[j].Port)
}
