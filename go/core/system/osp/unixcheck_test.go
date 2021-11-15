/*
   Copyright 2017 Simon Mudd, courtesy Booking.com

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
	"testing"
	"time"
)

func TestUserInGroups(t *testing.T) {
	// It is hard to come up with good results that will work on all systems
	// so the tests are limited but should work on most Linux or OSX systems.
	// If you find a case where the tests fail due to user differences please
	// adjust the test cases appropriately.
	testCases := []struct {
		user       string
		powerUsers []string
		expected   bool
	}{
		{"root", []string{"root", "wheel"}, true},                        // normal case 1
		{"", []string{"root", "wheel"}, false},                           // failed case 1: no user
		{"root", []string{}, false},                                      // failed case 2: no group
		{"root" + time.Now().String(), []string{"root", "wheel"}, false}, // failed case 3: user not exist
		{"root", []string{"not_in_this_group"}, false},                   // failed case 4: not in power group
	}
	for _, v := range testCases {
		if got := UserInGroups(v.user, v.powerUsers); got != v.expected {
			t.Errorf("(%q, %+v) failed. got %v, expected %v", v.user, v.powerUsers, got, v.expected)
		}
	}
}
