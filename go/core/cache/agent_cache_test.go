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
	"errors"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
	"time"
)

func TestIsExpire(t *testing.T) {

	// normal case 1:
	agt := &dtstruct.Agent{
		LastUpdate: time.Now(),
		Interval:   10,
	}
	tests.S(t).ExpectFalse(IsExpire(agt))

	// failed case 1:
	agt = &dtstruct.Agent{
		LastUpdate: time.Now().Add(-10 * time.Second),
		Interval:   10,
	}
	tests.S(t).ExpectTrue(IsExpire(agt))
}

func TestGetAgent(t *testing.T) {

	hostname := "abc.local"

	var agt *dtstruct.Agent
	var err error

	// normal case 1: agent not exist, get from func
	agt, err = GetAgent(hostname, func() (interface{}, error) {

		// simulate update time
		lastUpdate := time.Now()

		// simulate database query
		time.Sleep(50 * time.Millisecond)

		return &dtstruct.Agent{Hostname: hostname, Interval: 2, LastUpdate: lastUpdate}, nil
	})
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectNotNil(agt)
	tests.S(t).ExpectEquals(agt.Hostname, hostname)

	// normal case 2: agent exist, get from cache
	agt, err = GetAgent(hostname, func() (interface{}, error) {
		return nil, nil
	})
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectNotNil(agt)
	tests.S(t).ExpectEquals(agt.Hostname, hostname)

	// failed case 1: agent exist, but already expire, function return dead agent, get warning
	time.Sleep(2 * time.Second)
	agt, err = GetAgent(hostname, func() (interface{}, error) {
		// simulate dead agent
		return &dtstruct.Agent{Hostname: hostname, Interval: 1, LastUpdate: time.Now().Add(-10 * time.Second)}, nil
	})
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(agt == nil)

	// failed case 2: agent exist, but already expire, get error when from function
	time.Sleep(1 * time.Second)
	agt, err = GetAgent(hostname, func() (interface{}, error) {
		return nil, errors.New("exception happened")
	})
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(agt == nil)
}
