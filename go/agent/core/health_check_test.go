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
package core

import (
	"gitee.com/opengauss/ham4db/go/util/tests"
	"strconv"
	"testing"
)

func TestKeepAlive(t *testing.T) {

	// normal case
	tests.S(t).ExpectNil(KeepAlive("http://0.0.0.0:"+strconv.Itoa(HttpPort), "10.10.10.10", "test-1", 15000, 10))

	// failed case
	tests.S(t).ExpectNotNil(KeepAlive("http://0.0.0.0:"+strconv.Itoa(HttpPort+1), "10.10.10.10", "test-1", 15001, 10))
	tests.S(t).ExpectNotNil(KeepAlive("", "", "", 0, 0))
	tests.S(t).ExpectNotNil(KeepAlive("0.0.0.0:3000", "10.10.10.10", "test-1", 15002, 10))
	tests.S(t).ExpectNotNil(KeepAlive("http://0.0.0.0:3000", "10.10.10.10", "test-1", 15003, 10))
}
