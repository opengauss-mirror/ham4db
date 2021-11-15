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
	"gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
)

func TestRegex(t *testing.T) {
	tests.S(t).ExpectFalse(HttpAddr.MatchString("10.10.10.10:"))
	tests.S(t).ExpectFalse(HttpAddr.MatchString("http:10.10.10.10:1000"))
	tests.S(t).ExpectFalse(HttpAddr.MatchString("http:/10.10.10.10:1000"))
	tests.S(t).ExpectFalse(HttpAddr.MatchString("http//:10.10.10.10:1000"))
	tests.S(t).ExpectFalse(HttpAddr.MatchString("10.10.10.10"))
	tests.S(t).ExpectFalse(HttpAddr.MatchString("10.10.10.10:1000"))
	tests.S(t).ExpectTrue(HttpAddr.MatchString("http://10.10.10.10:1000"))
	tests.S(t).ExpectTrue(HttpAddr.MatchString("http://10.10.10.10:1000/"))
	tests.S(t).ExpectTrue(HttpAddr.MatchString("http://10.10.10.10:1000/xxx/xxx"))
	tests.S(t).ExpectTrue(HttpAddr.MatchString("https://10.10.10.10:1000/"))
}
