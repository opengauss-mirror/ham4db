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
package token

import (
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/test"
	"gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
	"time"
)

func init() {
	config.Config.AccessTokenUseExpirySeconds = 1
	config.Config.AccessTokenExpiryMinutes = 0.001
	test.DBTestInit()
}

func TestToken(t *testing.T) {

	// normal case 1: generate access token
	owner := "test_user"
	publicToken, err := GenerateAccessToken(owner)
	tests.S(t).ExpectNil(err)

	// normal case 2: first acquire
	var secretToken string
	secretToken, err = AcquireAccessToken(publicToken)
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectNotNil(secretToken)

	// normal case 3: check if public token and secret token is valid
	var valid bool
	valid, err = IsTokenValid(publicToken, secretToken)
	tests.S(t).ExpectTrue(valid)
	tests.S(t).ExpectNil(err)

	// normal case 4: expire
	tests.S(t).ExpectNil(ExpireAccessToken())

	// failed case 1: already update acquire data
	_, err = AcquireAccessToken(publicToken)
	tests.S(t).ExpectNotNil(err)

	// failed case 2: create new token, but expire before acquire
	publicToken, err = GenerateAccessToken(owner)
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectNotNil(publicToken)
	time.Sleep(100 * time.Millisecond)
	tests.S(t).ExpectNil(ExpireAccessToken())
	secretToken, err = AcquireAccessToken(publicToken)
	tests.S(t).ExpectNotNil(err)
	tests.S(t).ExpectTrue(secretToken == "")
}
