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
package dtstruct

import (
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/util"
)

// Token is used to identify and validate requests to this service
type Token struct {
	Hash string
}

func (this *Token) Short() string {
	if len(this.Hash) <= constant.TokenShortLength {
		return this.Hash
	}
	return this.Hash[0:constant.TokenShortLength]
}

var ProcessToken = NewToken()

func NewToken() *Token {
	return &Token{
		Hash: util.RandomHash(),
	}
}