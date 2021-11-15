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
package tests

import (
	"testing"
)

// Spec is an access point to test Expections
type Spec struct {
	t *testing.T
}

// S generates a spec. You will want to use it once in a test file, once in a test or once per each check
func S(t *testing.T) *Spec {
	return &Spec{t: t}
}

// ExpectNil expects given value to be nil, or errors
func (spec *Spec) ExpectNil(actual interface{}) {
	if actual == nil {
		return
	}
	spec.t.Errorf("Expected %+v to be nil", actual)
}

// ExpectNotNil expects given value to be not nil, or errors
func (spec *Spec) ExpectNotNil(actual interface{}) {
	if actual != nil {
		return
	}
	spec.t.Errorf("Expected %+v to be not nil", actual)
}

// ExpectEquals expects given values to be equal (comparison via `==`), or errors
func (spec *Spec) ExpectEquals(actual, value interface{}) {
	if actual == value {
		return
	}
	spec.t.Errorf("Expected:\n[[[%+v]]]\n- got:\n[[[%+v]]]", value, actual)
}

// ExpectNotEquals expects given values to be nonequal (comparison via `==`), or errors
func (spec *Spec) ExpectNotEquals(actual, value interface{}) {
	if !(actual == value) {
		return
	}
	spec.t.Errorf("Expected not %+v", value)
}

// ExpectEqualsAny expects given actual to equal (comparison via `==`) at least one of given values, or errors
func (spec *Spec) ExpectEqualsAny(actual interface{}, values ...interface{}) {
	for _, value := range values {
		if actual == value {
			return
		}
	}
	spec.t.Errorf("Expected %+v to equal any of given values", actual)
}

// ExpectNotEqualsAny expects given actual to be nonequal (comparison via `==`)tp any of given values, or errors
func (spec *Spec) ExpectNotEqualsAny(actual interface{}, values ...interface{}) {
	for _, value := range values {
		if actual == value {
			spec.t.Errorf("Expected not %+v", value)
		}
	}
}

// ExpectFalse expects given values to be false, or errors
func (spec *Spec) ExpectFalse(actual interface{}) {
	spec.ExpectEquals(actual, false)
}

// ExpectTrue expects given values to be true, or errors
func (spec *Spec) ExpectTrue(actual interface{}) {
	spec.ExpectEquals(actual, true)
}
