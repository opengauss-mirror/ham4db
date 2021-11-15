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
package util

import (
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/util/tests"
	"testing"
)

func TestCheckPort(t *testing.T) {

	// normal case 1: [1000,65535]
	tests.S(t).ExpectNil(CheckPort("1111"))

	// failed case 1: non-numeric, <1000, >65535
	testd := []string{"abc", "100", "100000"}
	for _, p := range testd {
		tests.S(t).ExpectNotNil(CheckPort(p))
	}
}

func TestCheckIP(t *testing.T) {

	// normal case 1: ipv4
	tests.S(t).ExpectNil(CheckIP("10.10.10.10"))

	// failed case 1:
	testd := []string{"abc", "a.b.c.d", "333.333.333.333", "10.10.10.333", "10.10.10.a"}
	for _, ip := range testd {
		tests.S(t).ExpectNotNil(CheckIP(ip))
	}
}

func TestCheckVersion(t *testing.T) {

	// normal case 1: x.y.z
	tests.S(t).ExpectNil(CheckVersion("3.1.1"))

	// failed case 1: wrong format
	testd := []string{"", "311", "3.1", "3.1.1.1", "3.1.a"}
	for _, version := range testd {
		tests.S(t).ExpectNotNil(CheckVersion(version))
	}
}

func TestIsGETVersion(t *testing.T) {

	// normal case 1: higher
	testd := [][]string{
		{"3.1.0", "2.2.3"},
		{"3.1.1", ""},
	}
	for _, td := range testd {
		err, ok := IsGTEVersion(td[0], td[1])
		tests.S(t).ExpectNil(err)
		tests.S(t).ExpectTrue(ok)
	}

	// normal case 2: lower
	err, ok := IsGTEVersion("2.1.1", "3.1.1")
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectFalse(ok)

	// normal case 3: equal
	err, ok = IsGTEVersion("2.1.1", "2.1.1")
	tests.S(t).ExpectNil(err)
	tests.S(t).ExpectTrue(ok)

	// failed case 1: no version, wrong version format
	testd = [][]string{
		{"", "311"},
		{"3.1", "3.1.1.1"},
		{"3.1.a", "2.1.1"},
	}
	for _, td := range testd {
		err, ok = IsGTEVersion(td[0], td[1])
		tests.S(t).ExpectNotNil(err)
		tests.S(t).ExpectFalse(ok)
	}
}

func TestRandomExpirationSecond(t *testing.T) {
	expiration := 1000
	for i := 0; i < 100; i++ {
		res := RandomExpirationSecond(expiration)
		tests.S(t).ExpectTrue(res.Seconds() >= float64(expiration) && res.Seconds() < float64(expiration+constant.CacheExpireRandomDelta))
	}
}

func TestRegexpMatchPatterns(t *testing.T) {
	type testPatterns struct {
		s        string
		patterns []string
		expected bool
	}
	patterns := []testPatterns{
		{"hostname", []string{}, false},
		{"hostname", []string{"blah"}, false},
		{"hostname", []string{"blah", "blah"}, false},
		{"hostname", []string{"host", "blah"}, true},
		{"hostname", []string{"blah", "host"}, true},
		{"hostname", []string{"ho.tname"}, true},
		{"hostname", []string{"ho.tname2"}, false},
		{"hostname", []string{"ho.*me"}, true},
	}
	for _, p := range patterns {
		if match := RegexpMatchPattern(p.s, p.patterns); match != p.expected {
			t.Errorf("RegexpMatchPattern failed with: %q, %+v, got: %+v, expected: %+v", p.s, p.patterns, match, p.expected)
		}
	}
}

func TestHasString(t *testing.T) {
	elem := "foo"
	a1 := []string{"bar", "foo", "baz"}
	a2 := []string{"bar", "fuu", "baz"}
	tests.S(t).ExpectTrue(HasString(elem, a1))
	tests.S(t).ExpectFalse(HasString(elem, a2))
}

func TestRandomHash(t *testing.T) {
	tests.S(t).ExpectTrue(len(RandomHash()) == 64)
	tests.S(t).ExpectTrue(len(RandomHash32()) == 32)
}
