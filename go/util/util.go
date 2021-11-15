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
	"fmt"
	"gitee.com/opengauss/ham4db/go/common/constant"
	"gitee.com/opengauss/ham4db/go/core/log"
	"math/rand"
	"net"
	"reflect"
	"strconv"
	"time"

	"regexp"
	"strings"
)

// CheckPort check if port is between 1000 and 65535
func CheckPort(port string) (err error) {
	p := 0
	if p, err = strconv.Atoi(port); err != nil {
		return log.Errore(err)
	}
	if p < 1000 || p > 65535 {
		return log.Errorf("illegal port: %s, should be between 1000 and 65535", port)
	}
	return nil
}

// CheckIP check if ip is valid
func CheckIP(ip string) error {
	if net.ParseIP(ip) == nil {
		return log.Errorf("invalid ip: %s", ip)
	}
	return nil
}

// CheckVersion check if version is with format x.y.z
func CheckVersion(version string) error {
	vs := strings.Split(version, ".")
	if len(vs) != 3 {
		return log.Errorf("invalid version: %s", version)
	}
	for _, vf := range vs {
		if _, err := strconv.Atoi(vf); err != nil {
			return log.Errore(err)
		}
	}
	return nil
}

// IsGTEVersion check if version v1 is gte v2
func IsGTEVersion(v1 string, v2 string) (error, bool) {
	if v2 == "" {
		return nil, true
	}
	if CheckVersion(v1) != nil || CheckVersion(v2) != nil {
		return log.Errorf("failed when check version"), false
	}
	hs := strings.Split(v1, ".")
	ls := strings.Split(v2, ".")
	for i := range hs {
		high, equal, err := IntCompare(hs[i], ls[i])
		if err != nil {
			return nil, false
		}
		if high {
			return nil, true
		}
		if equal {
			continue
		}
		return nil, false
	}
	return nil, true
}

// IntCompare check if v1 is higher than v2
func IntCompare(v1 string, v2 string) (bool, bool, error) {
	var err error
	iv1 := 0
	iv2 := 0
	if iv1, err = strconv.Atoi(v1); err != nil {
		return false, false, log.Errore(err)
	}
	if iv2, err = strconv.Atoi(v2); err != nil {
		return false, false, log.Errore(err)
	}
	return iv1 > iv2, iv1 == iv2, nil
}

// RandomExpirationSecond random expiration
func RandomExpirationSecond(expiration int) time.Duration {
	return time.Duration(rand.New(rand.NewSource(time.Now().UnixNano())).Intn(constant.CacheExpireRandomDelta)+expiration) * time.Second
}

// RandomHash get random string with 64 length
func RandomHash() string {
	return Random(64)
}

// RandomHash get random string with 32 length
func RandomHash32() string {
	return Random(32)
}

// Random generate random string with specified length
func Random(length int) string {

	// get current time
	now := time.Now().UnixNano()

	// random for this time
	rnd := rand.New(rand.NewSource(now))

	// use time as header, 19 byte
	var result []byte
	if now > 0 {
		result = append(result, []byte(strconv.FormatUint(uint64(time.Now().UnixNano()), 10))...)
	} else {
		log.Error("wrong time:%d, %s", now, time.Now())
		result = append(result, []byte("0000000000000000000")...)
	}

	// random char for left
	left := length - len(result)
	for i := 0; i < left; i++ {
		if i%5 == 0 {
			result = append(result, '-')
			continue
		}
		result = append(result, constant.RandomChars[rnd.Intn(len(constant.RandomChars))])
	}
	return string(result)
}

// RandomPort get random between start and end,[start,end)
func RandomPort(start int, end int) int {
	return rand.New(rand.NewSource(time.Now().UnixNano())).Intn(end-start) + start
}

// GetHint get query hint
func GetHint(params map[string]string) string {
	hint := ""
	if params["hint"] != "" {
		hint = params["hint"]
	}
	if params["clusterId"] != "" && params["host"] != "" && params["port"] != "" {
		hint = fmt.Sprintf("%s:%s:%s", params["clusterId"], params["host"], params["port"])
	}
	return fmt.Sprintf("%s:%s", params["type"], hint)
}

func GetClusterHint(params map[string]string) string {
	hint := ""
	if params["clusterHint"] != "" {
		hint = params["clusterHint"]
	}
	if params["clusterName"] != "" {
		hint = params["clusterName"]
	}
	if params["host"] != "" && params["port"] != "" {
		hint = fmt.Sprintf("%s:%s", params["host"], params["port"])
	}
	return fmt.Sprintf("%s:%s", params["type"], hint)
}

// RegexpMatchPattern returns true if s matches any of the provided regexpPatterns
func RegexpMatchPattern(s string, regexpPatterns []string) bool {
	for _, filter := range regexpPatterns {
		if matched, err := regexp.MatchString(filter, s); err == nil && matched {
			return true
		}
	}
	return false
}

// HasString determine if a string element is in a string array
func HasString(elem string, arr []string) bool {
	for _, s := range arr {
		if s == elem {
			return true
		}
	}
	return false
}

// SemicolonTerminated is a utility function that makes sure a statement is terminated with
// a semicolon, if it isn't already
func SemicolonTerminated(statement string) string {
	statement = strings.TrimSpace(statement)
	statement = strings.TrimRight(statement, ";")
	statement = statement + ";"
	return statement
}

// IsNil check if value is nil or not
func IsNil(i interface{}) bool {
	vi := reflect.ValueOf(i)
	if vi.Kind() == reflect.Ptr {
		return vi.IsNil()
	}
	return false
}
