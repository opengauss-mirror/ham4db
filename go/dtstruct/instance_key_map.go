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
	"encoding/json"
	"gitee.com/opengauss/ham4db/go/core/log"
	"sort"
	"strings"
)

// InstanceKeyMap is a convenience struct for listing InstanceKey-s
type InstanceKeyMap map[InstanceKey]bool

func NewInstanceKeyMap() *InstanceKeyMap {
	return &InstanceKeyMap{}
}

// AddKey adds a single key to this map
func (ikm *InstanceKeyMap) AddKey(key InstanceKey) {
	(*ikm)[key] = true
}

// AddKeys adds all given keys to this map
func (ikm *InstanceKeyMap) AddKeys(keys []InstanceKey) {
	for _, key := range keys {
		ikm.AddKey(key)
	}
}

// HasKey checks if given key is within the map
func (ikm *InstanceKeyMap) HasKey(key InstanceKey) bool {
	_, ok := (*ikm)[key]
	return ok
}

// GetInstanceKeys returns keys in this map in the form of an array
func (ikm *InstanceKeyMap) GetInstanceKeys() []InstanceKey {
	var res []InstanceKey
	for key := range *ikm {
		res = append(res, key)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Hostname < res[j].Hostname || res[i].Hostname == res[j].Hostname && res[i].Port < res[j].Port
	})
	return res
}

// Intersect returns a keymap which is the intersection of this and another map
func (ikm *InstanceKeyMap) Intersect(other *InstanceKeyMap) *InstanceKeyMap {
	intersected := NewInstanceKeyMap()
	for key := range *other {
		if ikm.HasKey(key) {
			intersected.AddKey(key)
		}
	}
	return intersected
}

// MarshalJSON will marshal this map as JSON
func (ikm InstanceKeyMap) MarshalJSON() ([]byte, error) {
	return json.Marshal(ikm.GetInstanceKeys())
}

// UnmarshalJSON reds this object from JSON
func (ikm *InstanceKeyMap) UnmarshalJSON(b []byte) error {
	var keys []InstanceKey
	if err := json.Unmarshal(b, &keys); err != nil {
		return err
	}
	*ikm = make(InstanceKeyMap)
	for _, key := range keys {
		ikm.AddKey(key)
	}
	return nil
}

// ToJSON will marshal this map as JSON
func (ikm *InstanceKeyMap) ToJSON() (string, error) {
	bytes, err := ikm.MarshalJSON()
	return string(bytes), err
}

// ToJSONString will marshal this map as JSON
func (ikm *InstanceKeyMap) ToJSONString() string {
	s, _ := ikm.ToJSON()
	return s
}

// ToCommaDelimitedList will export this map in comma delimited format
func (ikm *InstanceKeyMap) ToCommaDelimitedList() string {
	var keyDisplays []string
	for key := range *ikm {
		keyDisplays = append(keyDisplays, key.DisplayString())
	}
	return strings.Join(keyDisplays, ",")
}

// ReadJson unmarshalls a json into this map
func (ikm *InstanceKeyMap) ReadJson(jsonStr string) error {
	var keys []InstanceKey
	if err := json.Unmarshal([]byte(jsonStr), &keys); err != nil {
		return log.Errorf("instance key json unmarshal error:%s", err)
	}
	ikm.AddKeys(keys)
	return nil
}
