/*
   Copyright 2014 Outbrain Inc.

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
	"errors"
	"fmt"
	"regexp"
	"strings"

	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/log"
)

// Event entries may contains table IDs (can be different for same tables on different servers)
// and also COMMIT transaction IDs (different values on different servers).
// So these need to be removed from the event entry if we're to compare and validate matching
// entries.
var eventInfoTransformations map[*regexp.Regexp]string = map[*regexp.Regexp]string{
	regexp.MustCompile(`(.*) [/][*].*?[*][/](.*$)`):  "$1 $2",         // strip comments
	regexp.MustCompile(`(COMMIT) .*$`):               "$1",            // commit number varies cross servers
	regexp.MustCompile(`(table_id:) [0-9]+ (.*$)`):   "$1 ### $2",     // table ids change cross servers
	regexp.MustCompile(`(table_id:) [0-9]+$`):        "$1 ###",        // table ids change cross servers
	regexp.MustCompile(` X'([0-9a-fA-F]+)' COLLATE`): " 0x$1 COLLATE", // different ways to represent collate
	regexp.MustCompile(`(BEGIN GTID [^ ]+) cid=.*`):  "$1",            // MariaDB GTID someimtes gets addition of "cid=...". Stripping
}

var skippedEventTypes map[string]bool = map[string]bool{
	"Format_desc": true,
	"Stop":        true,
	"Rotate":      true,
}

type BinlogEvent struct {
	Coordinates  LogCoordinates
	NextEventPos int64
	EventType    string
	Info         string
}

//
func (this *BinlogEvent) NextBinlogCoordinates() LogCoordinates {
	return LogCoordinates{LogFile: this.Coordinates.LogFile, LogPos: this.NextEventPos, Type: this.Coordinates.Type}
}

//
func (this *BinlogEvent) NormalizeInfo() {
	for reg, replace := range eventInfoTransformations {
		this.Info = reg.ReplaceAllString(this.Info, replace)
	}
}

func (this *BinlogEvent) Equals(other *BinlogEvent) bool {
	return this.Coordinates.Equals(&other.Coordinates) &&
		this.NextEventPos == other.NextEventPos &&
		this.EventType == other.EventType && this.Info == other.Info
}

func (this *BinlogEvent) EqualsIgnoreCoordinates(other *BinlogEvent) bool {
	return this.NextEventPos == other.NextEventPos &&
		this.EventType == other.EventType && this.Info == other.Info
}

const maxEmptyEventsEvents int = 10

//
type BinlogEventCursor struct {
	CachedEvents      []BinlogEvent
	CurrentEventIndex int
	fetchNextEvents   func(LogCoordinates) ([]BinlogEvent, error)
	nextCoordinates   LogCoordinates
}

// fetchNextEventsFunc expected to return events starting at a given position, and automatically fetch those from next
// binary log when no more rows are found in current log.
// It is expected to return empty array with no error upon end of binlogs
// It is expected to return error upon error...
func NewBinlogEventCursor(startCoordinates LogCoordinates, fetchNextEventsFunc func(LogCoordinates) ([]BinlogEvent, error)) BinlogEventCursor {
	events, _ := fetchNextEventsFunc(startCoordinates)
	var initialNextCoordinates LogCoordinates
	if len(events) > 0 {
		initialNextCoordinates = events[0].NextBinlogCoordinates()
	}
	return BinlogEventCursor{
		CachedEvents:      events,
		CurrentEventIndex: -1,
		fetchNextEvents:   fetchNextEventsFunc,
		nextCoordinates:   initialNextCoordinates,
	}
}

// nextEvent will return the next event entry from binary logs; it will automatically skip to next
// binary log if need be.
// Internally, it uses the cachedEvents array, so that it does not go to the MySQL server upon each call.
// Returns nil upon reaching end of binary logs.
func (this *BinlogEventCursor) nextEvent(numEmptyEventsEvents int) (*BinlogEvent, error) {
	if numEmptyEventsEvents > maxEmptyEventsEvents {
		log.Debugf("End of logs. currentEventIndex: %d, nextCoordinates: %+v", this.CurrentEventIndex, this.nextCoordinates)
		// End of logs
		return nil, nil
	}
	if len(this.CachedEvents) == 0 {
		// Cache exhausted; get next bulk of entries and return the next entry
		nextFileCoordinates, err := this.nextCoordinates.NextFileCoordinates()
		if err != nil {
			return nil, err
		}
		log.Debugf("zero cached events, next file: %+v", nextFileCoordinates)
		this.CachedEvents, err = this.fetchNextEvents(nextFileCoordinates)
		if err != nil {
			return nil, err
		}
		this.CurrentEventIndex = -1
		// While this seems recursive do note that recursion level is at most 1, since we either have
		// entries in the next binlog (no further recursion) or we don't (immediate termination)
		return this.nextEvent(numEmptyEventsEvents + 1)
	}
	if this.CurrentEventIndex+1 < len(this.CachedEvents) {
		// We have enough cache to go by
		this.CurrentEventIndex++
		event := &this.CachedEvents[this.CurrentEventIndex]
		this.nextCoordinates = event.NextBinlogCoordinates()
		return event, nil
	} else {
		// Cache exhausted; get next bulk of entries and return the next entry
		var err error
		this.CachedEvents, err = this.fetchNextEvents(this.CachedEvents[len(this.CachedEvents)-1].NextBinlogCoordinates())
		if err != nil {
			return nil, err
		}
		this.CurrentEventIndex = -1
		// While this seems recursive do note that recursion level is at most 1, since we either have
		// entries in the next binlog (no further recursion) or we don't (immediate termination)
		return this.nextEvent(numEmptyEventsEvents + 1)
	}
}

// NextRealEvent returns the next event from binlog that is not meta/control event (these are start-of-binary-log,
// rotate-binary-log etc.)
func (this *BinlogEventCursor) NextRealEvent(recursionLevel int) (*BinlogEvent, error) {
	if recursionLevel > maxEmptyEventsEvents {
		log.Debugf("End of real events")
		return nil, nil
	}
	event, err := this.nextEvent(0)
	if err != nil {
		return event, err
	}
	if event == nil {
		return event, err
	}

	if _, found := skippedEventTypes[event.EventType]; found {
		// Recursion will not be deep here. A few entries (end-of-binlog followed by start-of-bin-log) are possible,
		// but we really don't expect a huge sequence of those.
		return this.NextRealEvent(recursionLevel + 1)
	}
	for _, skipSubstring := range config.Config.SkipBinlogEventsContaining {
		if strings.Index(event.Info, skipSubstring) >= 0 {
			// Recursion might go deeper here.
			return this.NextRealEvent(recursionLevel + 1)
		}
	}
	event.NormalizeInfo()
	return event, err
}

// NextCoordinates return the binlog coordinates of the next entry as yet unprocessed by the cursor.
// Moreover, when the cursor terminates (consumes last entry), these coordinates indicate what will be the futuristic
// coordinates of the next binlog entry.
// The value of this function is used by match-below to move a replica behind another, after exhausting the shared binlog
// entries of both.
func (this *BinlogEventCursor) GetNextCoordinates() (LogCoordinates, error) {
	if this.nextCoordinates.LogPos == 0 {
		return this.nextCoordinates, errors.New("Next coordinates unfound")
	}
	return this.nextCoordinates, nil
}

// used by GetNextBinlogCoordinatesToMatch to format debug information appropriately
// format the event information in debug output
func FormatEventCleanly(event BinlogEvent, length *int) string {
	return fmt.Sprintf("%+v %+v; %+v", Rpad(event.Coordinates, length), event.EventType, strings.Split(strings.TrimSpace(event.Info), "\n")[0])
}
