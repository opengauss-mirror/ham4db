/*
   Copyright 2017 Simon J Mudd

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

// MetricAggregatedQueue contains aggregate information some part queue metric
type MetricAggregatedQueue struct {
	ActiveMinEntries    float64
	ActiveMeanEntries   float64
	ActiveMedianEntries float64
	ActiveP95Entries    float64
	ActiveMaxEntries    float64
	QueuedMinEntries    float64
	QueuedMeanEntries   float64
	QueuedMedianEntries float64
	QueuedP95Entries    float64
	QueuedMaxEntries    float64
}
