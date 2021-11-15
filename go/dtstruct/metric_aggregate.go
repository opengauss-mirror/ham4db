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

import "github.com/montanaflynn/stats"

// MetricAggregate used to aggregate metric
type MetricAggregate struct {
	stats.Float64Data
}

// internal routine to return the average value or 0
func (m MetricAggregate) Mean(values stats.Float64Data) float64 {
	s, err := stats.Mean(values)
	if err != nil {
		return 0
	}
	return s
}

// internal routine to return the requested Percentile value or 0
func (m MetricAggregate) Percentile(values stats.Float64Data, percent float64) float64 {
	s, err := stats.Percentile(values, percent)
	if err != nil {
		return 0
	}
	return s
}

// internal routine to return the maximum value or 0
func (m MetricAggregate) Max(values stats.Float64Data) float64 {
	s, err := stats.Max(values)
	if err != nil {
		return 0
	}
	return s
}

// internal routine to return the minimum value or 9e9
func (m MetricAggregate) Min(values stats.Float64Data) float64 {
	s, err := stats.Min(values)
	if err != nil {
		return 9e9 // a large number (should use something better than this but it's ok for now)
	}
	return s
}

// internal routine to return the Median or 0
func (m MetricAggregate) Median(values stats.Float64Data) float64 {
	s, err := stats.Median(values)
	if err != nil {
		return 0
	}
	return s
}
