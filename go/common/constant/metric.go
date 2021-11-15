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
package constant

type CounterKey string
type HostKey string
type TimerKey string

const (
	// collection
	CollectionDefaultExpire = 1               // second
	CollectionWriteBuffer   = "write.buffer"  //
	CollectionBackendWrite  = "backend.write" // name for backend write

	// metric
	MetricAuditOpt                   = "audit.write" //
	MetricInstanceRead               = "instance.read"
	MetricInstanceWrite              = "instance.write"
	MetricAnalysisChangeWriteAttempt = "analysis.change.write.attempt"
	MetricAnalysisChangeWrite        = "analysis.change.write"

	MetricDiscoverAttempt                  = "discoveries.attempt"
	MetricDiscoverFail                     = "discoveries.fail"
	MetricDiscoverInstancePollSecondExceed = "discoveries.instance_poll_seconds_exceeded"
	MetricDiscoverQueueLength              = "discoveries.queue_length"
	MetricDiscoverRecentCount              = "discoveries.recent_count"
	MetricElectIsElected                   = "elect.is_elected"
	MetricHealthIsHealthy                  = "health.is_healthy"
	MetricRaftIsHealthy                    = "raft.is_healthy"
	MetricRaftIsLeader                     = "raft.is_leader"

	// for aggregated discovery metric
	FailedDiscoveries     CounterKey = "FailedDiscoveries"
	Discoveries                      = "Discoveries"
	InstanceKeys          HostKey    = "InstanceKeys"
	OkInstanceKeys                   = "OkInstanceKeys"
	FailedInstanceKeys               = "FailedInstanceKeys"
	TotalSeconds          TimerKey   = "TotalSeconds"
	BackendSeconds                   = "BackendSeconds"
	InstanceSeconds                  = "InstanceSeconds"
	FailedTotalSeconds               = "FailedTotalSeconds"
	FailedBackendSeconds             = "FailedBackendSeconds"
	FailedInstanceSeconds            = "FailedInstanceSeconds"

	// monitor type
	MonitorGraphite = "Graphite"
)
