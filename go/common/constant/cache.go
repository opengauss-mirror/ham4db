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

const (

	// cache config
	CacheAtomPrefix    = "atom-" // atom key prefix
	CacheNotExpire     = -1      //
	CacheExpireByCache = 0       // use default expiration of cache self

	CacheExpireDefault     = 1800 // second
	CacheExpireAtomKey     = 2    // second
	CacheExpireRandomDelta = 1800 // second

	CacheNamePrefix = "cache-"

	// name and expire and clean up for different key
	CacheAgentName            = "agent"
	CacheAgentExpire          = 3600 // second
	CacheAgentCleanupInterval = 1800 // second

	CacheGrpcName            = "grpc"
	CacheGrpcExpire          = 86400 // second
	CacheGrpcCleanupInterval = 1800  // second

	CacheHostnameName            = "hostname"
	CacheHostnameCleanupInterval = 60 // second

	CacheIPName            = "ip"
	CacheIPExpire          = 600 // second
	CacheIPCleanupInterval = 60  // second

	CacheLogName            = "log"
	CacheLogExpire          = 60 // second
	CacheLogCleanupInterval = 5  // second

	CacheInstanceForgetName            = "inst-forget"
	CacheInstanceForgetCleanupInterval = 1 //second

	CacheInstanceAnalysisName            = "inst-analysis"
	CacheInstanceAnalysisCleanupInterval = 1 //second

	CacheClusterName            = "cluster"
	CacheClusterCleanupInterval = 1 //second

	CacheKVFoundName            = "kv-found"
	CacheKVFoundExpire          = 600
	CacheKVFoundCleanupInterval = 60

	CacheOperationKeyName            = "operation-key"
	CacheOperationKeyExpire          = 600
	CacheOperationKeyCleanupInterval = 1
)
