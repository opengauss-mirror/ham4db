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

	// health check
	HealthCheck     = "api/agent/health" // check api from ham4db server
	HealthCheckTick = 5                  // interval(second) of health check

	// refresh
	RefreshTick = 300 // basic info refresh interval(second)

	// http test
	HttpLimit       = 0.2
	HttpLimitBurst  = 1
	HttpReadTimeout = 5 //second

	// for worker set
	WorkerPoolDoneCheckInterval = 2  // interval(second) of done worker run check
	WorkerQuitCheckInterval     = 50 // interval(milli second) of done worker run check
	WorkerDefaultQuantity       = 5  // default number of workers

	// worker name
	WorkerNameDoneChecker    = "DoneChecker" // name of worker to check if all worker done
	WorkerNameKeepAlive      = "KeepAlive"
	WorkerNameGrpcServer     = "GrpcServer"
	WorkerNameStopGrpcServer = "StopGrpcServer"
	WorkerNameHttpServer     = "HttpServer"
	WorkerNameStopHttpServer = "StopHttpServer"
)
