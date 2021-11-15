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

import (
	"golang.org/x/time/rate"
	"net/http"
)

// HttpServer now is only to facilitate restful api test, check if agent and database is available
type HttpServer struct {
	Server  *http.Server
	Limiter *rate.Limiter
	Args    *Args
	Process func(*rate.Limiter, *Args, http.ResponseWriter)
}

// ServeHTTP process http request
func (hs HttpServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	hs.Process(hs.Limiter, hs.Args, writer)
}
