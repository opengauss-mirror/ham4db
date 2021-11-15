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
package http

import (
	"gitee.com/opengauss/ham4db/go/core/log"
	"github.com/go-martini/martini"
	"net/http"
	"time"
)

const TimeFormat = "2006-01-02 15:04:05"

// CustomMartini
func CustomMartini() *martini.ClassicMartini {
	mt := martini.New()
	mt.Use(martini.Recovery())
	mt.Use(martini.Static("public"))
	mt.Use(func(res http.ResponseWriter, req *http.Request, c martini.Context) {
		start := time.Now()
		addr := req.Header.Get("X-Real-IP")
		if addr == "" {
			addr = req.Header.Get("X-Forwarded-For")
			if addr == "" {
				addr = req.RemoteAddr
			}
		}
		log.Infof("[martini] Started %s %s for %s", req.Method, req.URL.Path, addr)
		rw := res.(martini.ResponseWriter)
		c.Next()
		log.Infof("[martini] Completed %v %s in %v", rw.Status(), http.StatusText(rw.Status()), time.Since(start))
	})

	r := martini.NewRouter()
	mt.MapTo(r, (*martini.Routes)(nil))
	mt.Action(r.Handle)

	return &martini.ClassicMartini{Martini: mt, Router: r}
}
