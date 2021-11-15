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
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"gitee.com/opengauss/ham4db/go/core/log"

	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/consensus/raft"
	"github.com/go-martini/martini"
)

func raftReverseProxy(w http.ResponseWriter, r *http.Request, c martini.Context) {
	if !orcraft.IsRaftEnabled() {
		// No raft, so no reverse proxy to the leader
		return
	}
	if orcraft.IsLeader() {
		// I am the leader. I will handle the request directly.
		return
	}
	if orcraft.GetLeader() == "" {
		return
	}
	if orcraft.LeaderURI.IsThisLeaderURI() {
		// Although I'm not the leader, the value I see for LeaderURI is my own.
		// I'm probably not up-to-date with my raft transaction log and don't have the latest information.
		// But anyway, obviously not going to redirect to myself.
		// Gonna return: this isn't ideal, because I'm not really the leader. If the user tries to
		// run an operation they'll fail.
		return
	}
	url, err := url.Parse(orcraft.LeaderURI.Get())
	if err != nil {
		log.Errore(err)
		return
	}
	r.Header.Del("Accept-Encoding")
	switch strings.ToLower(config.Config.AuthenticationMethod) {
	case "basic", "multi":
		r.SetBasicAuth(config.Config.HTTPAuthUser, config.Config.HTTPAuthPassword)
	}
	proxy := httputil.NewSingleHostReverseProxy(url)
	proxy.ServeHTTP(w, r)
}
