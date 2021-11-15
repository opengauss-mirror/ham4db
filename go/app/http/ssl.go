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
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/util"
	"github.com/go-martini/martini"
	nethttp "net/http"
	"strings"
)

// TODO: make this testable?
func VerifyOUs(validOUs []string) martini.Handler {
	return func(res nethttp.ResponseWriter, req *nethttp.Request, c martini.Context) {
		log.Debug("Verifying client OU")
		if err := Verify(req, validOUs); err != nil {
			nethttp.Error(res, err.Error(), nethttp.StatusUnauthorized)
		}
	}
}

// Verify that the OU of the presented client certificate matches the list
// of Valid OUs
func Verify(r *nethttp.Request, validOUs []string) error {
	if strings.Contains(r.URL.String(), config.Config.StatusEndpoint) && !config.Config.StatusOUVerify {
		return nil
	}
	if r.TLS == nil {
		return log.Errorf("No TLS")
	}
	for _, chain := range r.TLS.VerifiedChains {
		s := chain[0].Subject.OrganizationalUnit
		log.Debug("All OUs:", strings.Join(s, " "))
		for _, ou := range s {
			log.Debug("Client presented OU:", ou)
			if util.HasString(ou, validOUs) {
				log.Debug("Found valid OU:", ou)
				return nil
			}
		}
	}
	log.Errorf("No valid OUs found")
	return log.Errorf("Invalid OU")
}
