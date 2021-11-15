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

package http

import (
	"gitee.com/opengauss/ham4db/go/core/metric"
	"gitee.com/opengauss/ham4db/go/core/security/ssl"

	//"gitee.com/opengauss/ham4db/go/core/security/ssl"
	"gitee.com/opengauss/ham4db/go/core/system/osp"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"net"
	"net/http"
	"time"

	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/ha/process"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/core/logic"
	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/gzip"
	"github.com/martini-contrib/render"
)

const discoveryMetricsName = "DISCOVERY_METRICS"

var sslPEMPassword []byte

//var discoveryMetrics *collection.MetricList

// Http starts serving
func Http(continuousDiscovery bool) {
	promptForSSLPasswords()
	process.ContinuousRegistration(process.ExecutionHttpMode, "")

	martini.Env = martini.Prod
	standardHttp(continuousDiscovery)
}

// Iterate over the private keys and get passwords for them
// Don't prompt for a password a second time if the files are the same
func promptForSSLPasswords() {
	if ssl.IsEncryptedPEM(config.Config.SSLPrivateKeyFile) {
		sslPEMPassword = ssl.GetPEMPassword(config.Config.SSLPrivateKeyFile)
	}
}

// standardHttp starts serving HTTP or HTTPS (api/web) requests, to be used by normal clients
func standardHttp(continuousDiscovery bool) {
	m := CustomMartini()
	m.Map(auth.User(""))
	m.Use(gzip.All())
	m.Use(render.Renderer(render.Options{
		Directory:       "resources",
		Layout:          "templates/layout",
		HTMLContentType: "text/html",
	}))
	m.Use(martini.Static("resources/public", martini.StaticOptions{Prefix: config.Config.URLPrefix, SkipLogging: true}))
	if config.Config.UseMutualTLS {
		m.Use(VerifyOUs(config.Config.SSLValidOUs))
	}

	dtstruct.SetMaintenanceOwner(osp.GetHostname())

	if continuousDiscovery {
		// start to expire metric collection info
		discoveryMetrics = metric.CreateOrReturnCollection(discoveryMetricsName)
		discoveryMetrics.SetExpirePeriod(time.Duration(config.Config.DiscoveryCollectionRetentionSeconds) * time.Second)

		log.Info("Starting Discovery")
		go logic.ContinuousDiscovery()
	}

	log.Info("Registering endpoints")
	API.URLPrefix = config.Config.URLPrefix
	Web.URLPrefix = config.Config.URLPrefix
	RegisterRequests(&API, m)
	Web.RegisterRequests(m)

	// register all enabled adaptor api
	for _, adp := range config.Config.EnableAdaptor {
		for url, api := range dtstruct.GetHamHandler(adp).RegisterAPIRequest() {
			RegisterAPIRequest(api[0].(string), &API, m, false, url, api[1])
		}
		for url, api := range dtstruct.GetHamHandler(adp).RegisterWebRequest() {
			RegisterAPIRequest(api[0].(string), &API, m, false, url, api[1])
		}
	}

	// Serve
	if config.Config.ListenSocket != "" {
		log.Infof("Starting HTTP listener on unix socket %v", config.Config.ListenSocket)
		unixListener, err := net.Listen("unix", config.Config.ListenSocket)
		if err != nil {
			log.Fatale(err)
		}
		defer unixListener.Close()
		if err := http.Serve(unixListener, m); err != nil {
			log.Fatale(err)
		}
	} else if config.Config.UseSSL {
		log.Info("Starting HTTPS listener")
		tlsConfig, err := ssl.NewTLSConfig(config.Config.SSLCAFile, config.Config.UseMutualTLS)
		if err != nil {
			log.Fatale(err)
		}
		tlsConfig.InsecureSkipVerify = config.Config.SSLSkipVerify
		if err = ssl.AppendKeyPairWithPassword(tlsConfig, config.Config.SSLCertFile, config.Config.SSLPrivateKeyFile, sslPEMPassword); err != nil {
			log.Fatale(err)
		}
		if err = ssl.ListenAndServeTLS(config.Config.ListenAddress, m, tlsConfig); err != nil {
			log.Fatale(err)
		}
	} else {
		log.Infof("Starting HTTP listener on %+v", config.Config.ListenAddress)
		if err := http.ListenAndServe(config.Config.ListenAddress, m); err != nil {
			log.Fatale(err)
		}
	}
}
