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

package init

import (
	"gitee.com/opengauss/ham4db/go/config"
	cdb "gitee.com/opengauss/ham4db/go/core/db"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/dtstruct"
)

// BackendDBInit attempts to create/upgrade the ham4db backend database.
func BackendDBInit() (err error) {

	// get backend db handler according to type in config
	backendHandler := cdb.GetBackendDBHandler(config.Config.BackendDB)

	//  create database
	if err = backendHandler.CreateDatabase(); err != nil {
		return err
	}

	// check version
	if msg, ok := backendHandler.DeployCheck(); !ok {
		return log.Errorf("version check failed, will not init schema again, detail:%s", msg)
	}

	// init base schema
	backendHandler.SchemaInit(append(GenerateSQLBase(), GenerateSQLPatch()...))

	// init schema for enabled plugin
	for _, dbt := range config.Config.EnableAdaptor {
		dh := dtstruct.HamHandlerMap[dbt]
		backendHandler.SchemaInit(append(dh.SchemaBase(), dh.SchemaPatch()...))
	}

	// update deploy info
	if _, err = cdb.ExecSQL("replace into ham_deployment (deployed_version, deployed_timestamp) values (?, current_timestamp)", dtstruct.RuntimeCLIFlags.ConfiguredVersion); err != nil {
		log.Fatal("unable to write to ham_deployment: %+v", err)
	}

	return nil
}
