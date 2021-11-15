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
package app

import (
	"fmt"
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/common/constant"
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/ham"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util"
	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/render"
	"net/http"
	"strconv"
)

func RegisterAPIRequest() map[string][]interface{} {
	return map[string][]interface{}{
		// Classic file:pos relocation:
		constant.DBTMysql + "/repoint/:host/:port/:belowHost/:belowPort":      {http.MethodGet, Repoint},
		constant.DBTMysql + "/master-equivalent/:host/:port/:logFile/:logPos": {http.MethodGet, MasterEquivalent},

		// Binlog server relocation:
		constant.DBTMysql + "/regroup-slaves-bls/:host/:port": {http.MethodGet, RegroupReplicasBinlogServers},
		// GTID relocation:
		constant.DBTMysql + "/move-below-gtid/:host/:port/:belowHost/:belowPort":  {http.MethodGet, MoveBelowGTID},
		constant.DBTMysql + "/move-slaves-gtid/:host/:port/:belowHost/:belowPort": {http.MethodGet, MoveReplicasGTID},
		constant.DBTMysql + "/regroup-slaves-gtid/:host/:port":                    {http.MethodGet, RegroupReplicasGTID},

		// Pseudo-GTID relocation:
		constant.DBTMysql + "/regroup-slaves-pgtid/:host/:port": {http.MethodGet, RegroupReplicasPseudoGTID},

		// Replication, general:
		constant.DBTMysql + "/enable-gtid/:host/:port":                {http.MethodGet, EnableGTID},
		constant.DBTMysql + "/disable-gtid/:host/:port":               {http.MethodGet, DisableGTID},
		constant.DBTMysql + "/locate-gtid-errant/:host/:port":         {http.MethodGet, LocateErrantGTID},
		constant.DBTMysql + "/gtid-errant-reset-master/:host/:port":   {http.MethodGet, ErrantGTIDResetMaster},
		constant.DBTMysql + "/gtid-errant-inject-empty/:host/:port":   {http.MethodGet, ErrantGTIDInjectEmpty},
		constant.DBTMysql + "/flush-binary-logs/:host/:port":          {http.MethodGet, FlushBinaryLogs},
		constant.DBTMysql + "/purge-binary-logs/:host/:port/:logFile": {http.MethodGet, PurgeBinaryLogs},
		constant.DBTMysql + "/restart-slave-statements/:host/:port":   {http.MethodGet, RestartReplicationStatements},

		// Replication information:
		constant.DBTMysql + "/can-replicate-from-gtid/:host/:port/:belowHost/:belowPort": {http.MethodGet, CanReplicateFromGTID},

		// Binary logs:
		constant.DBTMysql + "/last-pseudo-gtid/:host/:port":    {http.MethodGet, LastPseudoGTID},
		constant.DBTMysql + "/cluster-osc-slaves/:clusterHint": {http.MethodGet, ClusterOSCReplicas},
	}
}

func RegisterWebRequest() map[string][]interface{} {
	return map[string][]interface{}{}
}

// RegroupReplicasBinlogServers attempts to pick a replica of a given instance and make it take its siblings, efficiently, using GTID
func RegroupReplicasBinlogServers(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	_, promotedBinlogServer, err := ham.RegroupReplicasBinlogServers(&instanceKey, false)

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("promoted binlog server: %s",
		promotedBinlogServer.Key.DisplayString()), Detail: promotedBinlogServer.Key})
}

// RegroupReplicasGTID attempts to pick a replica of a given instance and make it take its siblings, efficiently, using GTID
func RegroupReplicasGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	lostReplicas, movedReplicas, cannotReplicateReplicas, promotedReplica, err := ham.RegroupReplicasGTID(&instanceKey, false, nil, nil, nil)
	lostReplicas = append(lostReplicas, cannotReplicateReplicas...)

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("promoted replica: %s, lost: %d, moved: %d",
		promotedReplica.Key.DisplayString(), len(lostReplicas), len(movedReplicas)), Detail: promotedReplica.Key})
}

// RegroupReplicas attempts to pick a replica of a given instance and make it take its siblings, efficiently,
// using pseudo-gtid if necessary
func RegroupReplicasPseudoGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	lostReplicas, equalReplicas, aheadReplicas, cannotReplicateReplicas, promotedReplica, err := ham.RegroupReplicasPseudoGTID(&instanceKey, false, nil, nil, nil)
	lostReplicas = append(lostReplicas, cannotReplicateReplicas...)

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("promoted replica: %s, lost: %d, trivial: %d, pseudo-gtid: %d",
		promotedReplica.Key.DisplayString(), len(lostReplicas), len(equalReplicas), len(aheadReplicas)), Detail: promotedReplica.Key})
}

// EnableGTID attempts to enable GTID on a replica
func EnableGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := ham.EnableGTID(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Enabled GTID on %+v", instance.Key), Detail: instance})
}

// DisableGTID attempts to disable GTID on a replica, and revert to binlog file:pos
func DisableGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := ham.DisableGTID(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Disabled GTID on %+v", instance.Key), Detail: instance})
}

// LocateErrantGTID identifies the binlog positions for errant GTIDs on an instance
func LocateErrantGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	errantBinlogs, err := ham.LocateErrantGTID(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("located errant GTID"), Detail: errantBinlogs})
}

// ErrantGTIDResetMaster removes errant transactions on a server by way of RESET MASTER
func ErrantGTIDResetMaster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := ham.ErrantGTIDResetMaster(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Removed errant GTID on %+v and issued a RESET MASTER", instance.Key), Detail: instance})
}

// ErrantGTIDInjectEmpty removes errant transactions by injecting and empty transaction on the cluster's master
func ErrantGTIDInjectEmpty(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, clusterMaster, countInjectedTransactions, err := ham.ErrantGTIDInjectEmpty(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Have injected %+v transactions on cluster master %+v", countInjectedTransactions, clusterMaster.Key), Detail: instance})
}

// Repoint positiones a replica under another (or same) master with exact same coordinates.
// Useful for binlog servers
func Repoint(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	belowKey, err := base.GetInstanceKey(constant.DBTMysql, params["belowHost"], params["belowPort"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	instance, err := ham.Repoint(&instanceKey, &belowKey, constant.GTIDHintNeutral)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Instance %+v repointed below %+v", instanceKey, belowKey), Detail: instance})
}

// FlushBinaryLogs runs a single FLUSH BINARY LOGS
func FlushBinaryLogs(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, err := ham.FlushBinaryLogs(&instanceKey, 1)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Binary logs flushed on: %+v", instance.Key), Detail: instance})
}

// PurgeBinaryLogs purges binary logs up to given binlog file
func PurgeBinaryLogs(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	logFile := params["logFile"]
	if logFile == "" {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "purge-binary-logs: expected log file name or 'latest'"})
		return
	}
	force := (req.URL.Query().Get("force") == "true") || (params["force"] == "true")
	var instance dtstruct.InstanceAdaptor
	if logFile == "latest" {
		instance, err = ham.PurgeBinaryLogsToLatest(&instanceKey, force)
	} else {
		instance, err = ham.PurgeBinaryLogsTo(&instanceKey, logFile, force)
	}
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Binary logs flushed on: %+v", instance.GetInstance().Key), Detail: instance})
}

// RestartReplicationStatements receives a query to execute that requires a replication restart to apply.
// As an example, this may be `set global rpl_semi_sync_slave_enabled=1`. ham4db will check
// replication status on given host and will wrap with appropriate stop/start statements, if need be.
func RestartReplicationStatements(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	query := req.URL.Query().Get("q")
	statements, err := ham.GetReplicationRestartPreserveStatements(&instanceKey, query)

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("statements for: %+v", instanceKey), Detail: statements})
}

// CanReplicateFromGTID attempts to move an instance below another via GTID.
func CanReplicateFromGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instance, found, err := ham.ReadFromBackendDB(&instanceKey)
	if (!found) || (err != nil) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", instanceKey)})
		return
	}
	belowKey, err := base.GetInstanceKey(constant.DBTMysql, params["belowHost"], params["belowPort"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	belowInstance, found, err := ham.ReadFromBackendDB(&belowKey)
	if (!found) || (err != nil) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", belowKey)})
		return
	}

	canReplicate, err := ham.CanReplicateFrom(instance, belowInstance)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	if !canReplicate {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("%t", canReplicate), Detail: belowKey})
		return
	}
	err = ham.CheckMoveViaGTID(instance, belowInstance)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	canReplicate = (err == nil)

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("%t", canReplicate), Detail: belowKey})
}

// MoveReplicasGTID attempts to move an instance below another, via GTID
func MoveReplicasGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	belowKey, err := base.GetInstanceKey(constant.DBTMysql, params["belowHost"], params["belowPort"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	movedReplicas, _, err, errs := ham.MoveReplicasGTID(&instanceKey, &belowKey, req.URL.Query().Get("pattern"))
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Moved %d replicas of %+v below %+v via GTID; %d errors: %+v", len(movedReplicas), instanceKey, belowKey, len(errs), errs), Detail: belowKey})
}

// MoveBelowGTID attempts to move an instance below another, via GTID
func MoveBelowGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	belowKey, err := base.GetInstanceKey(constant.DBTMysql, params["belowHost"], params["belowPort"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	instance, err := ham.MoveBelowGTID(&instanceKey, &belowKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Instance %+v moved below %+v via GTID", instanceKey, belowKey), Detail: instance})
}

// LastPseudoGTID attempts to find the last pseugo-gtid entry in an instance
func LastPseudoGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	instance, found, err := ham.ReadFromBackendDB(&instanceKey)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	if instance == nil || !found {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("Instance not found: %+v", instanceKey)})
		return
	}
	coordinates, text, err := ham.FindLastPseudoGTIDEntry(instance, instance.RelaylogCoordinates, nil, false, nil)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("%+v", *coordinates), Detail: text})
}

// MasterEquivalent provides (possibly empty) list of master coordinates equivalent to the given ones
func MasterEquivalent(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !base.IsAuthorizedForAction(req, user) {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := base.GetInstanceKey(constant.DBTMysql, params["host"], params["port"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	coordinates, err := getBinlogCoordinates(params["logFile"], params["logPos"])
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}
	instanceCoordinates := &dtstruct.InstanceBinlogCoordinates{Key: instanceKey, Coordinates: coordinates}

	equivalentCoordinates, err := ham.GetEquivalentMasterCoordinates(instanceCoordinates)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: err.Error()})
		return
	}

	base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.OK, Message: fmt.Sprintf("Found %+v equivalent coordinates", len(equivalentCoordinates)), Detail: equivalentCoordinates})
}

// ClusterOSCReplicas returns heuristic list of OSC replicas
func ClusterOSCReplicas(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := base.FigureClusterNameByHint(util.GetClusterHint(params))
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	// TODO why need database type
	instances, err := ham.GetClusterOSCReplicas(clusterName)
	if err != nil {
		base.Respond(r, &dtstruct.APIResponse{Code: dtstruct.ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}
func getBinlogCoordinates(logFile string, logPos string) (dtstruct.LogCoordinates, error) {
	coordinates := dtstruct.LogCoordinates{LogFile: logFile}
	var err error
	if coordinates.LogPos, err = strconv.ParseInt(logPos, 10, 0); err != nil {
		return coordinates, fmt.Errorf("Invalid logPos: %s", logPos)
	}

	return coordinates, err
}
