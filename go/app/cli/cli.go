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

package cli

import (
	"context"
	"fmt"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/core/logic"
	"gitee.com/opengauss/ham4db/go/core/recovery"
	"gitee.com/opengauss/ham4db/go/core/replication"
	"gitee.com/opengauss/ham4db/go/core/topology"
	"gitee.com/opengauss/ham4db/go/core/util"
	tutil "gitee.com/opengauss/ham4db/go/util/text"
	"time"

	"net"
	"sort"

	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/ha/process"
	"gitee.com/opengauss/ham4db/go/core/kv"
	"os/user"

	"gitee.com/opengauss/ham4db/go/config"
	instance2 "gitee.com/opengauss/ham4db/go/core/instance"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"os"
	"regexp"
	"strings"
)

var thisInstanceKey *dtstruct.InstanceKey
var commandMap map[string]dtstruct.CommandDesc

func init() {
	commandMap = make(map[string]dtstruct.CommandDesc)
}

// CliWrapper is called from main and allows for the instance parameter
// to take multiple instance names separated by a comma or whitespace.
func CliWrapper(section string, command string, strict bool, databaseType string, clusterId string, instances string, destination string, owner string, reason string, duration string, pattern string, clusterAlias string, pool string, hostnameFlag string) {
	if config.Config.RaftEnabled && !*dtstruct.RuntimeCLIFlags.IgnoreRaftSetup {
		log.Fatalf(`configured to run raft ("RaftEnabled": true). All access must go through the web API of the active raft node. You may use the ham4db-client script which has a similar interface to the command line invocation. You may override this with --ignore-raft-setup`)
	}
	r := regexp.MustCompile(`[ ,\r\n\t]+`)
	tokens := r.Split(instances, -1)
	switch command {
	case "submit-pool-instances":
		{
			// These commands unsplit the tokens (they expect a comma delimited list of instances)
			tokens = []string{instances}
		}
	}
	for _, instance := range tokens {
		if instance != "" || len(tokens) == 1 {
			Cli(section, command, strict, databaseType, clusterId, instance, destination, owner, reason, duration, pattern, clusterAlias, pool, hostnameFlag)
		}
	}
}

// Cli initiates a command line interface, executing requested command.
func Cli(section string, command string, strict bool, databaseType string, clusterId string, instance string, destination string, owner string, reason string, duration string, pattern string, clusterAlias string, pool string, hostnameFlag string) {

	skipDatabaseCommands := false
	switch command {
	case "meta-redeploy-internal-db":
		skipDatabaseCommands = true
	case "help":
		skipDatabaseCommands = true
	case "dump-config":
		skipDatabaseCommands = true
	}

	cliParam := &dtstruct.CliParam{}
	if command != "help" {
		instanceKey, err := base.ParseResolveInstanceKey(databaseType, instance)
		if err != nil {
			instanceKey = nil
		}
		if instanceKey != nil {
			instanceKey.ClusterId = clusterId
		}
		rawInstanceKey, err := base.ParseRawInstanceKey(databaseType, instance)
		if err != nil {
			rawInstanceKey = nil
		}
		if rawInstanceKey != nil {
			rawInstanceKey.ClusterId = clusterId
		}
		if destination != "" && !strings.Contains(destination, ":") {
			destination = fmt.Sprintf("%s:%d", destination, config.Config.DefaultInstancePort)
		}
		destinationKey, err := base.ParseResolveInstanceKey(databaseType, destination)
		if err != nil {
			destinationKey = nil
		}
		if !skipDatabaseCommands {
			destinationKey = base.ReadFuzzyInstanceKeyIfPossible(destinationKey)
		}
		if destinationKey != nil {
			destinationKey.ClusterId = clusterId
		}
		if hostname, err := os.Hostname(); err == nil {
			// TODO miss cluster id
			thisInstanceKey = &dtstruct.InstanceKey{Hostname: hostname, Port: config.Config.DefaultInstancePort, DBType: databaseType, ClusterId: clusterId}
		}
		postponedFunctionsContainer := dtstruct.NewPostponedFunctionsContainer()

		if len(owner) == 0 {
			// get osp username as owner
			usr, err := user.Current()
			if err != nil {
				log.Fatale(err)
			}
			owner = usr.Username
		}
		dtstruct.SetMaintenanceOwner(owner)

		if !skipDatabaseCommands && !*dtstruct.RuntimeCLIFlags.SkipContinuousRegistration {
			process.ContinuousRegistration(string(process.ExecutionCliMode), command)
		}
		kv.InitKVStores()

		// make cli command param
		cliParam = &dtstruct.CliParam{
			Command:                     command,
			Strict:                      strict,
			DatabaseType:                databaseType,
			ClusterId:                   clusterId,
			Instance:                    instance,
			Destination:                 destination,
			Owner:                       owner,
			Reason:                      reason,
			Duration:                    duration,
			Pattern:                     pattern,
			ClusterAlias:                clusterAlias,
			Pool:                        pool,
			HostnameFlag:                hostnameFlag,
			RawInstanceKey:              rawInstanceKey,
			InstanceKey:                 instanceKey,
			DestinationKey:              destinationKey,
			PostponedFunctionsContainer: postponedFunctionsContainer,
		}
	}

	// if database type is exist, get cli command for this type
	if section != "" {
		dtstruct.GetHamHandler(section).CliCmd(commandMap, cliParam)
	} else {
		CliCmd(commandMap, cliParam)
	}

	switch command {
	case "help":
		fmt.Fprintf(os.Stderr, util.CommandUsage(commandMap))
	default:
		if cmd, ok := commandMap[command]; ok {
			cmd.Func(cliParam)
		} else {
			log.Fatalf("Unknown command: \"%s\". %s", command, util.CommandUsage(commandMap))
		}
	}
}

// register cli command
func CliCmd(commandMap map[string]dtstruct.CommandDesc, cliParam *dtstruct.CliParam) {
	keyValueCmd()
	metaCmd()
	managementCmd()
	infoCmd()
	globalCmd()
	topologyCmd()
	replicationCmd()
	recoveryCmd()
	tagCmd()
	instanceMgtCmd()
}

// kvCmd
func keyValueCmd() {
	util.RegisterCliCommand(commandMap, "kv-submit-master-to-kv-store", "key-value", `submit master of a specific cluster, or all masters of all clusters to key-value store`,
		func(cliParam *dtstruct.CliParam) {
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			kvPairs, _, err := logic.SubmitMastersToKvStores(clusterName, true)
			if err != nil {
				log.Fatale(err)
			}
			for _, kvPair := range kvPairs {
				fmt.Println(fmt.Sprintf("%s:%s", kvPair.Key, kvPair.Value))
			}
		},
	)
}

// metaCmd
func metaCmd() {
	util.RegisterCliCommand(commandMap, "meta-hostname-resolve", "meta", `resolve given hostname`,
		func(cliParam *dtstruct.CliParam) {
			if cliParam.RawInstanceKey == nil {
				log.Fatal("Cannot deduce instance:", cliParam.Instance)
			}
			if conn, err := net.Dial("tcp", cliParam.RawInstanceKey.DisplayString()); err == nil {
				log.Debugf("tcp test is good; got connection %+v", conn)
				conn.Close()
			} else {
				log.Fatale(err)
			}
			if cname, err := base.GetCNAME(cliParam.RawInstanceKey.Hostname); err == nil {
				log.Debugf("GetCNAME() %+v, %+v", cname, err)
				cliParam.RawInstanceKey.Hostname = cname
				fmt.Println(cliParam.RawInstanceKey.DisplayString())
			} else {
				log.Fatale(err)
			}
		},
	)
	util.RegisterCliCommand(commandMap, "meta-redeploy-internal-db", "meta", `force internal schema migration to current backend structure`,
		func(cliParam *dtstruct.CliParam) {
			dtstruct.RuntimeCLIFlags.ConfiguredVersion = ""
			_, err := base.ReadClusters(cliParam.DatabaseType)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println("Redeployed internal db")
		},
	)
}

// managementCmd
func managementCmd() {
	util.RegisterCliCommand(commandMap, "mgt-instance-forget", "management", `forget about an instance's existence`,
		func(cliParam *dtstruct.CliParam) {
			if cliParam.RawInstanceKey == nil {
				log.Fatal("Cannot deduce instance:", cliParam.Instance)
			}
			instanceKey, _ := base.FigureInstanceKey(cliParam.RawInstanceKey, nil)
			err := instance2.ForgetInstance(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		},
	)
	util.RegisterCliCommand(commandMap, "mgt-instance-set-heuristic-domain", "management", `associate domain name of given cluster with what seems to be the writer master for that cluster`,
		func(cliParam *dtstruct.CliParam) {
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			instanceKey, err := instance2.HeuristicallyApplyClusterDomainInstanceAttribute(cliParam.DatabaseType, clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		},
	)
	util.RegisterCliCommand(commandMap, "mgt-continuous", "management", `enter continuous mode, and actively poll for instance, diagnose problems, do maintenance`,
		func(cliParam *dtstruct.CliParam) {
			logic.ContinuousDiscovery()
		},
	)
}

// infoCmd
func infoCmd() {
	util.RegisterCliCommand(commandMap, "instance-identify", "information", `output the fully-qualified hostname:port representation of the given instance, or error if unknown`,
		func(cliParam *dtstruct.CliParam) {
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unable to get master: unresolved instance")
			}
			instance := instance2.IsInstanceExistInBackendDB(instanceKey)
			fmt.Println(instance.GetInstance().Key.DisplayString())
		})
	util.RegisterCliCommand(commandMap, "instance-status", "information", `output short status on a given instance`,
		func(cliParam *dtstruct.CliParam) {
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unable to get status: unresolved instance")
			}
			instance := instance2.IsInstanceExistInBackendDB(instanceKey)
			fmt.Println(instance.HumanReadableDescription())
		})
	util.RegisterCliCommand(commandMap, "instance-cluster", "information", `output the name of the cluster an instance belongs to, or error if unknown`,
		func(cliParam *dtstruct.CliParam) {
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			fmt.Println(clusterName)
		})
	util.RegisterCliCommand(commandMap, "instance-master-identify", "information", `output the fully-qualified hostname:port representation of a given instance's master`,
		func(cliParam *dtstruct.CliParam) {
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unable to get master: unresolved instance")
			}
			instance := instance2.IsInstanceExistInBackendDB(instanceKey)
			if instance.GetInstance().UpstreamKey.IsValid() {
				fmt.Println(instance.GetInstance().UpstreamKey.DisplayString())
			}
		})
	util.RegisterCliCommand(commandMap, "instance-cluster-domain", "information", `output the domain name of the cluster an instance belongs to, or error if unknown`,
		func(cliParam *dtstruct.CliParam) {
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			clusterInfo, err := base.ReadClusterInfo("", clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(clusterInfo.ClusterDomain)
		})
	util.RegisterCliCommand(commandMap, "instance-cluster-alias", "information", `output the alias of the cluster an instance belongs to, or error if unknown`,
		func(cliParam *dtstruct.CliParam) {
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			clusterInfo, err := base.ReadClusterInfo("", clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(clusterInfo.ClusterAlias)
		})

	util.RegisterCliCommand(commandMap, "cluster-master", "information", `output the name of the master in a given cluster`,
		func(cliParam *dtstruct.CliParam) {
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			masters, err := instance2.ReadClusterMaster(cliParam.DatabaseType, clusterName)
			if err != nil {
				log.Fatale(err)
			}
			if len(masters) == 0 {
				log.Fatalf("No writeable masters found for cluster %+v", clusterName)
			}
			fmt.Println(masters[0].GetInstance().Key.DisplayString())
		})

	util.RegisterCliCommand(commandMap, "list-instance", "information", `the complete list of known instance`,
		func(cliParam *dtstruct.CliParam) {
			instances, err := instance2.SearchInstance(cliParam.DatabaseType, "")
			if err != nil {
				log.Fatale(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.GetInstance().Key.DisplayString())
				}
			}
		})
	util.RegisterCliCommand(commandMap, "list-instance-sorted", "information", `list instance sorted by qualified name`,
		func(cliParam *dtstruct.CliParam) {
			instances, err := instance2.BulkReadInstance(cliParam.DatabaseType)
			if err != nil {
				log.Fatalf("Error: Failed to retrieve instances: %v\n", err)
				return
			}
			var asciiInstances dtstruct.CommandSlice
			for _, v := range instances {
				asciiInstances = append(asciiInstances, v.String())
			}
			sort.Sort(asciiInstances)
			fmt.Printf("%s\n", strings.Join(asciiInstances, "\n"))
		})
	util.RegisterCliCommand(commandMap, "list-instance-downtime", "information", `list instance marked as downtime, potentially filtered by cluster`,
		func(cliParam *dtstruct.CliParam) {
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			instances, err := instance2.ReadInstanceDowntime(cliParam.DatabaseType, clusterName)
			if err != nil {
				log.Fatale(err)
			}
			for _, clusterInstance := range instances {
				fmt.Println(clusterInstance.GetInstance().Key.DisplayString())
			}
		})
	util.RegisterCliCommand(commandMap, "list-instance-lost-in-recovery", "information", `list instance marked as downtime for being lost in a recovery process`,
		func(cliParam *dtstruct.CliParam) {
			instances, err := instance2.ReadInstanceLostInRecovery(cliParam.DatabaseType, "")
			if err != nil {
				log.Fatale(err)
			}
			for _, instance := range instances {
				fmt.Println(instance.GetInstance().Key.DisplayString())
			}
		})
	util.RegisterCliCommand(commandMap, "list-instance-heuristic-cluster-pool", "information", `list instance of a given cluster which are in either any pool or in a specific pool`,
		func(cliParam *dtstruct.CliParam) {
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)

			instances, err := instance2.GetHeuristicClusterPoolInstance(cliParam.DatabaseType, clusterName, cliParam.Pool)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.GetInstance().Key.DisplayString())
				}
			}
		})
	util.RegisterCliCommand(commandMap, "list-cluster", "information", `list all cluster`,
		func(cliParam *dtstruct.CliParam) {
			clusters, err := base.ReadClusters("")
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(strings.Join(clusters, "\n"))
		})
	util.RegisterCliCommand(commandMap, "list-cluster-alias", "information", `list all cluster alias`,
		func(cliParam *dtstruct.CliParam) {
			clusters, err := base.ReadAllClusterInfo("", "")
			if err != nil {
				log.Fatale(err)
			}
			for _, cluster := range clusters {
				fmt.Println(fmt.Sprintf("%s\t%s", cluster.ClusterName, cluster.ClusterAlias))
			}
		})
	util.RegisterCliCommand(commandMap, "list-cluster-instance", "information", `output the list of instance participating in same cluster as given instance`,
		func(cliParam *dtstruct.CliParam) {
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			instances, err := instance2.ReadClusterInstance(cliParam.DatabaseType, clusterName)
			if err != nil {
				log.Fatale(err)
			}
			for _, clusterInstance := range instances {
				fmt.Println(clusterInstance.GetInstance().Key.DisplayString())
			}
		})
	util.RegisterCliCommand(commandMap, "list-cluster-master", "information", `list of writeable master, one per cluster`,
		func(cliParam *dtstruct.CliParam) {
			instances, err := instance2.ReadMasterWriteable()
			if err != nil {
				log.Fatale(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.Key.DisplayString())
				}
			}
		})

	util.RegisterCliCommand(commandMap, "list-host-unresolved", "information", `list the content of the ham_hostname_unresolved table. generally used for debugging`,
		func(cliParam *dtstruct.CliParam) {
			unresolves, err := base.ReadAllHostnameUnresolves()
			if err != nil {
				log.Fatale(err)
			}
			for _, r := range unresolves {
				fmt.Println(r)
			}
		})

	util.RegisterCliCommand(commandMap, "search", "information", `search instance by name, version, version comment, port`,
		func(cliParam *dtstruct.CliParam) {
			if cliParam.Pattern == "" {
				log.Fatal("No pattern given")
			}
			instances, err := instance2.SearchInstance(cliParam.DatabaseType, cliParam.Pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.GetInstance().Key.DisplayString())
				}
			}
		})
	util.RegisterCliCommand(commandMap, "find", "information", `find instance whose hostname matches given regex pattern`,
		func(cliParam *dtstruct.CliParam) {
			if cliParam.Pattern == "" {
				log.Fatal("No pattern given")
			}
			instances, err := instance2.FindInstance(cliParam.DatabaseType, cliParam.Pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, instance := range instances {
					fmt.Println(instance.GetInstance().Key.DisplayString())
				}
			}
		})
}

// replicationCmd
func replicationCmd() {
	util.RegisterCliCommand(commandMap, "repl-list-replica", "replication", `output the fully-qualified hostname:port list of replicas of a given instance`,
		func(cliParam *dtstruct.CliParam) {
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unable to get replicas: unresolved instance")
			}
			replicas, err := instance2.ReadInstanceDownStream(cliParam.DatabaseType, instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			for _, replica := range replicas {
				fmt.Println(replica.GetInstance().Key.DisplayString())
			}
		})
	util.RegisterCliCommand(commandMap, "repl-is-replicating", "replication", `is an instance (-i) actively replicating right now`,
		func(cliParam *dtstruct.CliParam) {
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance := instance2.IsInstanceExistInBackendDB(instanceKey)
			if instance.ReplicaRunning() {
				fmt.Println(instance.GetInstance().Key.DisplayString())
			}
		})
	util.RegisterCliCommand(commandMap, "repl-is-replication-stopped", "replication", `is an instance's (-i) replication stopped`,
		func(cliParam *dtstruct.CliParam) {
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance := instance2.IsInstanceExistInBackendDB(instanceKey)
			if !instance.ReplicaRunning() {
				fmt.Println(instance.GetInstance().Key.DisplayString())
			}
		})
	// Replication, information
	util.RegisterCliCommand(commandMap, "repl-can-replicate-from", "replication", `can an instance (-i) replicate from another (-d) according to replication rules? prints 'true|false'`,
		func(cliParam *dtstruct.CliParam) {
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance := instance2.IsInstanceExistInBackendDB(instanceKey)
			if cliParam.DestinationKey == nil {
				log.Fatal("Cannot deduce target instance:", cliParam.Destination)
			}
			otherInstance := instance2.IsInstanceExistInBackendDB(cliParam.DestinationKey)

			if canReplicate, _ := replication.CanReplicateFrom(instance, otherInstance); canReplicate {
				fmt.Println(cliParam.DestinationKey.DisplayString())
			}
		})
	util.RegisterCliCommand(commandMap, "repl-set-read-only", "replication", `turn an instance read-only`,
		func(cliParam *dtstruct.CliParam) {
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := replication.SetReadOnly(instanceKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		})
	util.RegisterCliCommand(commandMap, "repl-set-writeable", "replication", `turn an instance writeable`,
		func(cliParam *dtstruct.CliParam) {
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := replication.SetReadOnly(instanceKey, false)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		})
	util.RegisterCliCommand(commandMap, "repl-get-candidate", "replication", `Information command suggesting the most up-to-date replica of a given instance that is good for promotion`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", cliParam.Instance)
			}

			instance, _, _, _, _, err := topology.GetCandidateReplica(instanceKey, false)
			if err != nil {
				log.Fatale(err)
			} else {
				fmt.Println(instance.GetInstance().Key.DisplayString())
			}
		}
	})
}

// globalCmd
func globalCmd() {
	util.RegisterCliCommand(commandMap, "global-recovery-disable", "global", `disallow to perform recovery globally`,
		func(cliParam *dtstruct.CliParam) {
			if err := base.DisableRecovery(); err != nil {
				log.Fatalf("ERROR: Failed to disable recoveries globally: %v\n", err)
			}
			fmt.Println("OK: recoveries DISABLED globally")
		})
	util.RegisterCliCommand(commandMap, "global-recovery-enable", "global", `allow to perform recovery globally`,
		func(cliParam *dtstruct.CliParam) {
			if err := base.EnableRecovery(); err != nil {
				log.Fatalf("ERROR: Failed to enable recoveries globally: %v\n", err)
			}
			fmt.Println("OK: recoveries ENABLED globally")
		})
	util.RegisterCliCommand(commandMap, "global-recovery-check", "global", `show global recovery configuration`,
		func(cliParam *dtstruct.CliParam) {
			isDisabled, err := base.IsRecoveryDisabled()
			if err != nil {
				log.Fatalf("ERROR: Failed to determine if recoveries are disabled globally: %v\n", err)
			}
			fmt.Printf("OK: Global recoveries disabled: %v\n", isDisabled)
		})
}

// topologyCmd
func topologyCmd() {
	util.RegisterCliCommand(commandMap, "topology", "topology", `show an ascii-graph of a replication topology, given a member of that topology`,
		func(cliParam *dtstruct.CliParam) {
			//clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			// TODO====
			request := &dtstruct.Request{InstanceKey: cliParam.InstanceKey}
			output, err := topology.Topology(request, cliParam.Pattern, false, false)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(output)
		})
	util.RegisterCliCommand(commandMap, "topology-with-tag", "topology", `show an ascii-graph of a replication topology and instance tag, given a member of that topology`,
		func(cliParam *dtstruct.CliParam) {
			//clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			// TODO====
			request := &dtstruct.Request{InstanceKey: cliParam.InstanceKey}
			output, err := topology.Topology(request, cliParam.Pattern, false, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(output)
		})
	util.RegisterCliCommand(commandMap, "topology-move-up", "topology", `move a replica one level up the topology`,
		func(cliParam *dtstruct.CliParam) {
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			instance, err := topology.MoveUp(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", cliParam.InstanceKey.DisplayString(), instance.GetInstance().Key.DisplayString()))
		})
	util.RegisterCliCommand(commandMap, "topology-move-up-replica", "topology", `move replica of the given instance one level up the topology`,
		func(cliParam *dtstruct.CliParam) {
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", cliParam.Instance)
			}

			movedReplicas, _, err, errs := topology.MoveUpReplicas(cliParam.InstanceKey, cliParam.Pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, e := range errs {
					log.Errore(e)
				}
				for _, replica := range movedReplicas {
					fmt.Println(replica.GetInstance().Key.DisplayString())
				}
			}
		})
	util.RegisterCliCommand(commandMap, "topology-move-below", "topology", `move a replica beneath its sibling. both replicas must be actively replicating from same master`,
		func(cliParam *dtstruct.CliParam) {
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if cliParam.DestinationKey == nil {
				log.Fatal("Cannot deduce destination/sibling:", cliParam.Destination)
			}
			_, err := topology.MoveBelow(instanceKey, cliParam.DestinationKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), cliParam.DestinationKey.DisplayString()))
		})
	util.RegisterCliCommand(commandMap, "topology-move-equivalent", "topology", `move a replica beneath another instance`,
		func(cliParam *dtstruct.CliParam) {
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if cliParam.DestinationKey == nil {
				log.Fatal("Cannot deduce destination:", cliParam.Destination)
			}
			_, err := topology.MoveEquivalent(instanceKey, cliParam.DestinationKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), cliParam.DestinationKey.DisplayString()))
		})
	// smart mode
	util.RegisterCliCommand(commandMap, "topology-relocate", "topology", `relocate a replica beneath another instance`,
		func(cliParam *dtstruct.CliParam) {
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if cliParam.DestinationKey == nil {
				log.Fatal("Cannot deduce destination:", cliParam.Destination)
			}
			_, err := topology.RelocateBelow(instanceKey, cliParam.DestinationKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), cliParam.DestinationKey.DisplayString()))
		})
	// Instance

}

// recoveryCmd
func recoveryCmd() {
	util.RegisterCliCommand(commandMap, "recovery-replication-analysis", "recovery", `request an analysis of potential crash incidents in all known topologies`,
		func(cliParam *dtstruct.CliParam) {
			analysis, err := replication.GetReplicationAnalysis(cliParam.DatabaseType, "", cliParam.ClusterId, &dtstruct.ReplicationAnalysisHints{})
			if err != nil {
				log.Fatale(err)
			}
			for _, entry := range analysis {
				fmt.Println(fmt.Sprintf("%s (cluster %s): %s", entry.AnalyzedInstanceKey.DisplayString(), entry.ClusterDetails.ClusterName, entry.AnalysisString()))
			}
		})
	util.RegisterCliCommand(commandMap, "recovery-force-master-failover", "recovery", `forcibly discard master and initiate a failover, even if doesn't see a problem. this command choose the replacement master automatically`,
		func(cliParam *dtstruct.CliParam) {
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			topologyRecovery, err := recovery.ForceMasterFailOver(cliParam.DatabaseType, clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(topologyRecovery.SuccessorKey.DisplayString())
		})
	util.RegisterCliCommand(commandMap, "recovery-force-master-takeover", "recovery", `forcibly discard master and promote another (direct child) instance instead, even if everything is running well`,
		func(cliParam *dtstruct.CliParam) {
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			if cliParam.DestinationKey == nil {
				log.Fatal("Cannot deduce destination, the instance to promote in place of the master. Please provide with -d")
			}
			destination := instance2.IsInstanceExistInBackendDB(cliParam.DestinationKey)
			topologyRecovery, err := recovery.ForceMasterTakeover(clusterName, destination)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(topologyRecovery.SuccessorKey.DisplayString())
		})
	util.RegisterCliCommand(commandMap, "recovery-graceful-master-takeover", "recovery", `gracefully promote a new master. either indicate identity of new master via '-d designated.instance.com' or setup replication tree to have a single direct replica to the master`,
		func(cliParam *dtstruct.CliParam) {
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			if cliParam.DestinationKey != nil {
				instance2.IsInstanceExistInBackendDB(cliParam.DestinationKey)
			}
			topologyRecovery, promotedMasterCoordinates, err := recovery.GracefulMasterTakeover(clusterName, cliParam.DestinationKey, false)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(topologyRecovery.SuccessorKey.DisplayString())
			fmt.Println(*promotedMasterCoordinates)
			log.Debugf("Promoted %+v as new master. Binlog coordinates at time of promotion: %+v", topologyRecovery.SuccessorKey, *promotedMasterCoordinates)
		})
	util.RegisterCliCommand(commandMap, "recovery-graceful-master-takeover-auto", "recovery", `gracefully promote a new master. will attempt to pick the promoted replica automatically`,
		func(cliParam *dtstruct.CliParam) {
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			// destinationKey doesn't _have_ to be specified: if unspecified, ham4db will auto-deduce a replica.
			// but if specified, then that's the replica to promote, and it must be valid.
			if cliParam.DestinationKey != nil {
				instance2.IsInstanceExistInBackendDB(cliParam.DestinationKey)
			}
			topologyRecovery, promotedMasterCoordinates, err := recovery.GracefulMasterTakeover(clusterName, cliParam.DestinationKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(topologyRecovery.SuccessorKey.DisplayString())
			fmt.Println(*promotedMasterCoordinates)
			log.Debugf("Promoted %+v as new master. Binlog coordinates at time of promotion: %+v", topologyRecovery.SuccessorKey, *promotedMasterCoordinates)
		})
}

// tagCmd
func tagCmd() {
	util.RegisterCliCommand(commandMap, "tags", "tags", `List tags for a given instance`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey != nil {
				instanceKey.ClusterId = cliParam.ClusterId
			}
			tags, err := base.ReadInstanceTags(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			for _, tag := range tags {
				fmt.Println(tag.String())
			}
		}
	})
	util.RegisterCliCommand(commandMap, "tag-value", "tags", `Get tag value for a specific instance`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			tag, err := dtstruct.ParseTag(*dtstruct.RuntimeCLIFlags.Tag)
			if err != nil {
				log.Fatale(err)
			}
			if cliParam.ClusterId != "" {
				instanceKey.ClusterId = cliParam.ClusterId
			}
			if instanceKey != nil {
				instanceKey.ClusterId = cliParam.ClusterId
			}
			tagExists, err := base.ReadInstanceTag(instanceKey, tag)
			if err != nil {
				log.Fatale(err)
			}
			if tagExists {
				fmt.Println(tag.TagValue)
			}
		}
	})
	util.RegisterCliCommand(commandMap, "tagged", "tags", `List instances tagged by tag-string. Format: "tagname" or "tagname=tagvalue" or comma separated "tag0,tag1=val1,tag2" for intersection of all.`, func(cliParam *dtstruct.CliParam) {
		{
			tagsString := *dtstruct.RuntimeCLIFlags.Tag
			instanceKeyMap, err := base.GetInstanceKeysByTags(tagsString)
			if err != nil {
				log.Fatale(err)
			}
			keysDisplayStrings := []string{}
			for _, key := range instanceKeyMap.GetInstanceKeys() {
				keysDisplayStrings = append(keysDisplayStrings, key.DisplayString())
			}
			sort.Strings(keysDisplayStrings)
			for _, s := range keysDisplayStrings {
				fmt.Println(s)
			}
		}
	})
	util.RegisterCliCommand(commandMap, "tag", "tags", `Add a tag to a given instance. Tag in "tagname" or "tagname=tagvalue" format`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			tag, err := dtstruct.ParseTag(*dtstruct.RuntimeCLIFlags.Tag)
			if err != nil {
				log.Fatale(err)
			}
			if instanceKey != nil {
				instanceKey.ClusterId = cliParam.ClusterId
			}
			base.PutInstanceTag(instanceKey, tag)
			fmt.Println(instanceKey.DisplayString())
		}
	})
	util.RegisterCliCommand(commandMap, "untag", "tags", `Remove a tag from an instance`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			tag, err := dtstruct.ParseTag(*dtstruct.RuntimeCLIFlags.Tag)
			if err != nil {
				log.Fatale(err)
			}
			if instanceKey != nil {
				instanceKey.ClusterId = cliParam.ClusterId
			}
			untagged, err := base.Untag(instanceKey, tag)
			if err != nil {
				log.Fatale(err)
			}
			for _, key := range untagged.GetInstanceKeys() {
				fmt.Println(key.DisplayString())
			}
		}
	})
	util.RegisterCliCommand(commandMap, "untag-all", "tags", `Remove a tag from all matching instances`, func(cliParam *dtstruct.CliParam) {
		{
			tag, err := dtstruct.ParseTag(*dtstruct.RuntimeCLIFlags.Tag)
			if err != nil {
				log.Fatale(err)
			}
			untagged, err := base.Untag(nil, tag)
			if err != nil {
				log.Fatale(err)
			}
			for _, key := range untagged.GetInstanceKeys() {
				fmt.Println(key.DisplayString())
			}
		}
	})
}

// instanceMgtCmd
func instanceMgtCmd() {
	util.RegisterCliCommand(commandMap, "discover", "Instance management", `Lookup an instance, investigate it`, func(cliParam *dtstruct.CliParam) {
		{
			if cliParam.InstanceKey == nil {
				cliParam.InstanceKey = thisInstanceKey
			}
			if cliParam.InstanceKey == nil {
				log.Fatalf("Cannot figure instance key")
			}
			instance, err := instance2.GetInfoFromInstance(context.TODO(), cliParam.InstanceKey, "", nil)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instance.GetInstance().Key.DisplayString())
		}
	})
	util.RegisterCliCommand(commandMap, "begin-maintenance", "Instance management", `Request a maintenance lock on an instance`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if cliParam.Reason == "" {
				log.Fatal("--reason option required")
			}
			var durationSeconds int = 0
			if cliParam.Duration != "" {
				durationSeconds, err := tutil.SimpleTimeToSeconds(cliParam.Duration)
				if err != nil {
					log.Fatale(err)
				}
				if durationSeconds < 0 {
					log.Fatalf("Duration value must be non-negative. Given value: %d", durationSeconds)
				}
			}
			maintenanceKey, err := base.BeginBoundedMaintenance(instanceKey, dtstruct.GetMaintenanceOwner(), cliParam.Reason, uint(durationSeconds), true)
			if err == nil {
				log.Infof("Maintenance key: %+v", maintenanceKey)
				log.Infof("Maintenance duration: %d seconds", durationSeconds)
			}
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	util.RegisterCliCommand(commandMap, "end-maintenance", "Instance management", `Remove maintenance lock from an instance`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := base.EndMaintenanceByInstanceKey(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	util.RegisterCliCommand(commandMap, "in-maintenance", "Instance management", `Check whether instance is under maintenance`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			inMaintenance, err := base.InMaintenance(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			if inMaintenance {
				fmt.Println(instanceKey.DisplayString())
			}
		}
	})
	util.RegisterCliCommand(commandMap, "begin-downtime", "Instance management", `Mark an instance as downtimed`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if cliParam.Reason == "" {
				log.Fatal("--reason option required")
			}
			var durationSeconds = 0
			if cliParam.Duration != "" {
				durationSeconds, err := tutil.SimpleTimeToSeconds(cliParam.Duration)
				if err != nil {
					log.Fatale(err)
				}
				if durationSeconds < 0 {
					log.Fatalf("Duration value must be non-negative. Given value: %d", durationSeconds)
				}
			}
			duration := time.Duration(durationSeconds) * time.Second
			err := base.BeginDowntime(dtstruct.NewDowntime(instanceKey, dtstruct.GetMaintenanceOwner(), cliParam.Reason, duration))
			if err == nil {
				log.Infof("Downtime duration: %d seconds", durationSeconds)
			} else {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	util.RegisterCliCommand(commandMap, "end-downtime", "Instance management", `Indicate an instance is no longer downtimed`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := base.EndDowntime(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
}
