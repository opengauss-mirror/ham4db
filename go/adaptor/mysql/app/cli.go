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
	"context"
	"fmt"
	mconstant "gitee.com/opengauss/ham4db/go/adaptor/mysql/common/constant"
	"gitee.com/opengauss/ham4db/go/adaptor/mysql/ham"
	"gitee.com/opengauss/ham4db/go/config"
	"gitee.com/opengauss/ham4db/go/core/base"
	"gitee.com/opengauss/ham4db/go/core/ha/process"
	"gitee.com/opengauss/ham4db/go/core/log"
	"gitee.com/opengauss/ham4db/go/core/security/token"
	cutil "gitee.com/opengauss/ham4db/go/core/util"
	"gitee.com/opengauss/ham4db/go/dtstruct"
	"gitee.com/opengauss/ham4db/go/util/sqlutil"
	"sort"
	"strings"
)

var thisInstanceKey *dtstruct.InstanceKey
var commandMap map[string]dtstruct.CommandDesc

func init() {
	commandMap = make(map[string]dtstruct.CommandDesc)
	smartRelocationCmd()
	relocationCmd()
}

// smartRelocationCmd command for smart relocation
func smartRelocationCmd() {
	cutil.RegisterCliCommand(commandMap, "relocate-replicas", "Smart relocation", `Relocates all or part of the replicas of a given instance under another instance`, func(cliParam *dtstruct.CliParam) {
		instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
		if cliParam.DestinationKey == nil {
			log.Fatal("Cannot deduce destination:", cliParam.Destination)
		}
		replicas, _, err, errs := ham.RelocateReplicas(instanceKey, cliParam.DestinationKey, cliParam.Pattern)
		if err != nil {
			log.Fatale(err)
		} else {
			for _, e := range errs {
				log.Errore(e)
			}
			for _, replica := range replicas {
				fmt.Println(replica.Key.DisplayString())
			}
		}
	})

	cutil.RegisterCliCommand(commandMap, "take-siblings", "Smart relocation", `Turn all siblings of a replica into its sub-replicas.`, func(cliParam *dtstruct.CliParam) {
		instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
		if instanceKey == nil {
			log.Fatal("Cannot deduce instance:", cliParam.Instance)
		}
		_, _, err := ham.TakeSiblings(instanceKey)
		if err != nil {
			log.Fatale(err)
		}
		fmt.Println(instanceKey.DisplayString())
	})

	cutil.RegisterCliCommand(commandMap, "regroup-replicas", "Smart relocation", `Given an instance, pick one of its replicas and make it local master of its siblings`, func(cliParam *dtstruct.CliParam) {
		instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
		if instanceKey == nil {
			log.Fatal("Cannot deduce instance:", cliParam.Instance)
		}
		ham.ValidateInstanceIsFound(instanceKey)

		lostReplicas, equalReplicas, aheadReplicas, cannotReplicateReplicas, promotedReplica, err := ham.RegroupReplicas(instanceKey, false, func(candidateReplica dtstruct.InstanceAdaptor) {
			fmt.Println(candidateReplica.GetInstance().Key.DisplayString())
		}, cliParam.PostponedFunctionsContainer)
		lostReplicas = append(lostReplicas, cannotReplicateReplicas...)

		cliParam.PostponedFunctionsContainer.Wait()
		if promotedReplica == nil {
			log.Fatalf("Could not regroup replicas of %+v; error: %+v", *instanceKey, err)
		}
		fmt.Println(fmt.Sprintf("%s lost: %d, trivial: %d, pseudo-gtid: %d",
			promotedReplica.Key.DisplayString(), len(lostReplicas), len(equalReplicas), len(aheadReplicas)))
		if err != nil {
			log.Fatale(err)
		}
	})
}

func relocationCmd() {
	cutil.RegisterCliCommand(commandMap, "repoint", "Classic file:pos relocation", `Make the given instance replicate from another instance without changing the binglog coordinates. Use with care`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			// destinationKey can be null, in which case the instance repoints to its existing master
			instance, err := ham.Repoint(instanceKey, cliParam.DestinationKey, mconstant.GTIDHintNeutral)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), instance.UpstreamKey.DisplayString()))
		}
	})

	cutil.RegisterCliCommand(commandMap, "repoint-replicas", "Classic file:pos relocation", `Repoint all replicas of given instance to replicate back from the instance. Use with care`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			repointedReplicas, err, errs := ham.RepointReplicasTo(instanceKey, cliParam.Pattern, cliParam.DestinationKey)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, e := range errs {
					log.Errore(e)
				}
				for _, replica := range repointedReplicas {
					fmt.Println(fmt.Sprintf("%s<%s", replica.Key.DisplayString(), instanceKey.DisplayString()))
				}
			}
		}
	})

	cutil.RegisterCliCommand(commandMap, "take-master", "Classic file:pos relocation", `Turn an instance into a master of its own master; essentially switch the two.`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", cliParam.Instance)
			}
			_, err := ham.TakeMaster(instanceKey, false)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})

	cutil.RegisterCliCommand(commandMap, "make-co-master", "Classic file:pos relocation", `Create a master-master replication. Given instance is a replica which replicates directly from a master.`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := ham.MakeCoMaster(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
}

// Cli initiates a command line interface, executing requested command.
func CliCmd(commandMap map[string]dtstruct.CommandDesc, cliParam *dtstruct.CliParam) {
	cutil.RegisterCliCommand(commandMap, "regroup-replicas-bls", "Binlog server relocation", `Regroup Binlog Server replicas of a given instance`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", cliParam.Instance)
			}
			ham.ValidateInstanceIsFound(instanceKey)

			_, promotedBinlogServer, err := ham.RegroupReplicasBinlogServers(instanceKey, false)
			if promotedBinlogServer == nil {
				log.Fatalf("Could not regroup binlog server replicas of %+v; error: %+v", *instanceKey, err)
			}
			fmt.Println(promotedBinlogServer.Key.DisplayString())
			if err != nil {
				log.Fatale(err)
			}
		}
	})

	// move, GTID
	cutil.RegisterCliCommand(commandMap, "move-gtid", "GTID relocation", `Move a replica beneath another instance.`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if cliParam.DestinationKey == nil {
				log.Fatal("Cannot deduce destination:", cliParam.Destination)
			}
			_, err := ham.MoveBelowGTID(instanceKey, cliParam.DestinationKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), cliParam.DestinationKey.DisplayString()))
		}
	})
	cutil.RegisterCliCommand(commandMap, "move-replicas-gtid", "GTID relocation", `Moves all replicas of a given instance under another (destination) instance using GTID`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if cliParam.DestinationKey == nil {
				log.Fatal("Cannot deduce destination:", cliParam.Destination)
			}
			movedReplicas, _, err, errs := ham.MoveReplicasGTID(instanceKey, cliParam.DestinationKey, cliParam.Pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, e := range errs {
					log.Errore(e)
				}
				for _, replica := range movedReplicas {
					fmt.Println(replica.Key.DisplayString())
				}
			}
		}
	})
	cutil.RegisterCliCommand(commandMap, "regroup-replicas-gtid", "GTID relocation", `Given an instance, pick one of its replica and make it local master of its siblings, using GTID.`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", cliParam.Instance)
			}
			ham.ValidateInstanceIsFound(instanceKey)

			lostReplicas, movedReplicas, cannotReplicateReplicas, promotedReplica, err := ham.RegroupReplicasGTID(instanceKey, false, func(candidateReplica dtstruct.InstanceAdaptor) {
				fmt.Println(candidateReplica.GetInstance().Key.DisplayString())
			}, cliParam.PostponedFunctionsContainer, nil)
			lostReplicas = append(lostReplicas, cannotReplicateReplicas...)

			if promotedReplica == nil {
				log.Fatalf("Could not regroup replicas of %+v; error: %+v", *instanceKey, err)
			}
			fmt.Println(fmt.Sprintf("%s lost: %d, moved: %d",
				promotedReplica.Key.DisplayString(), len(lostReplicas), len(movedReplicas)))
			if err != nil {
				log.Fatale(err)
			}
		}
	})
	// Pseudo-GTID
	cutil.RegisterCliCommand(commandMap, "match", "Pseudo-GTID relocation", `Matches a replica beneath another (destination) instance using Pseudo-GTID`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if cliParam.DestinationKey == nil {
				log.Fatal("Cannot deduce destination:", cliParam.Destination)
			}
			_, _, err := ham.MatchBelow(instanceKey, cliParam.DestinationKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), cliParam.DestinationKey.DisplayString()))
		}
	})
	cutil.RegisterCliCommand(commandMap, "match-up", "Pseudo-GTID relocation", `Transport the replica one level up the hierarchy, making it child of its grandparent, using Pseudo-GTID`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			instance, _, err := ham.MatchUp(instanceKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), instance.UpstreamKey.DisplayString()))
		}
	})
	cutil.RegisterCliCommand(commandMap, "rematch", "Pseudo-GTID relocation", `Reconnect a replica onto its master, via PSeudo-GTID.`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			instance, _, err := ham.RematchReplica(instanceKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%s<%s", instanceKey.DisplayString(), instance.UpstreamKey.DisplayString()))
		}
	})
	cutil.RegisterCliCommand(commandMap, "match-replicas", "Pseudo-GTID relocation", `Matches all replicas of a given instance under another (destination) instance using Pseudo-GTID`, func(cliParam *dtstruct.CliParam) {
		{
			// Move all replicas of "instance" beneath "destination"
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", cliParam.Instance)
			}
			if cliParam.DestinationKey == nil {
				log.Fatal("Cannot deduce destination:", cliParam.Destination)
			}

			matchedReplicas, _, err, errs := ham.MultiMatchReplicas(instanceKey, cliParam.DestinationKey, cliParam.Pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, e := range errs {
					log.Errore(e)
				}
				for _, replica := range matchedReplicas {
					fmt.Println(replica.Key.DisplayString())
				}
			}
		}
	})
	cutil.RegisterCliCommand(commandMap, "match-up-replicas", "Pseudo-GTID relocation", `Matches replicas of the given instance one level up the topology, making them siblings of given instance, using Pseudo-GTID`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", cliParam.Instance)
			}

			matchedReplicas, _, err, errs := ham.MatchUpReplicas(instanceKey, cliParam.Pattern)
			if err != nil {
				log.Fatale(err)
			} else {
				for _, e := range errs {
					log.Errore(e)
				}
				for _, replica := range matchedReplicas {
					fmt.Println(replica.Key.DisplayString())
				}
			}
		}
	})
	cutil.RegisterCliCommand(commandMap, "regroup-replicas-pgtid", "Pseudo-GTID relocation", `Given an instance, pick one of its replica and make it local master of its siblings, using Pseudo-GTID.`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", cliParam.Instance)
			}
			ham.ValidateInstanceIsFound(instanceKey)

			onCandidateReplicaChosen := func(candidateReplica dtstruct.InstanceAdaptor) {
				fmt.Println(candidateReplica.GetInstance().Key.DisplayString())
			}
			lostReplicas, equalReplicas, aheadReplicas, cannotReplicateReplicas, promotedReplica, err := ham.RegroupReplicasPseudoGTID(instanceKey, false, onCandidateReplicaChosen, cliParam.PostponedFunctionsContainer, nil)
			lostReplicas = append(lostReplicas, cannotReplicateReplicas...)
			cliParam.PostponedFunctionsContainer.Wait()
			if promotedReplica == nil {
				log.Fatalf("Could not regroup replicas of %+v; error: %+v", *instanceKey, err)
			}
			fmt.Println(fmt.Sprintf("%s lost: %d, trivial: %d, pseudo-gtid: %d",
				promotedReplica.Key.DisplayString(), len(lostReplicas), len(equalReplicas), len(aheadReplicas)))
			if err != nil {
				log.Fatale(err)
			}
		}
	})

	cutil.RegisterCliCommand(commandMap, "list-cluster-heuristic-lag", "information", `for a given cluster (indicated by an instance or alias), output a heuristic "representative" lag of that cluster`, func(cliParam *dtstruct.CliParam) {
		{
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			lag, err := ham.GetClusterHeuristicLag(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(lag)
		}
	})

	// General replication commands
	cutil.RegisterCliCommand(commandMap, "enable-gtid", "Replication, general", `If possible, turn on GTID replication`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := ham.EnableGTID(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "disable-gtid", "Replication, general", `Turn off GTID replication, back to file:pos replication`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := ham.DisableGTID(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "which-gtid-errant", "Replication, general", `Get errant GTID set (empty results if no errant GTID)`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)

			instance, err := ham.GetInfoFromInstance(instanceKey, false, false, nil, "")
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			fmt.Println(instance.GtidErrant)
		}
	})
	cutil.RegisterCliCommand(commandMap, "gtid-errant-reset-master", "Replication, general", `Reset master on instance, remove GTID errant transactions`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := ham.ErrantGTIDResetMaster(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "skip-query", "Replication, general", `Skip a single statement on a replica; either when running with GTID or without`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := ham.SkipQuery(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "stop-slave", "Replication, general", `Issue a STOP SLAVE on an instance`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := ham.StopReplication(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "start-slave", "Replication, general", `Issue a START SLAVE on an instance`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := ham.StartReplication(context.TODO(), instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "restart-slave", "Replication, general", `STOP and START SLAVE on an instance`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := ham.RestartReplication(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "reset-slave", "Replication, general", `Issues a RESET SLAVE command; use with care`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := ham.ResetReplicationOperation(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "detach-replica-master-host", "Replication, general", `Stops replication and modifies Master_Host into an impossible, yet reversible, value.`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", cliParam.Instance)
			}
			_, err := ham.DetachMaster(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "reattach-replica-master-host", "Replication, general", `Undo a detach-replica-master-host operation`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatal("Cannot deduce instance:", cliParam.Instance)
			}
			_, err := ham.ReattachMaster(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "master-pos-wait", "Replication, general", `Wait until replica reaches given replication coordinates (--binlog=file:pos)`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance, err := ham.GetInfoFromInstance(instanceKey, false, false, nil, "")
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			var binlogCoordinates *dtstruct.LogCoordinates

			if binlogCoordinates, err = dtstruct.ParseLogCoordinates(*dtstruct.RuntimeCLIFlags.BinlogFile); err != nil {
				log.Fatalf("Expecing --binlog argument as file:pos")
			}
			_, err = ham.MasterPosWait(instanceKey, binlogCoordinates)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "enable-semi-sync-master", "Replication, general", `Enable semi-sync replication (master-side)`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := ham.SetSemiSyncMaster(instanceKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "disable-semi-sync-master", "Replication, general", `Disable semi-sync replication (master-side)`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := ham.SetSemiSyncMaster(instanceKey, false)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "enable-semi-sync-replica", "Replication, general", `Enable semi-sync replication (replica-side)`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := ham.SetSemiSyncReplica(instanceKey, true)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "disable-semi-sync-replica", "Replication, general", `Disable semi-sync replication (replica-side)`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			_, err := ham.SetSemiSyncReplica(instanceKey, false)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "restart-slave-statements", "Replication, general", `Get a list of statements to execute to stop then restore replica to same execution state. Provide --statement for injected statement`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			statements, err := ham.GetReplicationRestartPreserveStatements(instanceKey, *dtstruct.RuntimeCLIFlags.Statement)
			if err != nil {
				log.Fatale(err)
			}
			for _, statement := range statements {
				fmt.Println(statement)
			}
		}
	})

	// Binary log operations
	cutil.RegisterCliCommand(commandMap, "flush-binary-logs", "Binary logs", `Flush binary logs on an instance`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			var err error
			if *dtstruct.RuntimeCLIFlags.BinlogFile == "" {
				_, err = ham.FlushBinaryLogs(instanceKey, 1)
			} else {
				_, err = ham.FlushBinaryLogsTo(instanceKey, *dtstruct.RuntimeCLIFlags.BinlogFile)
			}
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "purge-binary-logs", "Binary logs", `Purge binary logs of an instance`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			var err error
			if *dtstruct.RuntimeCLIFlags.BinlogFile == "" {
				log.Fatal("expecting --binlog value")
			}

			_, err = ham.PurgeBinaryLogsTo(instanceKey, *dtstruct.RuntimeCLIFlags.BinlogFile, false)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "last-pseudo-gtid", "Binary logs", `Find latest Pseudo-GTID entry in instance's binary logs`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance, err := ham.GetInfoFromInstance(instanceKey, false, false, nil, "")
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			coordinates, text, err := ham.FindLastPseudoGTIDEntry(instance, instance.RelaylogCoordinates, nil, cliParam.Strict, nil)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%+v:%s", *coordinates, text))
		}
	})
	cutil.RegisterCliCommand(commandMap, "locate-gtid-errant", "Binary logs", `List binary logs containing errant GTIDs`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			errantBinlogs, err := ham.LocateErrantGTID(instanceKey)
			if err != nil {
				log.Fatale(err)
			}
			for _, binlog := range errantBinlogs {
				fmt.Println(binlog)
			}
		}
	})
	cutil.RegisterCliCommand(commandMap, "last-executed-relay-entry", "Binary logs", `Find coordinates of last executed relay log entry`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance, err := ham.GetInfoFromInstance(instanceKey, false, false, nil, "")
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			minCoordinates, err := ham.GetPreviousKnownRelayLogCoordinatesForInstance(instance)
			if err != nil {
				log.Fatalf("Error reading last known coordinates for %+v: %+v", instance.Key, err)
			}
			binlogEvent, err := ham.GetLastExecutedEntryInRelayLogs(instance, minCoordinates, instance.RelaylogCoordinates)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%+v:%d", *binlogEvent, binlogEvent.NextEventPos))
		}
	})
	cutil.RegisterCliCommand(commandMap, "correlate-relaylog-pos", "Binary logs", `Given an instance (-i) and relaylog coordinates (--binlog=file:pos), find the correlated coordinates in another instance's relay logs (-d)`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance, err := ham.GetInfoFromInstance(instanceKey, false, false, nil, "")
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			if cliParam.DestinationKey == nil {
				log.Fatal("Cannot deduce target instance:", cliParam.Destination)
			}
			otherInstance, err := ham.GetInfoFromInstance(cliParam.DestinationKey, false, false, nil, "")
			if err != nil {
				log.Fatale(err)
			}
			if otherInstance == nil {
				log.Fatalf("Instance not found: %+v", *cliParam.DestinationKey)
			}

			var relaylogCoordinates *dtstruct.LogCoordinates
			if *dtstruct.RuntimeCLIFlags.BinlogFile != "" {
				if relaylogCoordinates, err = dtstruct.ParseLogCoordinates(*dtstruct.RuntimeCLIFlags.BinlogFile); err != nil {
					log.Fatalf("Expecing --binlog argument as file:pos")
				}
			}
			instanceCoordinates, correlatedCoordinates, nextCoordinates, _, err := ham.CorrelateRelaylogCoordinates(instance, relaylogCoordinates, otherInstance)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%+v;%+v;%+v", *instanceCoordinates, *correlatedCoordinates, *nextCoordinates))
		}
	})
	cutil.RegisterCliCommand(commandMap, "find-binlog-entry", "Binary logs", `Get binlog file:pos of entry given by --pattern (exact full match, not a regular expression) in a given instance`, func(cliParam *dtstruct.CliParam) {
		{
			if cliParam.Pattern == "" {
				log.Fatal("No pattern given")
			}
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance, err := ham.GetInfoFromInstance(instanceKey, false, false, nil, "")
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			coordinates, err := ham.SearchEntryInInstanceBinlogs(instance, cliParam.Pattern, false, nil)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%+v", *coordinates))
		}
	})
	cutil.RegisterCliCommand(commandMap, "correlate-binlog-pos", "Binary logs", `Given an instance (-i) and binlog coordinates (--binlog=file:pos), find the correlated coordinates in another instance (-d)`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			if instanceKey == nil {
				log.Fatalf("Unresolved instance")
			}
			instance, err := ham.GetInfoFromInstance(instanceKey, false, false, nil, "")
			if err != nil {
				log.Fatale(err)
			}
			if instance == nil {
				log.Fatalf("Instance not found: %+v", *instanceKey)
			}
			if !instance.LogBinEnabled {
				log.Fatalf("Instance does not have binary logs: %+v", *instanceKey)
			}
			if cliParam.DestinationKey == nil {
				log.Fatal("Cannot deduce target instance:", cliParam.Destination)
			}
			otherInstance, err := ham.GetInfoFromInstance(cliParam.DestinationKey, false, false, nil, "")
			if err != nil {
				log.Fatale(err)
			}
			if otherInstance == nil {
				log.Fatalf("Instance not found: %+v", *cliParam.DestinationKey)
			}
			var binlogCoordinates *dtstruct.LogCoordinates
			if *dtstruct.RuntimeCLIFlags.BinlogFile == "" {
				binlogCoordinates = &instance.SelfBinlogCoordinates
			} else {
				if binlogCoordinates, err = dtstruct.ParseLogCoordinates(*dtstruct.RuntimeCLIFlags.BinlogFile); err != nil {
					log.Fatalf("Expecing --binlog argument as file:pos")
				}
			}

			coordinates, _, err := ham.CorrelateBinlogCoordinates(instance, binlogCoordinates, otherInstance)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%+v", *coordinates))
		}
	})
	// Pool
	cutil.RegisterCliCommand(commandMap, "submit-pool-instances", "Pools", `Submit a pool name with a list of instances in that pool`, func(cliParam *dtstruct.CliParam) {
		{
			if cliParam.Pool == "" {
				log.Fatal("Please submit --pool")
			}
			err := base.ApplyPoolInstance(dtstruct.NewPoolInstancesSubmission(cliParam.DatabaseType, cliParam.Pool, cliParam.Instance))
			if err != nil {
				log.Fatale(err)
			}
		}
	})
	cutil.RegisterCliCommand(commandMap, "cluster-pool-instances", "Pools", `List all pools and their associated instances`, func(cliParam *dtstruct.CliParam) {
		{
			clusterPoolInstances, err := base.ReadAllClusterPoolInstance()
			if err != nil {
				log.Fatale(err)
			}
			for _, clusterPoolInstance := range clusterPoolInstances {
				fmt.Println(fmt.Sprintf("%s\t%s\t%s\t%s:%d", clusterPoolInstance.ClusterName, clusterPoolInstance.ClusterAlias, clusterPoolInstance.Pool, clusterPoolInstance.Hostname, clusterPoolInstance.Port))
			}
		}
	})
	cutil.RegisterCliCommand(commandMap, "which-heuristic-domain-instance", "Information", `Returns the instance associated as the cluster's writer with a cluster's domain name.`, func(cliParam *dtstruct.CliParam) {
		{
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			instanceKey, err := base.GetHeuristicClusterDomainInstanceAttribute(mconstant.DBTMysql, clusterName)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "which-cluster-osc-replicas", "Information", `Output a list of replicas in a cluster, that could serve as a pt-online-schema-change operation control replicas`, func(cliParam *dtstruct.CliParam) {
		{
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			instances, err := ham.GetClusterOSCReplicas(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			for _, clusterInstance := range instances {
				fmt.Println(clusterInstance.GetInstance().Key.DisplayString())
			}
		}
	})
	cutil.RegisterCliCommand(commandMap, "which-cluster-gh-ost-replicas", "Information", `Output a list of replicas in a cluster, that could serve as a gh-ost working server`, func(cliParam *dtstruct.CliParam) {
		{
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			instances, err := getClusterGhostReplicas(clusterName)
			if err != nil {
				log.Fatale(err)
			}
			for _, clusterInstance := range instances {
				fmt.Println(clusterInstance.GetInstance().Key.DisplayString())
			}
		}
	})

	//	// Recovery & analysis
	//cutil.RegisterCliCommand(commandMap, "recover", "Recovery", `Do auto-recovery given a dead instance`), RegisterCliCommand("recover-lite", "Recovery", `Do auto-recovery given a dead instance. chooses the best course of actionwithout executing external processes`, func(cliParam *dtstruct.CliParam) {
	//	{
	//		instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
	//		if instanceKey == nil {
	//			log.Fatal("Cannot deduce instance:", instance)
	//		}
	//
	//		recoveryAttempted, promotedInstanceKey, err := base.CheckAndRecover(dtstruct.GetHamHandler(instanceKey.DBType), instanceKey, destinationKey, (command == "recover-lite"))
	//		if err != nil {
	//			log.Fatale(err)
	//		}
	//		if recoveryAttempted {
	//			if promotedInstanceKey == nil {
	//				log.Fatalf("Recovery attempted yet no replica promoted")
	//			}
	//			fmt.Println(promotedInstanceKey.DisplayString())
	//		}
	//	}})

	cutil.RegisterCliCommand(commandMap, "ack-all-recoveries", "Recovery", `Acknowledge all recoveries; this unblocks pending future recoveries`, func(cliParam *dtstruct.CliParam) {
		{
			if cliParam.Reason == "" {
				log.Fatal("--reason option required (comment your ack)")
			}
			countRecoveries, err := base.AcknowledgeAllRecoveries(dtstruct.GetMaintenanceOwner(), cliParam.Reason)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%d recoveries acknowledged", countRecoveries))
		}
	})
	cutil.RegisterCliCommand(commandMap, "ack-cluster-recoveries", "Recovery", `Acknowledge recoveries for a given cluster; this unblocks pending future recoveries`, func(cliParam *dtstruct.CliParam) {
		{
			if cliParam.Reason == "" {
				log.Fatal("--reason option required (comment your ack)")
			}
			clusterName := base.GetClusterNameWithAlias(cliParam.ClusterAlias, cliParam.InstanceKey, thisInstanceKey)
			countRecoveries, err := base.AcknowledgeClusterRecoveries(clusterName, dtstruct.GetMaintenanceOwner(), cliParam.Reason)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%d recoveries acknowledged", countRecoveries))
		}
	})
	cutil.RegisterCliCommand(commandMap, "ack-instance-recoveries", "Recovery", `Acknowledge recoveries for a given instance; this unblocks pending future recoveries`, func(cliParam *dtstruct.CliParam) {
		{
			if cliParam.Reason == "" {
				log.Fatal("--reason option required (comment your ack)")
			}
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)

			countRecoveries, err := base.AcknowledgeInstanceRecoveries(instanceKey, dtstruct.GetMaintenanceOwner(), cliParam.Reason)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(fmt.Sprintf("%d recoveries acknowledged", countRecoveries))
		}
	})
	// Instance meta
	cutil.RegisterCliCommand(commandMap, "register-candidate", "Instance, meta", `Indicate that a specific instance is a preferred candidate for master promotion`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			promotionRule, err := dtstruct.ParseCandidatePromotionRule(*dtstruct.RuntimeCLIFlags.PromotionRule)
			if err != nil {
				log.Fatale(err)
			}
			err = base.RegisterCandidateInstance(base.WithCurrentTime(dtstruct.NewCandidateDatabaseInstance(*instanceKey, promotionRule)))
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "register-hostname-unresolve", "Instance, meta", `Assigns the given instance a virtual (aka "unresolved") name`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			err := base.RegisterHostnameUnresolve(dtstruct.NewHostnameRegistration(instanceKey, cliParam.HostnameFlag))
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})
	cutil.RegisterCliCommand(commandMap, "deregister-hostname-unresolve", "Instance, meta", `Explicitly deregister/dosassociate a hostname with an "unresolved" name`, func(cliParam *dtstruct.CliParam) {
		{
			instanceKey, _ := base.FigureInstanceKey(cliParam.InstanceKey, thisInstanceKey)
			err := base.RegisterHostnameUnresolve(dtstruct.NewHostnameDeregistration(instanceKey))
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(instanceKey.DisplayString())
		}
	})

	// meta
	cutil.RegisterCliCommand(commandMap, "snapshot-topologies", "Meta", `Take a snapshot of existing topologies.`, func(cliParam *dtstruct.CliParam) {
		{
			err := base.SnapshotTopologies()
			if err != nil {
				log.Fatale(err)
			}
		}
	})

	cutil.RegisterCliCommand(commandMap, "active-nodes", "Meta", `List currently active ham4db nodes`, func(cliParam *dtstruct.CliParam) {
		{
			nodes, err := process.ReadAvailableNodes(false)
			if err != nil {
				log.Fatale(err)
			}
			for _, node := range nodes {
				fmt.Println(node)
			}
		}
	})
	cutil.RegisterCliCommand(commandMap, "access-token", "Meta", `Get a HTTP access token`, func(cliParam *dtstruct.CliParam) {
		{
			publicToken, err := token.GenerateAccessToken(cliParam.Owner)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(publicToken)
		}
	})
	cutil.RegisterCliCommand(commandMap, "reset-hostname-resolve-cache", "Meta", `Clear the hostname resolve cache`, func(cliParam *dtstruct.CliParam) {
		{
			err := base.ResetHostnameResolveCache()
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println("hostname resolve cache cleared")
		}
	})
	cutil.RegisterCliCommand(commandMap, "dump-config", "Meta", `Print out configuration in JSON format`, func(cliParam *dtstruct.CliParam) {
		{
			jsonString := config.Config.ToJSONString()
			fmt.Println(jsonString)
		}
	})
	cutil.RegisterCliCommand(commandMap, "show-resolve-hosts", "Meta", `Show the content of the hostname_resolve table. Generally used for debugging`, func(cliParam *dtstruct.CliParam) {
		{
			resolves, err := base.ReadAllHostnameResolves()
			if err != nil {
				log.Fatale(err)
			}
			for _, r := range resolves {
				fmt.Println(r)
			}
		}
	})

	cutil.RegisterCliCommand(commandMap, "internal-suggest-promoted-replacement", "Internal", `Internal only, used to test promotion logic in CI`, func(cliParam *dtstruct.CliParam) {
		{
			destination := ham.ValidateInstanceIsFound(cliParam.DestinationKey)
			replacement, _, err := ham.SuggestReplacementForPromotedReplica(&dtstruct.TopologyRecovery{}, cliParam.InstanceKey, destination, nil)
			if err != nil {
				log.Fatale(err)
			}
			fmt.Println(replacement.GetInstance().Key.DisplayString())
		}
	})
	// TODO double check
	//cutil.RegisterCliCommand(commandMap, "custom-command", "Agent", "Execute a custom command on the agent as defined in the agent conf", func(cliParam *dtstruct.CliParam) {
	//	{
	//		output, err := agent.CustomCommand(hostnameFlag, pattern)
	//		if err != nil {
	//			log.Fatale(err)
	//		}
	//
	//		fmt.Printf("%v\n", output)
	//	}})

	cutil.RegisterCliCommand(commandMap, "bulk-promotion-rules", "", `Return a list of promotion rules known to ham4db`, func(cliParam *dtstruct.CliParam) {
		{
			promotionRules, err := base.BulkReadCandidateDatabaseInstance()
			if err != nil {
				log.Fatalf("Error: Failed to retrieve promotion rules: %v\n", err)
			}
			var asciiPromotionRules dtstruct.CommandSlice
			for _, v := range promotionRules {
				asciiPromotionRules = append(asciiPromotionRules, v.String())
			}
			sort.Sort(asciiPromotionRules)

			fmt.Printf("%s\n", strings.Join(asciiPromotionRules, "\n"))
		}
	})
}

// GetClusterGhostReplicas returns a list of replicas that can serve as the connected servers
// for a [gh-ost](https://github.com/github/gh-ost) operation. A gh-ost operation prefers to talk
// to a RBR replica that has no children.
func getClusterGhostReplicas(clusterName string) (result []dtstruct.InstanceAdaptor, err error) {
	condition := `
			replication_depth > 0
			and mysql_database_instance.binlog_format = 'ROW'
			and cluster_name = ?
		`
	query := `
		select
			*,
			unix_timestamp() - unix_timestamp(last_checked_timestamp) as seconds_since_last_checked,
			ifnull(last_checked_timestamp <= last_seen_timestamp, 0) as is_last_check_valid,
			unix_timestamp() - unix_timestamp(last_seen_timestamp) as seconds_since_last_seen,
			ham_database_instance_candidate.last_suggested_timestamp is not null
				 and ham_database_instance_candidate.promotion_rule in ('must', 'prefer') as is_candidate,
			ifnull(nullif(ham_database_instance_candidate.promotion_rule, ''), 'neutral') as promotion_rule,
			ifnull(unresolved_hostname, '') as unresolved_hostname,
			(ham_database_instance_downtime.downtime_active is not null and ifnull(ham_database_instance_downtime.end_timestamp, now()) > now()) as is_downtimed,
			ifnull(ham_database_instance_downtime.reason, '') as downtime_reason,
			ifnull(ham_database_instance_downtime.owner, '') as downtime_owner,
			ifnull(unix_timestamp() - unix_timestamp(begin_timestamp), 0) as elapsed_downtime_seconds,
			ifnull(ham_database_instance_downtime.end_timestamp, '') as downtime_end_timestamp
		from
			ham_database_instance
			left join mysql_database_instance using (hostname, port)
			left join ham_database_instance_candidate using (hostname, port)
			left join ham_hostname_unresolved using (hostname)
			left join ham_database_instance_downtime using (hostname, port)
`
	instances, err := ham.ReadInstancesByCondition(query, condition, sqlutil.Args(clusterName), "downstream_count asc")
	if err != nil {
		return result, err
	}

	for _, instance := range instances {
		skipThisHost := false
		if instance.IsReplicaServer() {
			skipThisHost = true
		}
		if !instance.GetInstance().IsLastCheckValid {
			skipThisHost = true
		}
		//	TODO double check
		//if !instance.LogBinEnabled {
		//	skipThisHost = true
		//}
		//if !instance.LogReplicationUpdatesEnabled {
		//	skipThisHost = true
		//}
		if !skipThisHost {
			result = append(result, instance)
		}
	}

	return result, err
}
