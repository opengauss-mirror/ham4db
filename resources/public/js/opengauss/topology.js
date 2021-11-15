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
var refreshIntervalSeconds = 60; // seconds
var nodeModalVisible = false;

reloadPageHint = {
    hint: "",
    hostname: "",
    port: ""
}

function opengaussNormalizeInstance(instance) {
    instance.id = getInstanceId(instance.Key.Hostname, instance.Key.Port);
    instance.title = instance.Key.Hostname + ':' + instance.Key.Port;
    instance.canonicalTitle = instance.title;
    instance.masterTitle = instance.UpstreamKey.Hostname + ":" + instance.UpstreamKey.Port;

    masterKey = instance.UpstreamKey;
    instance.masterId = getInstanceId(masterKey.Hostname, masterKey.Port);

    instance.replicationRunning = (instance.ReplicationState === "normal");
    // instance.replicationAttemptingToRun = instance.ReplicationSQLThreadRuning || instance.ReplicationIOThreadRuning;
    instance.replicationLagReasonable = Math.abs(instance.ReplicationLagSeconds.Int64 - instance.SQLDelay) <= 10;
    instance.isSeenRecently = instance.SecondsSinceLastSeen.Valid && instance.SecondsSinceLastSeen.Int64 <= 3600;

    // used by cluster-tree
    instance.children = [];
    instance.parent = null;
    instance.hasMaster = true;
    instance.masterNode = null;
    instance.inMaintenance = false;
    instance.maintenanceReason = "";
    instance.maintenanceEntry = null;
    instance.isFirstChildInDisplay = false

    instance.isMaster = (instance.Role == "Primary");
    instance.isCoMaster = false;
    instance.isCandidateMaster = false;
    instance.isMostAdvancedOfSiblings = false;
    instance.isVirtual = false;
    instance.isAnchor = false;
    instance.isAggregate = false;

    instance.renderHint = "";

    if (instance.LastSQLError == '""') {
        instance.LastSQLError = '';
    }
    if (instance.LastIOError == '""') {
        instance.LastIOError = '';
    }
}

function opengaussNormalizeInstanceProblem(instance) {

    function instanceProblemIfExists(problemName) {
        if (instance.Problems.includes(problemName)) {
            return problemName
        }
        return null;
    }

    instance.inMaintenanceProblem = function () {
        return instanceProblemIfExists('in_maintenance');
    }
    instance.lastCheckInvalidProblem = function () {
        return instanceProblemIfExists('last_check_invalid');
    }
    instance.notRecentlyCheckedProblem = function () {
        return instanceProblemIfExists('not_recently_checked');
    }
    instance.notReplicatingProblem = function () {
        return instanceProblemIfExists('not_replicating');
    }
    instance.replicationLagProblem = function () {
        return instanceProblemIfExists('replication_lag');
    }

    instance.problem = null;
    instance.Problems = instance.Problems || [];
    if (instance.Problems.length > 0) {
        instance.problem = instance.Problems[0]; // highest priority one
    }
    instance.problemOrder = 0;
    if (instance.inMaintenanceProblem()) {
        instance.problemDescription = "This instance is now under maintenance due to some pending operation.\nSee audit page";
        instance.problemOrder = 1;
    } else if (instance.lastCheckInvalidProblem()) {
        instance.problemDescription = "Instance cannot be reached by ham4db.\nIt might be dead or there may be a network problem";
        instance.problemOrder = 2;
    } else if (instance.notRecentlyCheckedProblem()) {
        instance.problemDescription = "Has not made an attempt to reach this instance for a while now.\nThis should generally not happen; consider refreshing or re-discovering this instance";
        instance.problemOrder = 3;
    } else if (instance.notReplicatingProblem()) {
        // check replicas only; where not replicating
        instance.problemDescription = "Replication is not running.\nEither stopped manually or is failing on I/O or SQL error.";
        instance.problemOrder = 4;
    } else if (instance.replicationLagProblem()) {
        instance.problemDescription = "Replica is lagging.\nThis diagnostic is based on either Seconds_behind_master or configured ReplicationLagQuery";
        instance.problemOrder = 5;
    }
    instance.hasProblem = (instance.problem != null);
    instance.hasConnectivityProblem = (!instance.IsLastCheckValid || !instance.IsRecentlyChecked);
}

function opengaussCreateVirtualInstance(id) {
    var virtualInstance = {
        id: id,
        children: [],
        parent: null,
        hasMaster: false,
        inMaintenance: false,
        maintenanceEntry: null,
        isMaster: false,
        isCoMaster: false,
        isVirtual: true,
        ReplicationLagSeconds: 0,
        SecondsSinceLastSeen: 0
    }
    opengaussNormalizeInstanceProblem(virtualInstance);
    return virtualInstance;
}

function opengaussNormalizeInstances(instances, maintenanceList) {
    if (!instances) {
        instances = [];
    }
    instances.forEach(function (instance) {
        opengaussNormalizeInstance(instance);
    });
    // Take canonical host name: strip down longest common suffix of all hosts
    // (experimental; you may not like it)
    var hostNames = instances.map(function (instance) {
        return instance.title
    });
    instances.forEach(function (instance) {
        instance.canonicalTitle = canonizeInstanceTitle(instance.title)
    });
    var instancesMap = {};
    instances.forEach(function (instance) {
        instancesMap[instance.id] = instance;
    });
    // mark maintenance instances
    maintenanceList.forEach(function (maintenanceEntry) {
        var instanceId = getInstanceId(maintenanceEntry.Key.Hostname, maintenanceEntry.Key.Port)
        if (instanceId in instancesMap) {
            instancesMap[instanceId].inMaintenance = true;
            instancesMap[instanceId].maintenanceReason = maintenanceEntry.Reason;
            instancesMap[instanceId].maintenanceEntry = maintenanceEntry;
        }
    });
    instances.forEach(function (instance) {
        // Now that we also know about maintenance
        opengaussNormalizeInstanceProblem(instance);
    });
    // create the tree array
    instances.forEach(function (instance) {
        // add to parent
        var parent = instancesMap[instance.masterId];
        if (parent) {
            instance.parent = parent;
            instance.masterNode = parent;
            // create child array if it doesn't exist
            parent.children.push(instance);
            // (parent.contents || (parent.contents = [])).push(instance);
        } else {
            // parent is null or missing
            instance.hasMaster = false;
            instance.parent = null;
            instance.masterNode = null;
        }
    });

    instances.forEach(function (instance) {
        // if (instance.masterNode != null) {
        //     instance.isSQLThreadCaughtUpWithIOThread = (instance.ExecBinlogCoordinates.LogFile == instance.ReadBinlogCoordinates.LogFile &&
        //         instance.ExecBinlogCoordinates.LogPos == instance.ReadBinlogCoordinates.LogPos);
        // } else {
        //     instance.isSQLThreadCaughtUpWithIOThread = false;
        // }
    });

    instances.forEach(function (instance) {
        if (instance.isMaster && instance.parent != null && instance.parent.parent != null && instance.parent.parent.id == instance.id) {
            // In case there's a master-master setup, introduce a virtual node
            // that is parent of both.
            // This is for visualization purposes...
            var virtualCoMastersRoot = opengaussCreateVirtualInstance();
            coMaster = instance.parent;

            function setAsCoMaster(instance, coMaster) {
                instance.isCoMaster = true;
                instance.hasMaster = true;
                instance.masterId = coMaster.id;
                instance.masterNode = coMaster;

                var index = coMaster.children.indexOf(instance);
                if (index >= 0)
                    coMaster.children.splice(index, 1);

                instance.parent = virtualCoMastersRoot;
                virtualCoMastersRoot.children.push(instance);
            }

            setAsCoMaster(instance, coMaster);
            setAsCoMaster(coMaster, instance);

            instancesMap[virtualCoMastersRoot.id] = virtualCoMastersRoot;
        }
    });
    return instancesMap;
}

function opengaussRenderInstanceElement(popoverElement, instance, renderType) {
    // $(this).find("h3 .pull-left").html(anonymizeInstanceId(instanceId));
    // $(this).find("h3").attr("title", anonymizeInstanceId(instanceId));
    var anonymizedInstanceId = anonymizeInstanceId(instance.id);
    popoverElement.attr("data-nodeid", instance.id);
    var tooltip = "";
    if (isAnonymized()) {
        tooltip = anonymizedInstanceId;
    } else {
        tooltip = (isAliased() ? instance.InstanceAlias : instance.title);
        if (instance.Region) {
            tooltip += "\nRegion: " + instance.Region;
        }
        if (instance.DataCenter) {
            tooltip += "\nData center: " + instance.DataCenter;
        }
        if (instance.Environment) {
            tooltip += "\nEnvironment: " + instance.Environment;
        }
    }
    popoverElement.find("h3").attr('title', tooltip);
    popoverElement.find("h3").html('&nbsp;<div class="pull-left">' +
        (isAnonymized() ? anonymizedInstanceId : isAliased() ? instance.InstanceAlias : instance.canonicalTitle) +
        '</div><div class="pull-right instance-glyphs"><span class="glyphicon glyphicon-cog" title="Open config dialog"></span></div>');
    var indicateLastSeenInStatus = false;

    if (instance.isAggregate) {
        popoverElement.find("h3 div.pull-right span").remove();
        popoverElement.find(".instance-content").append('<div>Instances: <div class="pull-right"></div></div>');

        function addInstancesBadge(count, badgeClass, title) {
            popoverElement.find(".instance-content .pull-right").append('<span class="badge ' + badgeClass + '" data-toggle="tooltip" data-placement="bottom" data-html="true" title="' + title + '">' + count + '</span> ');
            popoverElement.find('[data-toggle="tooltip"]').tooltip();
        }

        var instancesHint = instance.aggregatedProblems[""].join("<br>");
        addInstancesBadge(instance.aggregatedInstances.length, "label-primary", "Aggregated instances<br>" + instancesHint);

        for (var problemType in instance.aggregatedProblems) {
            if (errorMapping[problemType]) {
                var description = errorMapping[problemType]["description"];
                var instancesHint = instance.aggregatedProblems[problemType].join("<br>");
                addInstancesBadge(instance.aggregatedProblems[problemType].length, errorMapping[problemType]["badge"], description + "<br>" + instancesHint);
            }
        }
    }
    if (!instance.isAggregate) {
        if (instance.isFirstChildInDisplay) {
            popoverElement.addClass("first-child-in-display");
            popoverElement.attr("data-first-child-in-display", "true");
        }
        if (!instance.ReadOnly) {
            popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-pencil" title="Writeable"></span> ');
        }
        if (instance.isMostAdvancedOfSiblings) {
            popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-star" title="Most advanced replica"></span> ');
        }
        if (instance.CountMySQLSnapshots > 0) {
            popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-camera" title="' + instance.CountMySQLSnapshots + ' snapshots"></span> ');
        }
        if (instance.HasReplicationFilters) {
            popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-filter" title="Using replication filters"></span> ');
        }
        if (instance.LogBinEnabled && instance.LogReplicationUpdatesEnabled) {
            popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-forward" title="Logs replication updates"></span> ');
        }
        if (instance.IsCandidate) {
            popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-heart" title="Candidate"></span> ');
        }
        if (instance.PromotionRule == "prefer_not") {
            popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-thumbs-down" title="Prefer not promote"></span> ');
        }
        if (instance.PromotionRule == "must_not") {
            popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-ban-circle" title="Must not promote"></span> ');
        }
        if (instance.inMaintenanceProblem()) {
            popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-wrench" title="In maintenance"></span> ');
        }
        if (instance.IsDetached) {
            popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-remove-sign" title="Replication forcibly detached"></span> ');
        }
        if (instance.IsDowntimed) {
            var downtimeMessage = 'Downtimed by ' + instance.DowntimeOwner + ': ' + instance.DowntimeReason + '.\nEnds: ' + instance.DowntimeEndTimestamp;
            popoverElement.find("h3 div.pull-right").prepend('<span class="glyphicon glyphicon-volume-off" title="' + downtimeMessage + '"></span> ');
        }

        if (instance.lastCheckInvalidProblem()) {
            instance.renderHint = "fatal";
            indicateLastSeenInStatus = true;
        } else if (instance.notRecentlyCheckedProblem()) {
            instance.renderHint = "stale";
            indicateLastSeenInStatus = true;
        } else if (instance.notReplicatingProblem()) {
            // check replicas only; check master only if it's co-master where not
            // replicating
            instance.renderHint = "danger";
        } else if (instance.replicationLagProblem()) {
            instance.renderHint = "warning";
        }
        if (instance.renderHint != "") {
            popoverElement.find("h3").addClass("label-" + instance.renderHint);
        }
        var statusMessage = formattedInterval(instance.ReplicationLagSeconds.Int64) + ' lag';
        if (indicateLastSeenInStatus) {
            statusMessage = 'seen ' + formattedInterval(instance.SecondsSinceLastSeen.Int64) + ' ago';
        }
        var identityHtml = '';
        if (isAnonymized()) {
            identityHtml += instance.Version.match(/[^.]+[.][^.]+/);
        } else {
            identityHtml += instance.Version;
        }
        // if (instance.LogBinEnabled) {
        //     var format = instance.Binlog_format;
        //     if (format == 'ROW' && instance.BinlogRowImage != '') {
        //         format = format + "/" + instance.BinlogRowImage.substring(0, 1);
        //     }
        //     identityHtml += " " + format;
        // }
        if (!isAnonymized()) {
            identityHtml += ', ' + instance.FlavorName;
        }

        var contentHtml = '' + '<div class="pull-right">' + statusMessage + ' </div>' + '<p class="instance-basic-info">' + identityHtml + '</p>' + '<p><strong>' + instance.Role + '</strong></p>';
        if (renderType == "search") {
            if (instance.SuggestedClusterAlias) {
                contentHtml += '<p>' + 'Cluster: <a href="' + appUrl(true, '/web/cluster/alias/' + instance.SuggestedClusterAlias) + '">' + instance.SuggestedClusterAlias + '</a>' + '</p>';
            } else {
                contentHtml += '<p>' + 'Cluster: <a href="' + appUrl(true, '/web/cluster/' + instance.ClusterName) + '">' + instance.ClusterName + '</a>' + '</p>';
            }
        }
        if (renderType == "problems") {
            contentHtml += '<p>' + 'Problem: <strong title="' + instance.problemDescription + '">' + instance.problem.replace(/_/g, ' ') + '</strong>' + '</p>';
        }
        popoverElement.find(".instance-content").html(contentHtml);
    }

    popoverElement.find("h3 .instance-glyphs").click(function () {
        openNodeModal(instance);
        return false;
    });
}
