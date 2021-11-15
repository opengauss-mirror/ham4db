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
function mysqlNodeModal(node) {
    if (!node) {
        return false;
    }
    if (node.isAggregate) {
        return false;
    }
    nodeModalVisible = true;
    var hiddenZone = $('#node_modal .hidden-zone');
    $('#node_modal #modalDataAttributesTable button[data-btn][data-grouped!=true]').appendTo("#node_modal .modal-footer");
    $('#node_modal #modalDataAttributesTable [data-btn-group]').appendTo("#node_modal .modal-footer");

    $('#node_modal .modal-title').html('<code class="text-primary">' + node.title + "</code>");

    $('#modalDataAttributesTable').html("");

    $('#node_modal button[data-btn=end-maintenance]').hide();
    if (node.inMaintenance) {
        td = addNodeModalDataAttribute("Maintenance", node.maintenanceReason);
        $('#node_modal button[data-btn=end-maintenance]').appendTo(td.find("div")).show();
    }

    if (node.InstanceAlias) {
        addNodeModalDataAttribute("Instance Alias", node.InstanceAlias);
    }
    addNodeModalDataAttribute("Last seen", node.LastSeenTimestamp + " (" + node.SecondsSinceLastSeen.Int64 + "s ago)");
    if (node.UnresolvedHostname) {
        addNodeModalDataAttribute("Unresolved hostname", node.UnresolvedHostname);
    }
    $('#node_modal [data-btn-group=move-equivalent]').appendTo(hiddenZone);
    if (node.UpstreamKey.Hostname) {
        var td = addNodeModalDataAttribute("Master", node.masterTitle);
        if (node.IsDetachedMaster) {
            $('#node_modal button[data-btn=reattach-replica-master-host]').appendTo(td.find("div"));
        } else {
            $('#node_modal button[data-btn=reattach-replica-master-host]').appendTo(hiddenZone);
        }
        $('#node_modal button[data-btn=reset-replica]').appendTo(td.find("div"))

        td = addNodeModalDataAttribute("Replication running", booleanString(node.replicationRunning));
        $('#node_modal button[data-btn=start-replica]').appendTo(td.find("div"))
        $('#node_modal button[data-btn=restart-replica]').appendTo(td.find("div"))
        $('#node_modal button[data-btn=stop-replica]').appendTo(td.find("div"))

        if (!node.replicationRunning) {
            if (node.LastSQLError) {
                td = addNodeModalDataAttribute("Last SQL error", node.LastSQLError);
                $('#node_modal button[data-btn=skip-query]').appendTo(td.find("div"))
            }
            if (node.LastIOError) {
                addNodeModalDataAttribute("Last IO error", node.LastIOError);
            }
        }
        addNodeModalDataAttribute("Seconds behind master", node.SecondsBehindMaster.Valid ? node.SecondsBehindMaster.Int64 : "null");
        addNodeModalDataAttribute("Replication lag", node.ReplicationLagSeconds.Valid ? node.ReplicationLagSeconds.Int64 : "null");
        addNodeModalDataAttribute("SQL delay", node.SQLDelay);

        var masterCoordinatesEl = addNodeModalDataAttribute("Master coordinates", node.ExecBinlogCoordinates.LogFile + ":" + node.ExecBinlogCoordinates.LogPos);
        $('#node_modal [data-btn-group=move-equivalent] ul').empty();
        $.get(appUrl(false, "/api/mysql/master-equivalent/") + node.UpstreamKey.Hostname + "/" + node.UpstreamKey.Port + "/" + node.ExecBinlogCoordinates.LogFile + "/" + node.ExecBinlogCoordinates.LogPos, function (equivalenceResult) {
            if (!equivalenceResult.Detail) {
                return false;
            }
            equivalenceResult.Detail.forEach(function (equivalence) {
                if (equivalence.Key.Hostname == node.Key.Hostname && equivalence.Key.Port == node.Key.Port) {
                    // This very instance; will not move below itself
                    return;
                }
                var title = canonizeInstanceTitle(equivalence.Key.Hostname + ':' + equivalence.Key.Port);
                $('#node_modal [data-btn-group=move-equivalent] ul').append('<li><a href="#" data-btn="move-equivalent" data-hostname="' + equivalence.Key.Hostname + '" data-port="' + equivalence.Key.Port + '">' + title + '</a></li>');
            });

            if ($('#node_modal [data-btn-group=move-equivalent] ul li').length) {
                $('#node_modal [data-btn-group=move-equivalent]').appendTo(masterCoordinatesEl.find("div"));
            }
        }, "json");
        if (node.IsDetached) {
            $('#node_modal button[data-btn=detach-replica]').appendTo(hiddenZone)
            $('#node_modal button[data-btn=reattach-replica]').appendTo(masterCoordinatesEl.find("div"))
        } else {
            $('#node_modal button[data-btn=detach-replica]').appendTo(masterCoordinatesEl.find("div"))
            $('#node_modal button[data-btn=reattach-replica]').appendTo(hiddenZone)
        }
    } else {
        $('#node_modal button[data-btn=reset-replica]').appendTo(hiddenZone);
        $('#node_modal button[data-btn=reattach-replica-master-host]').appendTo(hiddenZone);
        $('#node_modal button[data-btn=skip-query]').appendTo(hiddenZone);
        $('#node_modal button[data-btn=detach-replica]').appendTo(hiddenZone)
        $('#node_modal button[data-btn=reattach-replica]').appendTo(hiddenZone)
    }
    if (node.LogBinEnabled) {
        addNodeModalDataAttribute("Self coordinates", node.SelfBinlogCoordinates.LogFile + ":" + node.SelfBinlogCoordinates.LogPos);
    }
    var td = addNodeModalDataAttribute("Num replicas", node.DownstreamKeyMap != null ? node.DownstreamKeyMap.length : 0);
    $('#node_modal button[data-btn=regroup-replicas]').appendTo(td.find("div"))
    addNodeModalDataAttribute("Server ID", node.ServerID);
    if (node.ServerUUID) {
        addNodeModalDataAttribute("Server UUID", node.ServerUUID);
    }
    addNodeModalDataAttribute("Version", node.Version);
    var td = addNodeModalDataAttribute("Read only", booleanString(node.ReadOnly));
    $('#node_modal button[data-btn=set-read-only]').appendTo(td.find("div"))
    $('#node_modal button[data-btn=set-writeable]').appendTo(td.find("div"))

    // TODO double check
    addNodeModalDataAttribute("Has binary logs", booleanString(node.LogBinEnabled));
    if (node.LogBinEnabled) {
        var format = node.Binlog_format;
        if (format == 'ROW' && node.BinlogRowImage != '') {
            format = format + "/" + node.BinlogRowImage;
        }
        addNodeModalDataAttribute("Binlog format", format);
        var td = addNodeModalDataAttribute("Logs replication updates", booleanString(node.LogReplicationUpdatesEnabled));
        $('#node_modal button[data-btn=take-siblings]').appendTo(td.find("div"))
    }

    $('#node_modal [data-btn-group=gtid-errant-fix]').hide();
    addNodeModalDataAttribute("GTID supported", booleanString(node.supportsGTID));
    if (node.supportsGTID) {
        var td = addNodeModalDataAttribute("GTID based replication", booleanString(node.usingGTID));
        $('#node_modal button[data-btn=enable-gtid]').appendTo(td.find("div"))
        $('#node_modal button[data-btn=disable-gtid]').appendTo(td.find("div"))
        if (node.GTIDMode) {
            addNodeModalDataAttribute("GTID mode", node.GTIDMode);
        }
        if (node.ExecutedGtidSet) {
            addNodeModalDataAttribute("Executed GTID set", node.ExecutedGtidSet);
        }
        if (node.GtidPurged) {
            addNodeModalDataAttribute("GTID purged", node.GtidPurged);
        }
        if (node.GtidErrant) {
            td = addNodeModalDataAttribute("GTID errant", node.GtidErrant);
            $('#node_modal [data-btn-group=gtid-errant-fix]').appendTo(td.find("div"))
            $('#node_modal [data-btn-group=gtid-errant-fix]').show();
        }
    }
    addNodeModalDataAttribute("Semi-sync enforced", booleanString(node.SemiSyncEnforced));

    addNodeModalDataAttribute("Uptime", node.Uptime);
    addNodeModalDataAttribute("Allow TLS", node.AllowTLS);
    addNodeModalDataAttribute("Region", node.Region);
    addNodeModalDataAttribute("Data center", node.DataCenter);
    addNodeModalDataAttribute("Physical environment", node.Environment);
    addNodeModalDataAttribute("Cluster",
        '<a href="' + appUrl(true, '/web/cluster/' + node.ClusterName) + '">' + node.ClusterName + '</a>');
    addNodeModalDataAttribute("Audit",
        '<a href="' + appUrl(true, '/web/audit/instance/' + node.Key.Hostname + '/' + node.Key.Port) + '">' + node.title + '</a>');
    addNodeModalDataAttribute("Agent",
        '<a href="' + appUrl(false, '/web/agent/' + node.Key.Hostname) + '">' + node.Key.Hostname + '</a>');

    $('#node_modal [data-btn]').unbind("click");

    $("#beginDowntimeOwner").val(getUserId());
    $('#node_modal button[data-btn=begin-downtime]').click(function () {
        if (!$("#beginDowntimeOwner").val()) {
            return addModalAlert("You must fill the owner field");
        }
        if (!$("#beginDowntimeReason").val()) {
            return addModalAlert("You must fill the reason field");
        }
        var uri = "/api/begin-downtime/" + node.Key.Hostname + "/" + node.Key.Port + "/" + $("#beginDowntimeOwner").val() + "/" + $("#beginDowntimeReason").val() + "/" + $("#beginDowntimeDuration").val();
        apiGetCommand(true, uri);
    });
    $('#node_modal button[data-btn=refresh-instance]').click(function () {
        apiGetCommand(true, "/api/refresh/" + node.Key.Hostname + "/" + node.Key.Port, "refresh");
    });
    $('#node_modal button[data-btn=skip-query]').click(function () {
        apiGetCommand(true, "/api/skip-query/" + node.Key.Hostname + "/" + node.Key.Port);
    });
    $('#node_modal button[data-btn=start-replica]').click(function () {
        apiGetCommand(true, "/api/start-replica/" + node.Key.Hostname + "/" + node.Key.Port);
    });
    $('#node_modal button[data-btn=restart-replica]').click(function () {
        apiGetCommand(true, "/api/restart-replica/" + node.Key.Hostname + "/" + node.Key.Port);
    });
    $('#node_modal button[data-btn=stop-replica]').click(function () {
        apiGetCommand(true, "/api/stop-replica/" + node.ClusterId + "/" + node.Key.Hostname + "/" + node.Key.Port);
    });
    $('#node_modal button[data-btn=detach-replica]').click(function () {
        apiGetCommand(true, "/api/detach-replica/" + node.Key.Hostname + "/" + node.Key.Port);
    });
    $('#node_modal button[data-btn=reattach-replica]').click(function () {
        apiGetCommand(true, "/api/reattach-replica/" + node.Key.Hostname + "/" + node.Key.Port);
    });
    $('#node_modal button[data-btn=reattach-replica-master-host]').click(function () {
        apiGetCommand(true, "/api/reattach-replica-master-host/" + node.Key.Hostname + "/" + node.Key.Port);
    });
    $('#node_modal button[data-btn=reset-replica]').click(function () {
        var message = "<p>Are you sure you wish to reset <code><strong>" + node.Key.Hostname + ":" + node.Key.Port +
            "</strong></code>?" +
            "<p>This will stop and break the replication." +
            "<p>FYI, this is a destructive operation that cannot be easily reverted";
        bootbox.confirm(message, function (confirm) {
            if (confirm) {
                apiGetCommand(true, "/api/reset-replica/" + node.ClusterId + "/" + node.Key.Hostname + "/" + node.Key.Port);
            }
        });
        return false;
    });
    $('#node_modal [data-btn=gtid-errant-reset-master]').click(function () {
        var message = "<p>Are you sure you wish to reset master on <code><strong>" + node.Key.Hostname + ":" + node.Key.Port +
            "</strong></code>?" +
            "<p>This will purge binary logs on server.";
        bootbox.confirm(message, function (confirm) {
            if (confirm) {
                apiGetCommand(true, "/api/gtid-errant-reset-master/" + node.Key.Hostname + "/" + node.Key.Port);
            }
        });
        return false;
    });
    $('#node_modal [data-btn=gtid-errant-inject-empty]').click(function () {
        var message = "<p>Are you sure you wish to inject empty transactions on the master of this cluster?";
        bootbox.confirm(message, function (confirm) {
            if (confirm) {
                apiGetCommand(true, "/api/gtid-errant-inject-empty/" + node.Key.Hostname + "/" + node.Key.Port);
            }
        });
        return false;
    });
    $('#node_modal button[data-btn=set-read-only]').click(function () {
        apiGetCommand(true, "/api/set-read-only/" + node.Key.Hostname + "/" + node.Key.Port);
    });
    $('#node_modal button[data-btn=set-writeable]').click(function () {
        apiGetCommand(true, "/api/set-writeable/" + node.Key.Hostname + "/" + node.Key.Port);
    });
    $('#node_modal button[data-btn=enable-gtid]').click(function () {
        var message = "<p>Are you sure you wish to enable GTID on <code><strong>" + node.Key.Hostname + ":" + node.Key.Port +
            "</strong></code>?" +
            "<p>Replication <i>might</i> break as consequence";
        bootbox.confirm(message, function (confirm) {
            if (confirm) {
                apiGetCommand(true, "/api/enable-gtid/" + node.Key.Hostname + "/" + node.Key.Port);
            }
        });
    });
    $('#node_modal button[data-btn=disable-gtid]').click(function () {
        var message = "<p>Are you sure you wish to disable GTID on <code><strong>" + node.Key.Hostname + ":" + node.Key.Port +
            "</strong></code>?" +
            "<p>Replication <i>might</i> break as consequence";
        bootbox.confirm(message, function (confirm) {
            if (confirm) {
                apiGetCommand(true, "/api/disable-gtid/" + node.Key.Hostname + "/" + node.Key.Port);
            }
        });
    });
    $('#node_modal button[data-btn=forget-instance]').click(function () {
        var message = "<p>Are you sure you wish to forget <code><strong>" + node.Key.Hostname + ":" + node.Key.Port +
            "</strong></code>?" +
            "<p>It may be re-discovered if accessible from an existing instance through replication topology.";
        bootbox.confirm(message, function (confirm) {
            if (confirm) {
                apiCommand(true, "delete", "/api/forget/" + node.ClusterId + "/" + node.Key.Hostname + "/" + node.Key.Port);
            }
        });
        return false;
    });

    $("body").on("click", "#node_modal a[data-btn=move-equivalent]", function (event) {
        var targetHostname = $(event.target).attr("data-hostname");
        var targetPort = $(event.target).attr("data-port");
        apiGetCommand(true, "/api/move-equivalent/" + node.Key.Hostname + "/" + node.Key.Port + "/" + targetHostname + "/" + targetPort);
    });

    if (node.IsDowntimed) {
        $('#node_modal .end-downtime .panel-heading').html("Downtimed by <strong>" + node.DowntimeOwner + "</strong> until " + node.DowntimeEndTimestamp);
        $('#node_modal .end-downtime .panel-body').html(
            node.DowntimeReason
        );
        $('#node_modal .begin-downtime').hide();
        $('#node_modal button[data-btn=begin-downtime]').hide();

        $('#node_modal .end-downtime').show();
        $('#node_modal button[data-btn=end-downtime]').show();
    } else {
        $('#node_modal .begin-downtime').show();
        $('#node_modal button[data-btn=begin-downtime]').show();

        $('#node_modal .end-downtime').hide();
        $('#node_modal button[data-btn=end-downtime]').hide();
    }
    $('#node_modal button[data-btn=skip-query]').hide();
    $('#node_modal button[data-btn=start-replica]').hide();
    $('#node_modal button[data-btn=restart-replica]').hide();
    $('#node_modal button[data-btn=stop-replica]').hide();

    if (node.UpstreamKey.Hostname) {
        if (node.replicationRunning || node.replicationAttemptingToRun) {
            $('#node_modal button[data-btn=stop-replica]').show();
            $('#node_modal button[data-btn=restart-replica]').show();
        } else if (!node.replicationRunning) {
            $('#node_modal button[data-btn=start-replica]').show();
        }
        if (!node.ReplicationSQLThreadRuning && node.LastSQLError) {
            $('#node_modal button[data-btn=skip-query]').show();
        }
    }

    $('#node_modal button[data-btn=set-read-only]').hide();
    $('#node_modal button[data-btn=set-writeable]').hide();
    if (node.ReadOnly) {
        $('#node_modal button[data-btn=set-writeable]').show();
    } else {
        $('#node_modal button[data-btn=set-read-only]').show();
    }

    $('#node_modal button[data-btn=enable-gtid]').hide();
    $('#node_modal button[data-btn=disable-gtid]').hide();
    if (node.supportsGTID && node.usingGTID) {
        $('#node_modal button[data-btn=disable-gtid]').show();
    } else if (node.supportsGTID) {
        $('#node_modal button[data-btn=enable-gtid]').show();
    }

    $('#node_modal button[data-btn=regroup-replicas]').hide();
    if (node.DownstreamKeyMap != null && node.DownstreamKeyMap.length > 1) {
        $('#node_modal button[data-btn=regroup-replicas]').show();
    }
    $('#node_modal button[data-btn=regroup-replicas]').click(function () {
        var message = "<p>Are you sure you wish to regroup replicas of <code><strong>" + node.Key.Hostname + ":" + node.Key.Port +
            "</strong></code>?" +
            "<p>This will attempt to promote one replica over its siblings";
        bootbox.confirm(message, function (confirm) {
            if (confirm) {
                apiGetCommand(true, "/api/regroup-replicas/" + node.Key.Hostname + "/" + node.Key.Port);
            }
        });
    });

    $('#node_modal button[data-btn=take-siblings]').hide();
    if (node.LogBinEnabled && node.LogReplicationUpdatesEnabled) {
        $('#node_modal button[data-btn=take-siblings]').show();
    }
    $('#node_modal button[data-btn=take-siblings]').click(function () {
        var apiUrl = "/api/take-siblings/" + node.Key.Hostname + "/" + node.Key.Port;
        if (isSilentUI()) {
            apiGetCommand(true, apiUrl);
        } else {
            var message = "<p>Are you sure you want <code><strong>" + node.Key.Hostname + ":" + node.Key.Port +
                "</strong></code> to take its siblings?";
            bootbox.confirm(message, function (confirm) {
                if (confirm) {
                    apiGetCommand(true, apiUrl);
                }
            });
        }
    });
    $('#node_modal button[data-btn=end-downtime]').click(function () {
        apiGetCommand(true, "/api/end-downtime/" + node.Key.Hostname + "/" + node.Key.Port);
    });
    $('#node_modal button[data-btn=recover]').hide();
    if (node.lastCheckInvalidProblem() && node.children && node.children.length > 0) {
        $('#node_modal button[data-btn=recover]').show();
    }
    $('#node_modal button[data-btn=recover]').click(function () {
        apiGetCommand(true, "/api/recover/" + node.Key.Hostname + "/" + node.Key.Port);
    });
    $('#node_modal button[data-btn=end-maintenance]').click(function () {
        apiGetCommand(true, "/api/end-maintenance/" + node.Key.Hostname + "/" + node.Key.Port);
    });

    if (!isAuthorizedForAction()) {
        $('#node_modal button[data-btn]').hide();
        $('#node_modal [data-btn-group]').hide();
    }

    $('#node_modal').modal({})
    $('#node_modal').unbind('hidden.bs.modal');
    $('#node_modal').on('hidden.bs.modal', function () {
        nodeModalVisible = false;
    })
}
