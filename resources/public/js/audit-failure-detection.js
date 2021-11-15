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
$(document).ready(function () {
    showLoader();
    var apiUri = "/api/audit-failure-detection/" + currentPage();
    if (clusterAlias() != "") {
        apiUri = "/api/audit-failure-detection/alias/" + clusterAlias() + "/" + currentPage();
    } else if (detectionId() > 0) {
        apiUri = "/api/audit-failure-detection/id/" + detectionId();
    }
    $.get(appUrl(true, apiUri), function (auditEntries) {
        auditEntries = auditEntries || [];
        $.get(appUrl(true, "/api/replication-analysis-changelog"), function (analysisChangelog) {
            analysisChangelog = analysisChangelog || [];
            displayAudit(auditEntries, analysisChangelog);
        }, "json");
    }, "json");

    function displayAudit(auditEntries, analysisChangelog) {
        var baseWebUri = appUrl(true, "/web/audit-failure-detection/");
        if (clusterAlias()) {
            baseWebUri += "alias/" + clusterAlias() + "/";
        }
        var changelogMap = {}
        analysisChangelog.forEach(function (changelogEntry) {
            changelogMap[getInstanceId(changelogEntry.AnalyzedInstanceKey.Hostname, changelogEntry.AnalyzedInstanceKey.Port)] = changelogEntry.Changelog;
        });

        hideLoader();
        auditEntries.forEach(function (audit) {
            var analyzedInstanceDisplay = audit.AnalysisEntry.AnalyzedInstanceKey.Hostname + ":" + audit.AnalysisEntry.AnalyzedInstanceKey.Port;
            var row = $('<tr/>');
            var analysisElement = $('<a class="more-detection-info"/>').attr("data-detection-id", audit.Id).text(audit.AnalysisEntry.Analysis);

            $('<td/>').prepend(analysisElement).appendTo(row);
            $('<a/>', {
                text: analyzedInstanceDisplay,
                href: appUrl(true, "/web/search/" + analyzedInstanceDisplay)
            }).wrap($("<td/>")).parent().appendTo(row);
            $('<td/>', {
                text: audit.AnalysisEntry.CountReplicas
            }).appendTo(row);
            $('<a/>', {
                text: audit.AnalysisEntry.ClusterDetails.ClusterName,
                href: appUrl(true, "/web/cluster/" + audit.AnalysisEntry.ClusterDetails.ClusterName)
            }).wrap($("<td/>")).parent().appendTo(row);
            $('<a/>', {
                text: audit.AnalysisEntry.ClusterDetails.ClusterAlias,
                href: appUrl(true, "/web/cluster/alias/" + audit.AnalysisEntry.ClusterDetails.ClusterAlias)
            }).wrap($("<td/>")).parent().appendTo(row);
            $('<td/>', {
                text: audit.RecoveryStartTimestamp
            }).appendTo(row);

            var moreInfo = "";
            moreInfo += '<div>Detected: ' + audit.RecoveryStartTimestamp + '</div>';
            if (audit.AnalysisEntry.DownstreamKeyMap != null && audit.AnalysisEntry.DownstreamKeyMap.length > 0) {
                moreInfo += '<div>' + audit.AnalysisEntry.CountReplicas + ' replicating hosts :<ul>';
                audit.AnalysisEntry.DownstreamKeyMap.forEach(function (instanceKey) {
                    moreInfo += "<li><code>" + getInstanceTitle(instanceKey.Hostname, instanceKey.Port) + "</code></li>";
                });
                moreInfo += "</ul></div>";
            }
            var changelog = changelogMap[getInstanceId(audit.AnalysisEntry.AnalyzedInstanceKey.Hostname, audit.AnalysisEntry.AnalyzedInstanceKey.Port)];
            if (changelog) {
                moreInfo += '<div>Changelog :<ul>';
                changelog.reverse().forEach(function (changelogEntry) {
                    var changelogEntryTokens = changelogEntry.split(';');
                    var changelogEntryTimestamp = changelogEntryTokens[0];
                    var changelogEntryAnalysis = changelogEntryTokens[1];

                    if (changelogEntryTimestamp > audit.RecoveryStartTimestamp) {
                        // This entry is newer than the detection time; irrelevant
                        return;
                    }
                    moreInfo += "<li><code>" + changelogEntryTimestamp + " <strong>" + changelogEntryAnalysis + "</strong></code></li>";
                });
                moreInfo += "</ul></div>";
            }
            moreInfo += '<div><a href="' + appUrl(true, '/web/audit-recovery/id/' + audit.RelatedRecoveryId) + '">Related recovery</a></div>';

            moreInfo += "<div>Processed by <code>" + audit.ProcessingNodeHostname + "</code></div>";
            row.appendTo('#audit tbody');

            var row = $('<tr/>');
            row.attr("data-detection-id-more-info", audit.Id);
            row.addClass("more-info");
            $('<td colspan="6"/>').append(moreInfo).appendTo(row);
            row.hide().appendTo('#audit tbody');
        });
        if (auditEntries.length == 1) {
            $("[data-detection-id-more-info]").show();
        }
        if (currentPage() <= 0) {
            $("#audit .pager .previous").addClass("disabled");
        }
        if (auditEntries.length == 0) {
            $("#audit .pager .next").addClass("disabled");
        }
        $("#audit .pager .previous").not(".disabled").find("a").click(function () {
            window.location.href = appUrl(baseWebUri + (currentPage() - 1));
        });
        $("#audit .pager .next").not(".disabled").find("a").click(function () {
            window.location.href = appUrl(baseWebUri + (currentPage() + 1));
        });
        $("#audit .pager .disabled a").click(function () {
            return false;
        });
        $("body").on("click", ".more-detection-info", function (event) {
            var detectionId = $(event.target).attr("data-detection-id");
            $('[data-detection-id-more-info=' + detectionId + ']').slideToggle();
        });
    }
});
