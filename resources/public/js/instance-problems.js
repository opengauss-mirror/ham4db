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

    var problemsURI = "/api/problems";
    if (typeof currentClusterName != "undefined") {
        problemsURI += "/" + currentClusterName();
    }
    $.get(appUrl(true, problemsURI), function (instances) {
        instances = instances || [];
        $.get(appUrl(true, "/api/maintenance"), function (maintenanceList) {
            maintenanceList = maintenanceList || [];
            normalizeInstances(instances, maintenanceList);
            displayProblemInstances(instances);
        }, "json");
    }, "json");

    function displayProblemInstances(instances) {
        hideLoader();

        if (isAnonymized()) {
            $("#instance_problems").remove();
            return;
        }

        function SortByProblemOrder(instance0, instance1) {
            var orderDiff = instance0.problemOrder - instance1.problemOrder;
            if (orderDiff != 0) return orderDiff;
            var orderDiff = instance1.ReplicationLagSeconds.Int64 - instance0.ReplicationLagSeconds.Int64;
            if (orderDiff != 0) return orderDiff;
            orderDiff = instance0.title.localeCompare(instance1.title);
            if (orderDiff != 0) return orderDiff;
            return 0;
        }

        instances.sort(SortByProblemOrder);

        var countProblemInstances = 0;
        instances.forEach(function (instance) {
            var considerInstance = instance.hasProblem
            if (countProblemInstances >= 1000) {
                considerInstance = false;
            }
            if (considerInstance) {
                var li = $("<li/>");
                var instanceEl = Instance.createElement(instance).addClass("instance-problem").appendTo(li);
                $("#instance_problems ul").append(li);

                renderInstanceElement(instanceEl, instance, "problems"); //popoverElement
                instanceEl.click(function () {
                    openNodeModal(instance);
                    return false;
                });
                if (countProblemInstances == 0) {
                    // First problem instance
                    $("#instance_problems_button").addClass("btn-" + instance.renderHint)
                }
                countProblemInstances += 1;
            }
        });
        if (countProblemInstances > 0 && (autoshowProblems() == "true") && ($.cookie("anonymize") != "true")) {
            $("#instance_problems .dropdown-toggle").dropdown('toggle');
        }
        if (countProblemInstances == 0) {
            $("#instance_problems").hide();
        }

        $("div.popover").popover();
        $("div.popover").show();
    }
});
