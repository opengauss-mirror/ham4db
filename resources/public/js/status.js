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
function addPrimaryTableData(name, column1, column2, column3, column4) {
	$(".status-table-primary").append(
    '<tr><td>' + name + '</td>' +
    '<td>' + column1 + '</td>' +
    '<td><code class="text-info">' + column2 + '</code></td>' +
    '<td><code class="text-info">' + column3 + '</code></td>' +
    '<td><code class="text-info long-text">' + column4 + '</code></td></tr>'
	);
}
function addRaftTableData(name, column1, column2) {
	$(".status-table-raft").append(
    '<tr><td>' + name + '</td>' +
    '<td>' + column1 + '</td>' +
    '<td><code class="text-info">' + column2 + '</code></td></tr>'
	);
}
function addStatusActionButton(name, uri) {
	$("#ham4dbStatus .panel-footer").append(
		'<button type="button" class="btn btn-sm btn-info">'+name+'</button> '
	);
	var button = $('#ham4dbStatus .panel-footer button:last');
	button.click(function(){
		apiGetCommand(true, "/api/"+uri);
	});
}

$(document).ready(function () {
	var statusObject = $("#ham4dbStatus .panel-body");
    $.get(appUrl(false, "/api/health/"), function (health) {
    	statusObject.prepend('<h4>'+health.Message+'</h4>')
        $(".status-table-primary").append(
            '<tr><td></td>' +
            '<td><b>Hostname</b></td>' +
            '<td><b>Running Since</b></td>' +
            '<td><b>DB Backend</b></td>' +
            '<td><b>App Version</b></td></tr>'
        );
    	health.Detail.AvailableNodes.forEach(function(node) {
				var app_version = node.AppVersion;
				if (app_version == "") {
					app_version = "unknown version";
				}
				var message = '';
				message += '<code class="text-info"><strong>';
				message += node.Hostname;
				message += '</strong></code>';
				message += '</br>';

				message += '<code class="text-info">';
				if (node.Hostname == health.Detail.ActiveNode.Hostname && node.Token == health.Detail.ActiveNode.Token) {
					message += '<span class="text-success">[Elected at '+health.Detail.ActiveNode.FirstSeenActive+']</span>';
				}
				if (node.Hostname == health.Detail.Hostname) {
					message += '<span class="text-primary">[This node]</span>';
    		}
				message += '</code>';

        var running_since ='<span class="text-info">'+node.FirstSeenActive+'</span>';
				var address = node.DBBackend;

        addPrimaryTableData("Available node", message, running_since, address, app_version);
    	})

    	var userId = getUserId();
    	if (userId == "") {
    		userId = "[unknown]"
    	}
    	var userStatus = (isAuthorizedForAction() ? "admin" : "read only");
      addPrimaryTableData("You", userId + ", " + userStatus, "", "", "");

			if (health.Detail.RaftLeader != "") {
				$(".status-table-raft").append(
            '<tr><td></td>' +
            '<td><b>Advertised</b></td>' +
            '<td><b>URI</b></td>'
        );
				var message = '';
				message += '<code class="text-info"><strong>';
				message += health.Detail.RaftLeader;
				message += '</strong></code>';
				message += '</br>';
				if (health.Detail.IsRaftLeader) {
					message += '<code class="text-info"><span class="text-primary">[This node]</span></code>';
				}
				addRaftTableData("Raft leader", message, '<a href="'+health.Detail.RaftLeaderURI+'">'+health.Detail.RaftLeaderURI+'</a>');
			}
			health.Detail.RaftHealthyMembers = health.Detail.RaftHealthyMembers || []
			if (health.Detail.RaftHealthyMembers) {
				health.Detail.RaftHealthyMembers.sort().forEach(function(node) {
					var message = '';
					message += '<code class="text-info"><strong>';
					message += node;
					message += '</strong></code>';
					message += '</br>';
					if (node == health.Detail.RaftAdvertise) {
						message += '<code class="text-info"><span class="text-primary">[This node]</span></code>';
					}
					addRaftTableData("Healthy raft member", message, "");
				})
			}

    	if (isAuthorizedForAction()) {
    		addStatusActionButton("Reload configuration", "reload-configuration");
    		addStatusActionButton("Reset hostname resolve cache", "reset-hostname-resolve-cache");
    		addStatusActionButton("Reelect", "reelect");
    	}

    }, "json");
});
