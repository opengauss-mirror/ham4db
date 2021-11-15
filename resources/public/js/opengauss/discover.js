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
    $('button[data-btn=discover-instance]').unbind("click");
    $('button[data-btn=discover-instance]').click(function () {

        if (!$("#discoverHostName").val()) {
            return addAlert("You must enter a host name");
        }
        if (!$("#discoverPort").val()) {
            return addAlert("You must enter a port");
        }
        discover($("#discoverHostName").val(), $("#discoverPort").val())
        return false;
    });
    $("#discoverHostName").focus();
});

function discover(hostname, port) {
    showLoader();
    var uri = "/api/discover/" + hostname + "/" + port;
    $.get(appUrl(true, uri), function (operationResult) {
        hideLoader();
        if (operationResult.Code === "ERROR" || operationResult.Detail == null) {
            addAlert(operationResult.Message)
        } else {
            var instance = operationResult.Detail;
            addInfo('Discovered <a href="' + appUrl(true, '/web/search?s=' + instance.Key.Hostname + ":" + instance.Key.Port) + '" class="alert-link">'
                + instance.Key.Hostname + ":" + instance.Key.Port + '</a>'
            );
        }
    }, "json");
}