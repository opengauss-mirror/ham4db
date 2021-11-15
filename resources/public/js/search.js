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
	$("#searchInput").val(currentSearchString());
  showLoader();
  $.get(appUrl(true, "/api/search/"+currentSearchString()), function (instances) {
		instances = instances || [];
    $.get(appUrl(true, "/api/maintenance"), function (maintenanceList) {
			maintenanceList = maintenanceList || [];
  		normalizeInstances(instances, maintenanceList);
      displaySearchInstances(instances);
    }, "json");
  }, "json");
  function displaySearchInstances(instances) {
    hideLoader();
  	instances.forEach(function (instance) {
      var instanceEl = Instance.createElement(instance).addClass("instance-search").appendTo("#searchResults");
  		renderInstanceElement(instanceEl, instance, "search");
	    instanceEl.find("h3").click(function () {
	    	openNodeModal(instance);
	    	return false;
	    });
		});

    if (instances.length == 0) {
    	addAlert("No search results found for "+currentSearchString());
    }
	}
});
