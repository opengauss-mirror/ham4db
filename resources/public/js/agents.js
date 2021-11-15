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
    activateRefreshTimer();
    
    $.get(appUrl(false,"/api/agents"), function (agents) {
    	displayAgents(agents);
    }, "json");
    function displayAgents(agents) {
        hideLoader();
        
        agents.forEach(function (agent) {
    		$("#agents").append('<div xmlns="http://www.w3.org/1999/xhtml" class="popover instance right" data-agent-name="'+agent.Hostname+'"><div class="arrow"></div><div class="popover-content"></div></div>');
    		var popoverElement = $("#agents [data-agent-name='" + agent.Hostname + "'].popover");
    		//var title = agent.Hostname;
    		//popoverElement.find("h3 a").html(title);
    	    var contentHtml = ''
    	    	+ '<a href="' + appUrl(false,'/web/agent/'+ agent.Hostname) +'" class="small">'
    	    	+ agent.Hostname
    	    	+ '</a>'
    			;
    	    popoverElement.find(".popover-content").html(contentHtml);
        });     
        
        $("div.popover").popover();
        $("div.popover").show();
	
        if (agents.length == 0) {
        	addAlert("No agents found");
        }
    }
});	
