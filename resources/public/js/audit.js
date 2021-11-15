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
    var apiUri = "/api/audit/"+currentPage();
    if (auditHostname()) {
    	apiUri = "/api/audit/instance/"+auditHostname()+"/"+auditPort()+"/"+currentPage();
    }
    $.get(appUrl(true, apiUri), function (auditEntries) {
      auditEntries = auditEntries || [];
      displayAudit(auditEntries);
  	}, "json");
    function displayAudit(auditEntries) {
    	var baseWebUri = appUrl(true, "/web/audit/");
    	if (auditHostname()) {
    		baseWebUri += "instance/"+auditHostname()+"/"+auditPort()+"/";
        }
        hideLoader();
        auditEntries.forEach(function (audit) {
      		var row = jQuery('<tr/>');
      		jQuery('<td/>', { text: audit.AuditTimestamp }).appendTo(row);
      		jQuery('<td/>', { text: audit.AuditType }).appendTo(row);
      		if (audit.AuditInstanceKey.Hostname) {
      			var uri = appUrl(true, "/web/audit/instance/"+audit.AuditInstanceKey.Hostname+"/"+audit.AuditInstanceKey.Port);
      			$('<a/>',  { text: audit.AuditInstanceKey.Hostname+":"+audit.AuditInstanceKey.Port , href: uri}).wrap($("<td/>")).parent().appendTo(row);
      		} else {
      			jQuery('<td/>', { text: audit.AuditInstanceKey.Hostname+":"+audit.AuditInstanceKey.Port }).appendTo(row);
      		}
      		jQuery('<td/>', { text: audit.Message }).appendTo(row);
      		row.appendTo('#audit tbody');
      	});
        if (currentPage() <= 0) {
        	$("#audit .pager .previous").addClass("disabled");
        }
        if (auditEntries.length == 0) {
        	$("#audit .pager .next").addClass("disabled");
        }
        $("#audit .pager .previous").not(".disabled").find("a").click(function() {
            window.location.href = baseWebUri+(currentPage() - 1);
        });
        $("#audit .pager .next").not(".disabled").find("a").click(function() {
            window.location.href = baseWebUri+(currentPage() + 1);
        });
        $("#audit .pager .disabled a").click(function() {
            return false;
        });
    }
});
