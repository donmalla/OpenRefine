/*

Copyright 2010, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

 * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
 * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */


$(function() {
	
  var cssLink = $("<link rel='stylesheet' type='text/css' href='extension/ohdfs/styles/flexigrid.css'>");
  $("head").append(cssLink);  
  $.getScript("/extension/ohdfs/styles/flexigrid.js", function(){ });
  
});



function JobStatusDialog() {
  this._createDialog();
  this._signedin = false;
  	
}

JobStatusDialog.prototype._createDialog = function() {
  var self = this;
  var dialog = $(DOM.loadHTML("ohdfs", "scripts/dialogs/jobstatus-loading-dialog.html"));
  this._elmts = DOM.bind(dialog);
  this._elmts.cancelButton.click(function() { DialogSystem.dismissAll();  });

  this._elmts.dialogHeader.text("Hadoop - Job Status");
  this._elmts.cancelButton.text($.i18n._('fb-buttons')["cancel"]);
/*
  this._elmts.cancelButton.click(function() { 
	// DialogSystem.dismiss();
	$('.dialog-container').hide();
  	});
*/	
  this._level = DialogSystem.showDialog(dialog);
  $.get(
      "/command/core/importing-controller?controller=ohdfs/ohdfs-importing-controller&subCommand=hdfs-job-status", 
      null,
      function(data) {
         	var jobs = data["jobs"]["job"];
		var j=0;
		for(var i=0; i<jobs.length; i++)
		{
			j++;
			var job = jobs[i];
			if (job.name=="ApplyJobMapper")
			{
			var vStatus = (j%2==0?"even":"odd");
		        $('#jobStatusTbl > tbody:last').append('<tr class="' + vStatus + '">' +
				'<td><div>' + j + '</div></td>' +
				'<td><div class="data-table-cell-content"> <span>' + job.id + '</span> </div></td>'+
				'<td><div class="data-table-cell-content"> <span>' + job.name + '</span> </div></td>'+
				'<td><div class="data-table-cell-content"> <span>' + job.startTime + '</span> </div></td>'+
				'<td><div class="data-table-cell-content"> <span>' + job.finishTime + '</span> </div></td>'+
				'<td><div class="data-table-cell-content"> <span>' + job.state + '</span> </div></td>'+
				'<td><div class="data-table-cell-content"> <a href="#" onClick="createHive()">Create Hive Table </a> </div></td>'+
			'</tr>');	
		}
		}
		$("#jobStatusTbl th").each(function() {
  			$(this).attr("width", $(this).width());
		});
	
  		$('#jobStatusTbl').flexigrid();
	  },
      "json"
  );
  
  
};  

function createHive() {
	
} 
