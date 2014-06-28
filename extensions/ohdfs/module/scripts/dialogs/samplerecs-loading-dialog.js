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

function SampleRecordsLoadingDialog() {
  this._createDialog();
  this._signedin = false;
}

SampleRecordsLoadingDialog.prototype._createDialog = function() {
  var self = this;
  var dialog = $(DOM.loadHTML("ohdfs", "scripts/dialogs/samplerecs-loading-dialog.html"));
  this._elmts = DOM.bind(dialog);

  this._elmts.dialogHeader.text("Sampling Setup");
  this._elmts.cancelButton.text($.i18n._('fb-buttons')["cancel"]);
  this._elmts.okButton.text("Submit Sampling Request");
  
  this._elmts.sampleTypes.change(function() {
	  if ($('#sampleTypes').val()=="SBT")
		  {
		  		$('#row1').show();
		  		$('#row2').show();
		  		$('#row3').hide();
		  		
		  }else 
		  {
			    $('#row1').hide();
		  		$('#row2').hide();
		  		$('#row3').show();
		  }
  });
  
  this._elmts.cancelButton.click(function() {
	//SampleRecordsLoadingDialog.dismiss();	
	DialogSystem.dismissAll();

  });
 
  this._elmts.okButton.click(function() { 
	  // Submit Sampling Job.
	  $.post(
		    "command/core/importing-controller?" + $.param({
		      "controller": "ohdfs/ohdfs-importing-controller",
		      "subCommand": "submit-sampling-job"
		    }) + "&" + $('#form1').serialize(),
		    null,
		    function(o) {
		      alert("Your Job has been submitted");
		    },
		    "json"
		  );
  });
  
  this._level = DialogSystem.showDialog(dialog);
  
};  


