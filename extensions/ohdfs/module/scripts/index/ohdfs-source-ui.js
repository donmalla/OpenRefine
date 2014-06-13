/*

Copyright 2011, Google Inc.
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

Refine.HDFSSourceUI = function(controller) {
  this._controller = controller;
};

Refine.HDFSSourceUI.prototype.attachUI = function(body) {
  this._body = body;
  
  this._body.html(DOM.loadHTML("ohdfs", "scripts/index/import-from-ohdfs-form.html"));
  this._elmts = DOM.bind(this._body);
  
  $('#ohdfs-title').text($.i18n._("ohdfs-import")["title"]);
  $('#ohdfs-import').html($.i18n._("ohdfs-import")["import-by-url"]);
  $('#ohdfs-next').html($.i18n._("ohdfs-import")["next->"]);
  $('#ohdfs-auth-doc').text($.i18n._("ohdfs-import")["auth-doc"]);
  $('#ohdfs-please').text($.i18n._("ohdfs-import")["please"]);
  $('#ohdfs-signin-btn').text($.i18n._("ohdfs-import")["sign-in"]);
  $('#ohdfs-access-data').text($.i18n._("ohdfs-import")["access-data"]);
  $('#ohdfs-retrieving').text($.i18n._("ohdfs-import")["retrieving"]);
  $('#ohdfs-signout').text($.i18n._("ohdfs-import")["sign-out"]);
  $('#ohdfs-resignin').text($.i18n._("ohdfs-import")["re-sign-in"]);
  $('#ohdfs-another-account').text($.i18n._("ohdfs-import")["another-account"]);
  
  var self = this;
  
  self._listDocuments();
  /*
  this._body.find('.ohdfs-signin.button').click(function() {
    GdataExtension.showAuthorizationDialog(
      function() {
        self._listDocuments();
      },
      function() {
        self._body.find('.ohdfs-page').hide();
       // self._elmts.signinPage.show();
      }
    );
  });
  this._body.find('.ohdfs-signout.button').click(function() {
      $.get("command/ohdfs/deauthorize" );
      self._body.find('.ohdfs-page').hide();
      //self._elmts.signinPage.show();
  });
  */
  /*
  this._elmts.urlNextButton.click(function(evt) {
    var url = $.trim(self._elmts.urlInput[0].value);
    if (url.length === 0) {
      window.alert($.i18n._('ohdfs-source')["alert-url"]);
    } else {
      var doc = {};
      doc.docSelfLink = url;
      if (doc.docSelfLink.contains('spreadsheet')) { // TODO: fragile?
        doc.type = 'spreadsheet';
      } else {
        doc.type = 'table';
      }
      self._controller.startImportingDocument(doc);
    }
  });
  */
  
  this._body.find('.ohdfs-page').show();
  this._elmts.progressPage.hide();
  //this._elmts.signinPage.show();
  
  if (GdataExtension.isAuthorized()) {
    this._listDocuments();
  }
};

Refine.HDFSSourceUI.prototype.focus = function() {
};

Refine.HDFSSourceUI.prototype._listDocuments = function() {
  this._elmts.progressPage.hide();	
  this._body.find('.ohdfs-page').show();
  //this._elmts.progressPage.show();
  
  var self = this;
  $.post(
    "command/core/importing-controller?" + $.param({
      "controller": "ohdfs/ohdfs-importing-controller",
      "subCommand": "list-documents"
    }),
    null,
    function(o) {
      self._renderDocuments(o);
    },
    "json"
  );
};

Refine.HDFSSourceUI.prototype._renderDocuments = function(o) {
  var self = this;
  
  this._elmts.listingContainer.empty();
  
  var table = $(
    '<table><tr>' +
      '<th></th>' + // starred
      '<th>'+$.i18n._('ohdfs-source')["type"]+'</th>' +
      '<th>'+$.i18n._('ohdfs-source')["title"]+'</th>' +
      '<th>'+$.i18n._('ohdfs-source')["authors"]+'</th>' +
      '<th>'+$.i18n._('ohdfs-source')["updated"]+'</th>' +
    '</tr></table>'
  ).appendTo(this._elmts.listingContainer)[0];
  
  var renderDocument = function(doc) {

    var tr = table.insertRow(table.rows.length);
    
    var td = tr.insertCell(tr.cells.length);
    if (doc.isStarred) {
      $('<img>').attr('src', 'images/star.png').appendTo(td);
    }
    
    td = tr.insertCell(tr.cells.length);
    $('<span>').text(doc.type).appendTo(td);
    
    td = tr.insertCell(tr.cells.length);
    $('<a>')
    .addClass('ohdfs-doc-title')
    .attr('href', 'javascript:{}')
    .text(doc.title)
    .appendTo(td)
    .click(function(evt) {
      self._controller.startImportingDocument(doc);
    });
    
    $('<a>')
    .addClass('ohdfs-doc-preview')
    .attr('href', doc.docLink)
    .attr('target', '_blank')
    .text('preview')
    .appendTo(td);
    
    td = tr.insertCell(tr.cells.length);
    $('<span>')
    .addClass('ohdfs-doc-authors')
    .text((doc.authors) ? doc.authors.join(', ') : '<unknown>')
    .appendTo(td);
    
    td = tr.insertCell(tr.cells.length);
    $('<span>')
    .addClass('ohdfs-doc-date')
    .text('<unknown>')
    .attr('title', (doc.updated) ? doc.updated : '<unknown>')
    .appendTo(td);
  };
  
  var docs = o.documents;
  $.each(docs, function() {
    this.updatedDate = (this.updated) ? new Date(this.updated) : null;
    this.updatedDateTime = (this.updatedDate) ? this.updatedDate.getTime() : 0;
  });
  docs.sort(function(a, b) { return b.updatedDateTime -  a.updatedDateTime; });
  
  for (var i = 0; i < docs.length; i++) {
    renderDocument(docs[i]);
  }
  
  this._body.find('.ohdfs-page').hide();
  this._elmts.listingPage.show();
};
