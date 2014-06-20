/*
 * Copyright (c) 2010, Thomas F. Morris
 *        All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions are met:
 * - Redistributions of source code must retain the above copyright notice, this 
 *   list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution.
 * 
 * Neither the name of Google nor the names of its contributors may be used to 
 * endorse or promote products derived from this software without specific 
 * prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR 
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR 
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; 
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR 
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF 
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.google.refine.extension.ohdfs;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;






import com.google.refine.ProjectMetadata;
import com.google.refine.importers.TabularImportingParserBase;
import com.google.refine.importers.TabularImportingParserBase.TableDataReader;
import com.google.refine.importing.ImportingJob;
import com.google.refine.model.Project;
import com.google.refine.util.JSONUtilities;

/**
 * OpenRefine parser for Google Spreadsheets.
 * 
 * @author Tom Morris <tfmorris@gmail.com>
 * @copyright 2010 Thomas F. Morris
 * @license New BSD http://www.opensource.org/licenses/bsd-license.php
 */
public class HiveDataImporter {
    static public void parse(
        String token,
        Project project,
        ProjectMetadata metadata,
        final ImportingJob job,
        int limit,
        JSONObject options,
        List<Exception> exceptions) {
    
        String docType = JSONUtilities.getString(options, "docType", null);
        parse(  project,
                metadata,
                job,
                limit,
                options,
                exceptions
            );
    }
    
    static public void parse(
        Project project,
        ProjectMetadata metadata,
        final ImportingJob job,
        int limit,
        JSONObject options,
        List<Exception> exceptions) {
        
        String docUrlString = JSONUtilities.getString(options, "docUrl", null);
        HiveService _hiveService = new HiveService(docUrlString);    
                parseOneWorkSheet(
                    project,
                    metadata,
                    job,
                    docUrlString,
                    limit,
                    options,
                    exceptions,
                    _hiveService);
            
        }
    
    
    static public void parseOneWorkSheet(
        Project project,
        ProjectMetadata metadata,
        final ImportingJob job,
        String docURL,
        int limit,
        JSONObject options,
        List<Exception> exceptions,
        HiveService _hiveService
        ) {
        
        
            String spreadsheetName = docURL;
            String fileSource = spreadsheetName;
            
            setProgress(job, fileSource, 0);
            TabularImportingParserBase.readTable(
                project,
                metadata,
                job,
                new HiveBatchRowReader(job, fileSource, 20,_hiveService),
                fileSource,
                limit,
                options,
                exceptions
            );
            setProgress(job, fileSource, 100);
       
    }
    
    static private void setProgress(ImportingJob job, String fileSource, int percent) {
        job.setProgress(percent, "Reading " + fileSource);
    }
    
    static private class HiveBatchRowReader implements TableDataReader {
        final ImportingJob job;
        final String fileSource;
        final int batchSize;
        final int totalRows;
        HiveService hiveService;
        
        int nextRow = 0; // 0-based
        int batchRowStart = 0; // 0-based
        List<List<Object>> rowsOfCells = null;
        
        public HiveBatchRowReader(ImportingJob job, String fileSource,int batchSize,
                HiveService _hiveService) {
            this.job = job;
            this.fileSource = fileSource;
            this.batchSize = batchSize;
            this.totalRows = new HiveService(fileSource).getRowCount(); // worksheet.getRowCount();
            hiveService = _hiveService;
        }
        
        @Override
        public List<Object> getNextRowOfCells() throws IOException {
            if (rowsOfCells == null || (nextRow >= batchRowStart + rowsOfCells.size() && nextRow < totalRows)) {
                int newBatchRowStart = batchRowStart + (rowsOfCells == null ? 0 : rowsOfCells.size());
                    try
                    {
                    rowsOfCells = getRowsOfCells(
                        hiveService,    
                        newBatchRowStart + 1, // convert to 1-based
                        batchSize);
                    
                    batchRowStart = newBatchRowStart;
                    
                    setProgress(job, fileSource, batchRowStart * 100 / totalRows);
                    }catch(Exception e) {
                        throw new IOException(e);
                    }
            }
            
            if (rowsOfCells != null && nextRow - batchRowStart < rowsOfCells.size()) {
                return rowsOfCells.get(nextRow++ - batchRowStart);
            } else {
                return null;
            }
        }
        
        
        List<List<Object>> getRowsOfCells(
            HiveService hiveService,    
            int startRow, // 1-based
            int rowCount
        ) throws IOException, Exception {
            
            
            List<List<Object>> rowsOfCells = new ArrayList<List<Object>>();
            HiveDBConnection hdb = new HiveDBConnection();
            Connection con = hdb.getConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select * from CA_NORTH_DISCHARGES_2007_STG limit 50");
            ResultSetMetaData rsMeta = rs.getMetaData();
            rowsOfCells.add(new ArrayList<Object>());
            List<Object> headerRow= rowsOfCells.get(0);
            
            int minRow = startRow;
            int maxRow = Math.min(hiveService.getRowCount(), startRow + rowCount - 1);
            int cols = rsMeta.getColumnCount();
            int rows = 50; //hiveService.getRowCount();
            
            for(int j=1; j<cols; j++)
            {
                headerRow.add(rsMeta.getColumnName(j));
            }
            
            int i=1;
            while (rs.next()) {
                rowsOfCells.add(new ArrayList<Object>());
                List<Object> row = rowsOfCells.get(i);
                for(int j=1; j<cols; j++)
                {
                    row.add(rs.getString(j));
                }
                i++;
                //rowsOfCells.add(row);
            }
            con.close();
            return rowsOfCells;
        }
    }
}
    
