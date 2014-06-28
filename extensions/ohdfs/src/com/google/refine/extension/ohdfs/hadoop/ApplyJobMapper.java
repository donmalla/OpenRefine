
package com.google.refine.extension.ohdfs.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;

import com.google.refine.commands.history.ApplyOperationsCommand;
import com.google.refine.history.HistoryProcess;
import com.google.refine.io.FileProjectManager;
import com.google.refine.io.ProjectUtilities;
import com.google.refine.model.Cell;
import com.google.refine.model.Project;
import com.google.refine.model.Row;
import com.google.refine.process.Process;

public class ApplyJobMapper extends Mapper<Object, Text, Text, Text> {
    
    public static final String SEPARATOR_FIELD = new String(new char[] {1}); 
    public static final String SEPARATOR_ARRAY_VALUE = new String(new char[] {2}); 
    
    private Text word = new Text();
    
    @Override
    public void map(Object key, Text value, Context contex)
            throws IOException, InterruptedException {
        // Break line into words for processing
        // StringTokenizer wordList = new StringTokenizer(value.toString());
        Text w2 = new Text();
        
        Configuration conf = contex.getConfiguration();
        
        String jsonString=conf.get("JSON_OP");
        String projectID=conf.get("PROJECT_ID");
        String fileDir =conf.get("PROJECT_PATH");
        try {
            w2.set(process(fileDir,projectID,jsonString,value.toString()));
        } catch (Exception e) {
            // TODO Auto-generated catch block
           e.printStackTrace();
           throw new IOException(e);
        }
        //w2.set(processRow(value.toString()));
        contex.write(null, w2);
    }

    public String processRow(String str) {
        return str + "-AAA";
    }

    public String process(String fileDir, String projectID, String jsonString,String rowData)
            throws Exception {
        Project project;

        // File dir =
        // TestUtils.createTempDirectory("c:\\temp\\openrefine-test-workspace-dir");
        System.out.println("Initializing Project Dir: " + fileDir + " START ");
        File dir = new java.io.File(fileDir);
        // dir.mkdirs();
        FileProjectManager.initialize(dir);
        System.out.println("Initializing Project Dir: " + fileDir + " DONE ");
        
        // System.out.println(FileProjectManager.singleton==null);
        Long pid = Long.parseLong(projectID);
        project = ProjectUtilities.load(new File(dir.toString() + File.separator + pid + ".project"), pid);
        FileProjectManager.singleton.registerProject(project, project.getMetadata());
        Process process = new HistoryProcess(project, 0);
        process.performImmediate();
        //project.processManager.queueProcess(process);
        
        String[] r = rowData.split("\\t");
        Row row = project.rows.get(0);
        for(int i=0; i<r.length; i++)
        {
            com.google.refine.model.Cell cell = row.getCell(i);
            com.google.refine.model.Cell cell2 = new com.google.refine.model.Cell(r[i],cell.recon);
            row.setCell(i, cell2);
        }
        System.out.println("New Row: " + row);
        project.rows = new ArrayList<Row>();
        project.rows.add(row);
        System.out.println(project.rows.size());

        ApplyOperationsCommand aoc = new ApplyOperationsCommand();

        try {
            String processedRow = aoc.process(project, jsonString);
            System.out.println(processedRow);
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        String str="";
        
        StringBuilder hiveRow = new StringBuilder(); 
        List<Cell> cells = row.cells;
        for (int i = 0; i < cells.size(); i++) {
            hiveRow.append(cells.get(i)).append(SEPARATOR_FIELD);
        }
        str = hiveRow.substring(0,hiveRow.length()-1);
        System.out.println(">>>>> " + str);
        return str;
    }
    
    
    public static void main(String[] args) throws Exception {
        String jsonString="[ { \"op\": \"core/text-transform\", \"description\": \"Text transform on cells in column address using expression value.toUppercase()\", \"engineConfig\": { \"facets\": [], \"mode\": \"row-based\" }, \"columnName\": \"address\", \"expression\": \"value.toUppercase()\", \"onError\": \"keep-original\", \"repeat\": false, \"repeatCount\": 10 } ]";
        String projectID="2364863862308";
        String fileDir = "c:\\temp\\ws2";
        String rowData="2,Ratnakar";
        String result = new ApplyJobMapper().process(fileDir,projectID,jsonString,rowData);
        System.out.println("ProcessedRow: " + result);
    }

}
