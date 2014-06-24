
package com.google.refine.extension.ohdfs.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;

import com.google.refine.commands.history.ApplyOperationsCommand;
import com.google.refine.io.FileProjectManager;
import com.google.refine.io.ProjectUtilities;
import com.google.refine.model.Project;
import com.google.refine.model.Row;

public class ApplyJobMapper extends Mapper<Object, Text, Text, Text> {

    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context contex)
            throws IOException, InterruptedException {
        // Break line into words for processing
        // StringTokenizer wordList = new StringTokenizer(value.toString());
        Text w2 = new Text();
        String jsonString="[ { \"op\": \"core/text-transform\", "
                + "\"description\": \"Text transform on cells in column address using expression value.toUpperCase()\", \"engineConfig\": { \"facets\": [], \"mode\": \"row-based\" }, \"columnName\": \"address\", \"expression\": \"value.toUppercase()\", \"onError\": \"keep-original\", \"repeat\": false, \"repeatCount\": 10 } ]";
        String projectID="2364863862308";
        String fileDir = "/tmp/ws2";
        try {
            w2.set(process(fileDir,projectID,jsonString,value.toString()));
        } catch (JSONException e) {
            // TODO Auto-generated catch block
           e.printStackTrace();
           throw new IOException(e);
        }
        //w2.set(processRow(value.toString()));
        contex.write(word, w2);

    }

    public String processRow(String str) {
        return str + "-AAA";
    }

    public String process(String fileDir, String projectID, String jsonString,String rowData)
            throws JSONException, IOException {
        Project project;

        // File dir =
        // TestUtils.createTempDirectory("c:\\temp\\openrefine-test-workspace-dir");
        File dir = new java.io.File(fileDir);
        // dir.mkdirs();
        FileProjectManager.initialize(dir);

        // System.out.println(FileProjectManager.singleton==null);
        Long pid = Long.parseLong(projectID);
        project = ProjectUtilities.load(new File(dir.toString() + File.separator + pid + ".project"), pid);
        FileProjectManager.singleton.registerProject(project, project.getMetadata());
        
       
        String[] r = rowData.split(",");
        Row row = project.rows.get(0);
        for(int i=0; i<r.length; i++)
        {
            com.google.refine.model.Cell cell = row.getCell(i);
            com.google.refine.model.Cell cell2 = new com.google.refine.model.Cell(r[i],cell.recon);
            row.setCell(i, cell2);
        }
        System.out.println("New Row: " + project.rows);
        System.out.println(project.rows.size());

        ApplyOperationsCommand aoc = new ApplyOperationsCommand();

        try {
            String processedRow = aoc.process(project, jsonString);
            System.out.println(processedRow);
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        List<Row> list = project.rows;
        String str="";
        for (int i = 0; i < list.size(); i++) {
            str += list.get(i) + ",";
           // System.out.println(list.get(i));
        }
        
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
