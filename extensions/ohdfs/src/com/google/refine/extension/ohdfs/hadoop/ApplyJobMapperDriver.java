package com.google.refine.extension.ohdfs.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ApplyJobMapperDriver {

    public static Job runJob(String inputPath,String outputPath,
            String jsonOp,String projectID,String projectPath) 
            throws IllegalArgumentException, IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:8020/");   
        conf.set("yarn.nodemanager.aux-services","mapreduce_shuffle");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "127.0.0.1:8032");
        conf.set("yarn.resourcemanager.address", "127.0.0.1:8032");
        conf.set("mapreduce.jobhistory.address", "127.0.0.1:10020");
        conf.set("mapreduce.jobhistory.webapp.address", "127.0.0.1:19888");
        
        
        /*
        conf.set("yarn.resourcemanager.hostname", "127.0.0.1:8032");
        conf.set("yarn.resourcemanager.address", "127.0.0.1:8032");
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:8020/");        
        
        conf.set("mapreduce.jobhistory.address", "127.0.0.1:10020");
        conf.set("mapreduce.jobhistory.webapp.address", "127.0.0.1:19888");
        
        conf.set("yarn.nodemanager.aux-services","mapreduce_shuffle");
        conf.set("mapreduce.framework.name", "yarn");
        
        conf.set("mapreduce.jobhistory.address", "localhost.localdomain:10020");
        conf.set("mapreduce.jobhistory.admin.address","0.0.0.0:10033");
        conf.set("mapreduce.jobhistory.webapp.address","localhost.localdomain:19888");
        
        conf.set("mapreduce.jobtracker.persist.jobstatus.active","true");
        conf.set("mapreduce.jobtracker.persist.jobstatus.dir","/jobtracker/jobsInfo");
        */
        conf.set("JSON_OP", jsonOp);
        conf.set("PROJECT_ID", projectID);
        conf.set("PROJECT_PATH", projectPath);
        conf.set("FILE_SEPERATOR", "\\t");
        
        Job job = Job.getInstance(conf, "ApplyJobMapper");
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(ApplyJobMapper.class); 
        job.setNumReduceTasks(0);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
 
        job.setJarByClass(ApplyJobMapperDriver.class);
 
        job.submit();
        
        return job;
 
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
          System.out.println("usage: [input] [output] [openRefineDir] [jsonOp] [projectID]");
          System.exit(-1);
        }
        // String jsonOp="[ { \"op\": \"core/column-addition\", \"description\": \"Create column UcaseText at index 2 based on column text using expression grel:value.toUppercase()\", \"engineConfig\": { \"facets\": [], \"mode\": \"row-based\" }, \"newColumnName\": \"UcaseText\", \"columnInsertIndex\": 2, \"baseColumnName\": \"text\", \"expression\": \"grel:value.toUppercase()\", \"onError\": \"set-to-blank\" } ]";
        
        String fileDir = args[2];  //"/home/ratnakar/.local/share/openrefine";
        String jsonOp= new String(org.apache.commons.codec.binary.Base64.decodeBase64(args[3]));
        String projectID=args[4]; //"1699415687416";
        Job job = runJob(args[0],args[1],jsonOp,projectID,fileDir);
        System.out.println("********* job.getTrackingURL: " + job.getTrackingURL());
 }
    
    

}
