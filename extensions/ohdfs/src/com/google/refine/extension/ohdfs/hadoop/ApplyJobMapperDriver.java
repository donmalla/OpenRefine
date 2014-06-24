package com.google.refine.extension.ohdfs.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ApplyJobMapperDriver {

    static {
        File workaround = new File(".");
        System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
        new File("./bin").mkdirs();
        File f = new File("./bin/winutils.exe");
        if (!f.exists())
        {
            try {
                f.createNewFile();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
          System.out.println("usage: [input] [output]");
          System.exit(-1);
        }
   
   
        Job job = Job.getInstance(new Configuration());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(ApplyJobMapper.class); 
       // job.setReducerClass(ApplyJobReducer.class);  
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        job.setJarByClass(ApplyJobMapperDriver.class);
 
        job.submit();
         
         
   
 }
    
    

}
