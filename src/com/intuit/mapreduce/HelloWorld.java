package com.intuit.mapreduce;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class HelloWorld extends Configured implements Tool { 
  
    public int run(String[] args) throws Exception {
		 
        Configuration conf = new Configuration();
	 
        Job job = new Job(conf, "WordCount example for hadoop 0.20.1");    

        job.setJarByClass(HelloWorld.class);    
	   
        job.setOutputKeyClass(Text.class);	   
        job.setOutputValueClass(Text.class);
	   
        job.setMapperClass(Map.class);  
        // job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
	   

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);  

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  
        job.waitForCompletion(true);
	  
        return 0;
    }
	
    public static void main(String[] args) throws Exception {
       
        int res = ToolRunner.run(new Configuration(), new HelloWorld(), args);

        System.exit(res);
    
    }
}
