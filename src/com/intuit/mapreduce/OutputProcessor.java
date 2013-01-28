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

/*
 * Main class running mapreduce job for aggregate analysis of output.
 */
public class OutputProcessor extends Configured implements Tool { 
  
    public int run(String[] args) throws Exception {
		 
        Configuration conf = new Configuration();
	 
        Job job = new Job(conf, "Aggregate analysis of output");    

        job.setJarByClass(OutputProcessor.class);    
	   
        job.setOutputKeyClass(Text.class);	   
        job.setOutputValueClass(Text.class);
	   
        job.setMapperClass(OutputProcessorMapper.class);  
        // job.setCombinerClass(Reduce.class);
        job.setReducerClass(OutputProcessorReducer.class);
	   
        //Setting reduce tasks to 1
        job.setNumReduceTasks(1);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);  

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  
        job.waitForCompletion(true);
	  
        return 0;
    }
	
    public static void main(String[] args) throws Exception {
       
        int res = ToolRunner.run(new Configuration(), new OutputProcessor(), args);

        System.exit(res);
    
    }
}
