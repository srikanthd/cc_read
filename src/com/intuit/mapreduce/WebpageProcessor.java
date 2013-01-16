package com.intuit.mapreduce;

// Java classes
import java.lang.IllegalArgumentException;
import java.lang.Integer;
import java.lang.Math;
import java.lang.OutOfMemoryError;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// log4j classes
import org.apache.log4j.Logger;

// Hadoop classes
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Common Crawl classes
import org.commoncrawl.hadoop.mapred.ArcInputFormat;
import org.commoncrawl.hadoop.mapred.ArcRecord;

// jsoup classes
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Main class to run the mapreduce job for processing the arc.gz files as "Arc Records"
 */
public class WebpageProcessor
    extends    Configured
    implements Tool {

  private static final Logger LOG = Logger.getLogger(WebpageProcessor.class);


 
  /**
   * Hadoop FileSystem PathFilter for ARC files, allowing users to limit the
   * number of files processed.
   *
   * @author Chris Stephens <chris@commoncrawl.org>
   */
  public static class SampleFilter
      implements PathFilter {

    private static int count =         0;
    private static int max   = 999999999;

    public boolean accept(Path path) {
	
      if (!path.getName().endsWith(".arc.gz"))
        return false;

      SampleFilter.count++;

      if (SampleFilter.count > SampleFilter.max)
        return false;
	
      return true;
    }
  }

  
  
  /**
   * Implmentation of Tool.run() method, which builds and runs the Hadoop job.
   *
   * @param  args command line parameters, less common Hadoop job parameters stripped
   *              out and interpreted by the Tool class.  
   * @return      0 if the Hadoop job completes successfully, 1 if not. 
   */
  
  @Override
  public int run(String[] args)
      throws Exception {

    String outputPath = null;
    String configFile = null;

    // Read the command line arguments.
    if (args.length <  1)
      throw new IllegalArgumentException("Example JAR must be passed an output path.");

    outputPath = args[0];

   
    // Read in any additional config parameters.
    if (configFile != null) {
      LOG.info("adding config parameters from '"+ configFile + "'");
      this.getConf().addResource(configFile);
    }

    
    // Creates a new job configuration for this Hadoop job.
    JobConf job = new JobConf(this.getConf());
    
    //Set configuration parameters in the JobConf.
    job.setMaxMapTaskFailuresPercent(50);
    long MAX_JOBCONF_SIZE = 10*1024*1024L;
    job.setLong(JobTracker.MAX_USER_JOBCONF_SIZE_KEY, MAX_JOBCONF_SIZE);
    
    
    //Set the main class in the Jar file.
    job.setJarByClass(WebpageProcessor.class);

    //Read the input file location on S3 which has the list of arc.gz files to be processed.
    String s3InputFile = args[1];
    Path s3InputFilePath = new Path(s3InputFile);
    FileSystem fs1 = FileSystem.get(new URI(s3InputFile),job);
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs1.open(s3InputFilePath)));
    
    
    String line= "";
    
   
    LOG.info("Adding input paths");

    while((line=reader.readLine())!=null)
    {
    	FileInputFormat.addInputPath(job, new Path(line));
    }
   
    reader.close();
    
    
    FileInputFormat.setInputPathFilter(job, SampleFilter.class);

    FileSystem fs = FileSystem.get(new URI(outputPath), job);

    if (fs.exists(new Path(outputPath)))
      fs.delete(new Path(outputPath), true);

    // Set the path where final output 'part' files will be saved.
    LOG.info("setting output path to '" + outputPath + "'");
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
   // FileOutputFormat.setCompressOutput(job, false);
    FileOutputFormat.setCompressOutput(job, true);

    
    // Set which InputFormat class to use.
    job.setInputFormat(ArcInputFormat.class);

    // Set which OutputFormat class to use.
    job.setOutputFormat(TextOutputFormat.class);

    // Set the output data types.
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Set which Mapper and Reducer classes to use.
    job.setMapperClass(WebPageProcessorMapper.class);
    job.setReducerClass(WebPageProcessorReducer.class);

 
    
    if (JobClient.runJob(job).isSuccessful())
      return 0;
    else
      return 1;
  }
  

  /**
   * Main entry point that uses the {@link ToolRunner} class to run the example
   * Hadoop job.
   */
  public static void main(String[] args)
      throws Exception {
    int res = ToolRunner.run(new Configuration(), new WebpageProcessor(), args);
    System.exit(res);
  }
}
