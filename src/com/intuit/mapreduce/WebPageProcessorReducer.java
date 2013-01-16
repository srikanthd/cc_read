package com.intuit.mapreduce;


import java.io.*;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.hadoop.mapred.ArcRecord;


/**
 * Reducer class for processing the webpage as "Arcrecord"
 * @author rgrandhi
 *
 */


public class WebPageProcessorReducer  
extends    MapReduceBase
implements Reducer<Text, Text, Text, Text>

{
	public void reduce(Text key, Iterator<Text> iter, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        long sum1 = 0, sum2 = 0, sum3 = 0, sum4 = 0, sum5 = 0, sum6 = 0;
        String text = "";
	
        // sum all the output variable counts across all the values for the current URL.
        while (iter.hasNext()) {
		
            String s[] = (iter.next().toString()).split("///");

            sum1 += Integer.parseInt(s[0]);
            sum2 += Integer.parseInt(s[1]);
            sum3 += Integer.parseInt(s[2]);
            sum4 += Integer.parseInt(s[3]);
            sum5 += Integer.parseInt(s[4]);
            sum6 += Integer.parseInt(s[5]);
            if (s.length > 6) {
                text = text + s[6];
            }
        }
        output.collect(key,
                new Text(
                sum1 + "///" + sum2 + "///" + sum3 + "///" + sum4 + "///" + sum5 + "///" + sum6));
    }


    
}