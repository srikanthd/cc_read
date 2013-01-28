package com.intuit.mapreduce;


import java.io.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

/*
 * Reducer class for performing aggregate analysis of the output.
 */
public class OutputProcessorReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        String text = "";
        Iterator iter = values.iterator();
	
       
        long cnt=0;
        Set<String> domainNames = new HashSet<String>();
        while (iter.hasNext()) {
		
            String s = (iter.next().toString());

            //Depending on the key, track the unique number of domain names or perform the sum of the values.
            if(key.toString().equals("DOMAIN_NAME"))
            {
            	domainNames.add(s);
            }
            else
            {
            	sum += Integer.parseInt(s);
            	
            }
            
            cnt++;
        }
        
        
        //perform the average operation for anchor text count features.
        if(key.toString().equals("AVG_INBOUND_ANCHOR_TEXT_WORDS_CNT") )
        {
        	sum = sum/cnt;
        	
        }
        
        if(key.toString().equals("DOMAIN_NAME") )
        {
           context.write(new Text("NUM_DOMAINS_PROCESSED"),new Text(String.valueOf(domainNames.size())));
        	
        }
        else
        {
        	context.write(key,new Text(String.valueOf(sum)));
        }
        
    }
}