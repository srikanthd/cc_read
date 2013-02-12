package com.intuit.mapreduce;


import java.io.*;
import java.util.HashMap;
import java.util.Map;
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
        	
        	Map<String,String> domainTypeHistogram = new HashMap<String,String>();
        	
        	for (Iterator iterator = domainNames.iterator(); iterator.hasNext();) {
				String curDomain = (String) iterator.next();
				if(curDomain.length()>4 && curDomain.substring(curDomain.length()-4, curDomain.length()-3).equals("."))
				{
					String domainType = curDomain.substring(curDomain.length()-3);
					//System.out.println(curDomain + "\t" + domainType);
					//System.out.println("histogram size::" + domainTypeHistogram.size());
					domainType = domainType.toLowerCase().trim();
					if(domainTypeHistogram.containsKey(domainType))
					{
						String curVal = domainTypeHistogram.get(domainType);
						curVal = String.valueOf(Long.valueOf(curVal) + 1);
						domainTypeHistogram.put(domainType, curVal);
					}
					else
					{
						domainTypeHistogram.put(domainType, "1");
					}
					
				}
			}
        	
           context.write(new Text("NUM_DOMAINS_PROCESSED"),new Text(String.valueOf(domainNames.size())));
        
           for (Iterator iterator = domainTypeHistogram.keySet().iterator(); iterator.hasNext();) {
			String curDomainType = (String) iterator.next();
	           context.write(new Text("NUM_DOMAINS_PROCESSED_Type_" + curDomainType),new Text(String.valueOf(domainTypeHistogram.get(curDomainType))));
	           
           }
           
        }
        else
        {
        	context.write(key,new Text(String.valueOf(sum)));
        }
        
    }
}