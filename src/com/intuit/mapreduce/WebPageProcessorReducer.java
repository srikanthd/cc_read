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
 * 
 *
 */


public class WebPageProcessorReducer  
extends    MapReduceBase
implements Reducer<Text, Text, Text, Text>

{
	public void reduce(Text key, Iterator<Text> iter, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        
		//if the output is a widget link
		if(key.toString().startsWith("WIDGET_"))
		{
			int res=0;
			while (iter.hasNext()) {
		  		 String curStr = iter.next().toString();
		         res += Integer.parseInt(curStr);
			 }
			 output.collect(key,new Text(String.valueOf(res)));
		                
		    return;
		}
		
		
		
		//if the output is a hyper link
		long sum1 = 0, sum2 = 0, sum3 = 0, sum4 = 0, sum5 = 0, sum6 = 0 , sum7=0, sum8=0 , anchorTextMatchesLink =0;
        String pageAnchorText = "";
        String inBoundAnchorText = "";
        
        long anchorTextWordHistogram[] = {0,0,0,0,0,0};
        long anchorTextMatchesLinkHistogram[] = {0,0,0,0,0};
        long linkRelevanceHistogram[] = {0,0,0,0,0,0,0};
        
        
        String anchorTextWordHistogramStr = "";
        
        String linkRelevanceStr = "";
        boolean histogramOutput =false;
        boolean linkRelevanceOutput = false;
        boolean anchorTextMatchesLinkOutput = false;
        String anchorTextMatchesLinkHistogramStr = "";
        
        // sum all the output variable counts across all the values for the current URL.
        while (iter.hasNext()) {
		
        	String curStr = iter.next().toString();
        	
        	//System.out.println("current value str::" + curStr);
            String s[] = curStr.split("///",-1);

            sum1 += Integer.parseInt(s[0]);
            sum2 += Integer.parseInt(s[1]);
            sum3 += Integer.parseInt(s[2]);
            sum4 += Integer.parseInt(s[3]);
            sum5 += Integer.parseInt(s[4]);
            sum6 += Integer.parseInt(s[5]);
            sum7 += Integer.parseInt(s[6]);
            sum8 += Integer.parseInt(s[7]);
            
            //if anchor text is output.
            if (s.length > 8 && s[8].length()>0) {
            	if(inBoundAnchorText.length()>0) inBoundAnchorText += "$#";
            	inBoundAnchorText = inBoundAnchorText + s[8];
            }
            
            
            //if anchor text word histogram is output. compute the sum of histogram values.
            if (s.length > 9 && s[9].length()>0) {
            	//System.out.println("anchor text histogram::" + s[9]);
            	String histogramFields[] = s[9].split("#",-1);
            	for(int m=0;m<anchorTextWordHistogram.length;++m)
            	{
            		anchorTextWordHistogram[m] += Long.parseLong(histogramFields[m]);
            	}
            	histogramOutput = true;
            }
            	
            
            //anchor text matches the link.
            if (s.length > 10 && s[10].length()>0) 
            {
            	
            	String anchorTextMatchesHistogramFields[] = s[10].split("#",-1);
            	for(int m=0;m<anchorTextMatchesLinkHistogram.length;++m)
            	{
            		anchorTextMatchesLinkHistogram[m] += Long.parseLong(anchorTextMatchesHistogramFields[m]);
            	}
            	anchorTextMatchesLinkOutput = true;
		
            }
            
            
            
            //anchor text matches the link.
            if (s.length > 11 && s[11].length()>0) 
            {
            	
            	String linkRelevanceFields[] = s[11].split("#",-1);
            	for(int m=0;m<linkRelevanceHistogram.length;++m)
            	{
            		linkRelevanceHistogram[m] += Long.parseLong(linkRelevanceFields[m]);
            	}
            	linkRelevanceOutput = true;
		
            }
            
            
            
            
            
            }
            
        
	    //if histogram is output, compute the updated string representation of the histogram.
		if(histogramOutput)
		{
			for(int m=0 ; m<anchorTextWordHistogram.length ; ++m)
			{
				if(anchorTextWordHistogramStr.length()>0) anchorTextWordHistogramStr += "#";
				anchorTextWordHistogramStr += String.valueOf(anchorTextWordHistogram[m]);
			}
		  	
		}
		
		
		if(anchorTextMatchesLinkOutput)
		{
			for(int m=0 ; m<anchorTextMatchesLinkHistogram.length ; ++m)
			{
				if(anchorTextMatchesLinkHistogramStr.length()>0) anchorTextMatchesLinkHistogramStr += "#";
				anchorTextMatchesLinkHistogramStr += String.valueOf(anchorTextMatchesLinkHistogram[m]);
			}
		  	
		}
		
		
		if(linkRelevanceOutput)
		{
			for(int m=0 ; m<linkRelevanceHistogram.length ; ++m)
			{
				if(linkRelevanceStr.length()>0) linkRelevanceStr += "#";
				linkRelevanceStr += String.valueOf(linkRelevanceHistogram[m]);
			}
		  	
		}
		
		
        output.collect(key,
                new Text(
                sum1 + "///" + sum2 + "///" + sum3 + "///" + sum4 + "///" + sum5 + "///" + sum6 + "///" + sum7 + "///" + sum8 + "///" + inBoundAnchorText + "///" + anchorTextWordHistogramStr + "///" + anchorTextMatchesLinkHistogramStr + "///" + linkRelevanceStr));
                
    }


    
}