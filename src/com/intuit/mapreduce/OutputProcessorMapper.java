
package com.intuit.mapreduce;
 

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
 
/*
 * Mapper class for performing aggregate analysis of output.
 */
public class OutputProcessorMapper extends Mapper<LongWritable, Text, Text, Text> {
  
	public int NUM_INBOUND_THRESHOLD =1000;
	public boolean INBOUND_POPULARITY=false;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String content = value.toString();
            String fields[] = content.split("\t");
            String curUrl = fields[0];
            String outputStr = fields[1];
            
            String outputFields[] = outputStr.split("///",-1);

          
            String inboundAnchorText = outputFields[8];
            
            String anchorTextWordHistogramStr = outputFields[9];
          
            
            String inboundAnchorTextFields[] = inboundAnchorText.split("\\$#",-1);
            
            long avgInboundAnchorWords = 0;
            

            //Skip this page if the inbound link popularity is turned on and it is  less than the threshold
            if(INBOUND_POPULARITY && !(inboundAnchorText.trim().length()>0 && inboundAnchorTextFields.length>this.NUM_INBOUND_THRESHOLD))
            {
            	return;
            }
            
            
            //tracking number of inbound anchor links as a proxy for the popularity of the web page.
            if(inboundAnchorText.trim().length()>0 && inboundAnchorTextFields.length>0)
            {
            	String keyName = "NUM_INBOUND_ANCHOR_LINKS_" + inboundAnchorTextFields.length; 
            	context.write(new Text(keyName), new Text("1"));

            }
          
            
            //tracking number of domain names.
      	   //String cwurldom = WebPageProcessorMapper.getBaseDomain(curUrl);
           // context.write(new Text("DOMAIN_NAME"), new Text(cwurldom.toLowerCase()));


      	  
            //Computing the histogram of number of words in the inbound anchor text of the current page.
            //Uses the ouput anchor text.
            for(int k=0;k<inboundAnchorTextFields.length;++k)
            {
            	String curAnchor = inboundAnchorTextFields[k];
            	if(curAnchor.trim().length()>0)
            	{
            		String words[] = curAnchor.trim().split(" ");
            		avgInboundAnchorWords += words.length;
            		String keyName = "ANCHOR_TEXT_WORD_COUNT_" + words.length; 
            		context.write(new Text(keyName), new Text("1"));
            	}
            }
            
            
            //Aggregating the histogram of word count in anchor text. uses the histogram output.
            String anchorTextWordHistogramFields[] = anchorTextWordHistogramStr.split("#");
            for(int k=0;k<anchorTextWordHistogramFields.length ; ++k)
            {
            	if(k<5)
            	{
            		context.write(new Text("ANCHOR_TEXT_HISTOGRAM_WORDS_COUNT_" + (k+1)), new Text(anchorTextWordHistogramFields[k]));

            	}
            	else
            	{
            		context.write(new Text("ANCHOR_TEXT_HISTOGRAM_WORDS_COUNT_GREATER_THAN_5"), new Text(anchorTextWordHistogramFields[k]));

            	}
            	
            }
            
            
            
            //System.out.println("cur url::" + curUrl);
            //System.out.println("number of fields::" + outputFields.length);
           
            context.write(new Text("NUMBER_OF_WEBPAGES"), new Text("1"));
            
              
            context.write(new Text("TOT_INBOUND_FROM_EXTERNAL_DOMAIN_PAGES"), new Text(outputFields[0]));
            context.write(new Text("TOT_INBOUND_FROM_INTERNAL_DOMAIN_PAGES"), new Text(outputFields[1]));
            context.write(new Text("TOT_LINKS"), new Text(outputFields[2]));
            context.write(new Text("TOT_LINKS_TO_EXTERNAL_DOMAIN_PAGES"), new Text(outputFields[3]));
            context.write(new Text("TOT_LINKS_TO_SAME_PAGE"), new Text(outputFields[4]));
            context.write(new Text("TOT_LINKS_PAGES_SAME_BASE_DOMAIN"), new Text(outputFields[5]));
            context.write(new Text("TOT_LINKS_PAGES_SAME_SUB_DOMAIN"), new Text(outputFields[6]));
            context.write(new Text("TOT_JS_LINK_CNT"), new Text(outputFields[7]));
            context.write(new Text("TOT_ANCHOR_TEXT_MATCHES_LINK"), new Text(outputFields[10]));

      
         
            
          //output only if it is non-zero
            if(avgInboundAnchorWords>0)
            {
            	
            	avgInboundAnchorWords = avgInboundAnchorWords/inboundAnchorTextFields.length;
            	context.write(new Text("AVG_INBOUND_ANCHOR_TEXT_WORDS_CNT"), new Text(String.valueOf(avgInboundAnchorWords)));
            }
            
            
            //Tracking the number of hyphens and underscores in the web pages.
            
            if(curUrl.indexOf("-")>=0)
            {
                context.write(new Text("NUM_HYPHEN_URLS"), new Text("1"));

            }
           
            
            if(curUrl.indexOf("_")>=0)
            {
                context.write(new Text("NUM_UNDERSCORE_URLS"), new Text("1"));

            }
            
                   
        } catch (Exception e) {}
    }
 
}
