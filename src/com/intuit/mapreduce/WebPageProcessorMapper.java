package com.intuit.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.commoncrawl.hadoop.mapred.ArcRecord;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Mapper class for processing the webpage as "Arcrecord"
 * @author rgrandhi
 *
 */
public class WebPageProcessorMapper
extends    MapReduceBase
implements Mapper<Text, ArcRecord, Text, Text> {

	
private static final Logger LOG = Logger.getLogger(WebpageProcessor.class);

// create a counter group for Mapper-specific statistics
private final String _counterGroup = "Custom Mapper Counters";

public void map(Text key, ArcRecord value, OutputCollector<Text, Text> output, Reporter reporter)
  throws IOException {

	    int outindomcnt = 0;
	    int outsubdomcnt = 0;
	    int totLinks = 0;
	    int totoutboundcnt = 0;
	    int jslinkscnt = 0;
	    int totinboundcnt = 0;
	    String title = "";
	    String h1 = "";
	    String text = "";
	    String anc = "";	
	
	
try {

  if (!value.getContentType().contains("html")) {
    reporter.incrCounter(this._counterGroup, "Skipped - Not HTML", 1);
    return;
  }

  System.out.println(value.getContentType());
  
  //content type of the file we've seen
  reporter.incrCounter(this._counterGroup, "Content Type - "+value.getContentType(), 1);

  // ensure sample instances have enough memory to parse HTML
  if (value.getContentLength() > (5 * 1024 * 1024)) {
    reporter.incrCounter(this._counterGroup, "Skipped - HTML Too Long", 1);
    return;
  }

  // Get the Document object of the parsed HTML using Jsoup parser.
  Document doc = value.getParsedHTML();

 
  if (doc == null) {
    reporter.incrCounter(this._counterGroup, "Skipped - Unable to Parse HTML", 1);
    return;
  }
  
  //the page URL we are processing in the current step.
  String c_url = value.getURL();
  
  //All link elements in the current web page.
  Elements links  = doc.select("a[href]"); 
 
  //total number of links present in the current webpage.
  totLinks  = links.size();
    
  String cwurlhost = getHost(c_url);
  String cwurldom = getBaseDomain(c_url);
    
    for(int k=0;k<links.size(); ++k)
    {
    	
    Element curLinkElem = links.get(k);
    String curLink	= curLinkElem.attr("href");
    
    //If the link points to the same domain.
    if (curLink.contains(cwurlhost)) {
        outindomcnt++;
    } 
    else {
    	
    	// if the link points to another page in sub domain.
        if (getBaseDomain(curLink).equals(cwurldom)) {
            outsubdomcnt++;
        } else {
        	
        	//if the link contains relative paths or points to the same page.
            if (curLink.startsWith("/")
                    || curLink.startsWith("../")
                    || curLink.startsWith("#")) {
                outindomcnt++;
               
            } else if ((curLink.startsWith("http:"))
                    && (!curLink.contains(cwurlhost))) {

            	//else the link points to external web page.
            	totoutboundcnt++;
            	
            	//increment the count of "inbound" link from the current page to the external webpage.
            	String resLinkText = "1///0///0///0///0///0" ;
            	
                output.collect(new Text(curLink), new Text(resLinkText));
            } else if (curLink.startsWith(
                    "javascript:")) {
                jslinkscnt++;
            }
        }
    }
    
    }
    
    
    String resText = totinboundcnt + "///" + totLinks + "///" + totoutboundcnt + "///"
            + outindomcnt + "///" + outsubdomcnt + "///"
            + jslinkscnt ;

	output.collect(new Text(c_url), new Text(resText));
  
    
  
}
catch (Throwable e) {

  // occassionally Jsoup parser runs out of memory ...
  if (e.getClass().equals(OutOfMemoryError.class))
    System.gc();

  LOG.error("Caught Exception", e);
  reporter.incrCounter(this._counterGroup, "Skipped - Exception Thrown", 1);
}
}

//get the current host of a given URL
public static String getHost(String url) {
    if (url == null || url.length() == 0) {
        return "";
    }
    
    int doubleslash = url.indexOf("//");

    if (doubleslash == -1) {
        doubleslash = 0;
    } else {
        doubleslash += 2;
    }

    int end = url.indexOf('/', doubleslash);

    end = end >= 0 ? end : url.length();

    return url.substring(doubleslash, end);
}


//get the base domain of a given URL
public static String getBaseDomain(String url) {
    String host = getHost(url);

    int startIndex = 0;
    int nextIndex = host.indexOf('.');
    int lastIndex = host.lastIndexOf('.');

    while (nextIndex < lastIndex) {
        startIndex = nextIndex + 1;
        nextIndex = host.indexOf('.', startIndex);
    }
    if (startIndex > 0) {
        return host.substring(startIndex);
    } else {
        return host;
    }
}


public static void main(String[] args)
{
	
	
	
	
}



}