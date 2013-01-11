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
	    int totoutboundcnt = 0;
	    int jslinkscnt = 0;
	    int indomaincnt = 0;
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
  
  // just curious how many of each content type we've seen
  reporter.incrCounter(this._counterGroup, "Content Type - "+value.getContentType(), 1);

  // ensure sample instances have enough memory to parse HTML
  if (value.getContentLength() > (5 * 1024 * 1024)) {
    reporter.incrCounter(this._counterGroup, "Skipped - HTML Too Long", 1);
    return;
  }

  // Count all 'itemtype' attributes referencing 'schema.org'
  Document doc = value.getParsedHTML();

 
  if (doc == null) {
    reporter.incrCounter(this._counterGroup, "Skipped - Unable to Parse HTML", 1);
    return;
  }
  
  
  String c_url = value.getURL();
  
  Elements links  = doc.select("a[href]"); 
 
    totoutboundcnt  = links.size();
    
    String cwurlhost = getHost(c_url);
    String cwurldom = getBaseDomain(c_url);
    
    for(int k=0;k<links.size(); ++k)
    {
    	
    Element curLinkElem = links.get(k);
    String curLink	= curLinkElem.attr("href");
    
    if (curLink.contains(cwurlhost)) {
        outindomcnt++;
    } else {
        if (getBaseDomain(curLink).equals(cwurldom)) {
            outsubdomcnt++;
        } else {
            if (curLink.startsWith("/")
                    || curLink.startsWith("../")
                    || curLink.startsWith("#")) {
                outindomcnt++;
            } else if ((curLink.startsWith("http:"))
                    && (!curLink.contains(cwurlhost))) {

            	String resLinkText = "1///0///0///0///0" ;
            	
                output.collect(new Text(curLink), new Text(resLinkText));
            } else if (curLink.startsWith(
                    "javascript:")) {
                jslinkscnt++;
            }
        }
    }
    
    }
    
    
    String resText = indomaincnt + "///" + totoutboundcnt + "///"
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