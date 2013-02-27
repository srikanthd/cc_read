package com.intuit.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map;

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
 * 
 *
 */
public class WebPageProcessorMapper
extends    MapReduceBase
implements Mapper<Text, ArcRecord, Text, Text> {

	
private static final Logger LOG = Logger.getLogger(WebpageProcessor.class);

// create a counter group for Mapper-specific statistics
private final String _counterGroup = "Custom Mapper Counters";

private final boolean INCLUDE_ANCHOR_TEXT = false;

private final boolean TOP_100_DOMAIN_FLAG = true;
public void map(Text key, ArcRecord value, OutputCollector<Text, Text> output, Reporter reporter)
  throws IOException {
        //Total number of links pointing to with in page.
	    int totLinksToSamePage = 0;
	    
	    //Total links pointing to the same base domain as the current page.
	    int totLinkstoPagesBaseDomain = 0;
	    
	    //Total links pointing to the same sub domain as the current page.
	    int totLinkstoPagesSubDomain = 0;
	    
	    //Total number of links in a page.
	    int totLinks = 0;
	    
	    //Total number of links to external web pages.
	    int totLinksToExternalDomainPages = 0;
	    
	    //total number of javascript links.
	    int jslinkscnt = 0;
	    
	    //Total number of links pointing to this page from external domain webpages.
	    int totInBoundLinksExternalDomainPages = 0;
	    
	    //Total number of links pointing to this page from internal domain webpages.
	    int totInBoundLinksInternalDomainPages = 0;
	   	
	
try {

  if (!value.getContentType().contains("html")) {
    reporter.incrCounter(this._counterGroup, "Skipped - Not HTML", 1);
    return;
  }

  
  //content type of the file we've seen
  reporter.incrCounter(this._counterGroup, "Content Type - "+value.getContentType(), 1);

  // ensure sample instances have enough memory to parse HTML
  if (value.getContentLength() > (5 * 1024 * 1024)) {
    reporter.incrCounter(this._counterGroup, "Skipped - HTML Too Long", 1);
    return;
  }

  // Get the Document object of the parsed HTML using Jsoup parser.
  Document doc = value.getParsedHTML();


 
  Map< String, Integer > docTokenMap = LinkAnalysis.tokenizeHTMLDocument(doc);
  
  
  
  if (doc == null) {
    reporter.incrCounter(this._counterGroup, "Skipped - Unable to Parse HTML", 1);
    return;
  }
 
  

  
  
  //the page URL we are processing in the current step.
  String c_url = value.getURL();

  
  if(WebpageProcessor.DEBUG)
  {
	  System.out.println("Processing webpage URL:" + c_url);
	  System.out.println("Start HTML");
	  System.out.println(doc.html());
	  System.out.println("End HTML");
  }
  
  
  //Detect widgets
  String widgetURL = LinkAnalysis.detectWidgets(doc);
  if(widgetURL.length()>0)
  {
	if(TOP_100_DOMAIN_FLAG)
	 {
		if(WebPageClassifier.verifyTop100Domain(widgetURL))
		{
			
			output.collect(new Text("WIDGET_" + widgetURL), new Text("1")); 
		}
	}
	else
	{
			output.collect(new Text("WIDGET_" + widgetURL), new Text("1"));  
	}
  }
  
  
  
  //All link elements in the current web page.
  Elements links  = doc.select("a[href]"); 
 

    
  String cwurlhost = getHost(c_url);
  String cwurldom = getBaseDomain(c_url);
   
  if(WebpageProcessor.DEBUG)
  {
	  System.out.println("cur url host" + cwurlhost);
	  System.out.println("cur url domain" + cwurldom);
	  System.out.println("number of links" + links.size());
  }
  
  
  for(int k=0;k<links.size(); ++k)
   {
    	
    Element curLinkElem = links.get(k);
    String curLink	= curLinkElem.attr("href");
       
    //skip links to images.
    if(curLink.endsWith(".jpg"))
    {
    	continue;
    }
    
    if(curLink.trim().length()==0) continue;
   
    
    //if top 100 domain flag is set include only links that point to top 100 domain pages.
    if(TOP_100_DOMAIN_FLAG && !WebPageClassifier.verifyTop100Domain(curLink))
    {
    	continue;
    }
    
    String curAnchorText = curLinkElem.text();
 
    
    String linkRelevanceStr = LinkAnalysis.computePageRelevanceStr(curAnchorText, curLink, docTokenMap);
    
    if(WebpageProcessor.DEBUG)
    {
  	  System.out.println("cur link" + curLink);
  	  System.out.println("anchor text" + curAnchorText);
  	  System.out.println("link relevance str::" + linkRelevanceStr);
    }
    
    
    
    
    
     //url matches link#url has three parts#url matches first part#url matches second part#url matches third part
    String anchorTextMatchesLink = "";
    
    List<String> urlParts = getURLParts(curLink);
    
    if(WebpageProcessor.DEBUG)
    {
    	for(int m=0;m<urlParts.size() ; ++m)
    	{
    		System.out.println("URL Part " + k + "\t" + urlParts.get(m));
    	}
    }
    
    
    
    //Track if the anchor text matches the words in the link. 
    if(verifyAnchorTextMatchesStr(curAnchorText,curLink))
    {
    	anchorTextMatchesLink += "1" ; 
    }
    else
    {
    	anchorTextMatchesLink += "0";
    }
    
    
    //verify if url has three parts.
    if(urlParts.size()==3)
    {
    	//url has three parts
    	anchorTextMatchesLink += "#1";
    	
    	for(int m=0;m<urlParts.size();++m)
    	{
    		anchorTextMatchesLink += "#";
    		if(verifyAnchorTextMatchesStr(curAnchorText,urlParts.get(m)))
    		{
    			anchorTextMatchesLink += "1";
    		}
    		else
    		{
    			anchorTextMatchesLink += "0";
    		}
    	}
    }
    else if(urlParts.size()==2)  //if there are two parts , assume middle part as empty and use the second part as last part.
    {
    	//url has three parts
    	anchorTextMatchesLink += "#1";
    	
    	//first part
    	anchorTextMatchesLink += "#";
    	if(verifyAnchorTextMatchesStr(curAnchorText,urlParts.get(0)))
    	{
    		anchorTextMatchesLink += "1";
    	}
    	else
    	{
    		anchorTextMatchesLink += "0";
    	}
    
    
    	//middle part is empty
    	anchorTextMatchesLink += "#0";

    	//third part
    	anchorTextMatchesLink += "#";
    	if(verifyAnchorTextMatchesStr(curAnchorText,urlParts.get(1)))
    	{
    		anchorTextMatchesLink += "1";
    	}
    	else
    	{
    		anchorTextMatchesLink += "0";
    	}
    	  
    }
    else {
    	//url doesnt have 3 parts..so all the parts correspond to zero.
    	anchorTextMatchesLink += "#0#0#0#0";
    }
    
    //Histogram for word counts of 1,2,3,4,5,>5
    String curAnchorTextWordHistogram[] = {"0","0","0","0","0","0"} ;
    
    String curAnchorTextwords[] = curAnchorText.toLowerCase().trim().split(" ");
    
    if(curAnchorTextwords.length>=1 && curAnchorTextwords.length<=5)
    {
    	curAnchorTextWordHistogram[curAnchorTextwords.length-1] = "1";
    }
    else if(curAnchorTextwords.length>5)
    {
    	curAnchorTextWordHistogram[5] = "1";
    }
    
    String curAnchorTextWordHistogramStr = "";
   
    for(int m=0 ; m<curAnchorTextWordHistogram.length;++m)
    {
    	if(curAnchorTextWordHistogramStr.length()>0) curAnchorTextWordHistogramStr += "#";
    	
    	curAnchorTextWordHistogramStr += curAnchorTextWordHistogram[m];
    }
    
    
   
  
    totLinks++;
    
    boolean internalLink = false;
     //If the link points to the same sub domain/host.
    	if (curLink.contains(cwurlhost)) {
    		totLinkstoPagesSubDomain++;
    		internalLink=true;
    	} 
    	
        // if the link points to another page in sub domain.
        if (getBaseDomain(curLink).equals(cwurldom)) {
        	totLinkstoPagesBaseDomain++;
        	internalLink=true;
        } 
        	
        
        //if the link contains relative paths
        if (curLink.startsWith("/") || curLink.startsWith("../"))
        {
        	totLinkstoPagesBaseDomain++;
        	internalLink=true;
        }
        	 
        
        //if the link points to different section of the page.
        if(curLink.startsWith("#")) {
            	totLinksToSamePage++;
            	internalLink=true;
         }
               
        
        String curAnchorTextOutput = "";
        
        if(this.INCLUDE_ANCHOR_TEXT)
        {
        	curAnchorTextOutput = curAnchorText;
        }
        
        if (curLink.startsWith("http:") && (!internalLink)) {

            	//else the link points to external web page.
            	totLinksToExternalDomainPages++;
            	
            	//increment the count of "inbound" link from the current page to the external webpage.
            	String resLinkText = "1///0///0///0///0///0///0///0///" + curAnchorTextOutput + "///" +  curAnchorTextWordHistogramStr + "///" + anchorTextMatchesLink + "///" + linkRelevanceStr ;
            	
            	if(WebpageProcessor.DEBUG)
            	{
            		System.out.println(curLink + "\t" + resLinkText);
            	}
                output.collect(new Text(curLink), new Text(resLinkText));
            }
        
        
        
        if (curLink.startsWith("http:") && (internalLink)) {
        	
        	//increment the count of "inbound" link from the current page to the internal webpage.
        	String resLinkText = "0///1///0///0///0///0///0///0///" + curAnchorTextOutput + "///" + curAnchorTextWordHistogramStr  + "///" + anchorTextMatchesLink + "///" + linkRelevanceStr;
        	
        	if(WebpageProcessor.DEBUG)
        	{
        		System.out.println(curLink + "\t" + resLinkText);
        	}
        	
            output.collect(new Text(curLink), new Text(resLinkText));
        }
        
        
        if (curLink.startsWith("javascript:")) {
                jslinkscnt++;
            }
        
        
    }
 
 
   String resText = totInBoundLinksExternalDomainPages + "///" +totInBoundLinksInternalDomainPages + "///" + totLinks + "///" + totLinksToExternalDomainPages + "///"
            + totLinksToSamePage + "///" + totLinkstoPagesBaseDomain + "///" + totLinkstoPagesSubDomain + "///" +  jslinkscnt + "///" + "" + "///" + "0#0#0#0#0#0" + "///" + "0#0#0#0#0" + "///" + "0#0#0#0#0#0#0" ;
   
 //if top 100 domain flag is set include the output in the reducer only if it is the  top 100 domain page.
   if(TOP_100_DOMAIN_FLAG && !WebPageClassifier.verifyTop100Domain(c_url))
   {
	   return;
   }
   

   if(WebpageProcessor.DEBUG)
	{
		System.out.println(c_url + "\t" + resText);
	}
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


/*
 * Returns 1 if all the words of anchor text are present in the title. Words need not
 * be in sequential order.
 */
public static boolean verifyAnchorTextMatchesStr(String anchorText, String title)
{
	if(anchorText.trim().length()==0) return false;
	
	String anchorTextFields[] = anchorText.toLowerCase().split(" ");
	boolean res= true;
	
	title = title.toLowerCase();
	title = title.replaceAll("/", " ");
	title = title.replaceAll("\\.", " ");
	title = title.replaceAll("_", " ");
	title = title.replaceAll("-", " ");
	title = title.trim();
	String titleFields[] = title.split(" ");
	Set<String> titleSet = new HashSet<String>();
	
	for(int k=0; k<titleFields.length ; ++k)
	{
		titleSet.add(titleFields[k]);
	}
	
	for(int k=0;k<anchorTextFields.length ; ++k)
	{
		
		if(!titleSet.contains(anchorTextFields[k]))
		{
			if(WebpageProcessor.DEBUG)
			{
				System.out.println("verifying anchor text:" + anchorText + "\tmatches\t" + title + "\t" + "false");
			}
			
			return false;
		}
	}
	
	if(WebpageProcessor.DEBUG)
	{
		System.out.println("verifying anchor text:" + anchorText + "\tmatches\t" + title + "\t" + res);
	}
	return res;
	
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


//divide the URL into three parts. the first one before "/", the middle part and the last part after the last "/"
public static List<String> getURLParts(String url)
{
	List<String> resList = new ArrayList<String>();
	
	int ind0 = url.indexOf("//");
	
	int indFirst = -1;
	
	if(ind0>=0)
	{
		indFirst = url.indexOf("/", ind0+3);
	}
	else
	{
		indFirst= url.indexOf("/");
	}
	
	
	int indLast = url.lastIndexOf("/");
	
	
	if(indFirst>=0 && indLast>=0 && indFirst<indLast)
	{
		resList.add(url.substring(0,indFirst));
		resList.add(url.substring(indFirst+1,indLast));
		resList.add(url.substring(indLast+1));
	}
	
	return resList;
}

public static void main(String[] args)
{
	
	String c_url = "http://news.yahoo.com/news/456";
	String cwurlhost = getHost(c_url);
	  String cwurldom = getBaseDomain(c_url);
	  
	System.out.println(cwurlhost);
	System.out.println(cwurldom);
	
	
	String curLink = c_url;
	 if (curLink.contains(cwurlhost)) {
	        System.out.println("in domain");
	    } 
	    else {
	    	  if (getBaseDomain(curLink).equals(cwurldom)) {
	    		  System.out.println("in sub domain");
	    	
	    	  }
	    }
	
	 
	 String anchorText = "news yahoo1 money";
	 String url = "http://news.yahoo.com/news/456_t-yahoo1";

	 
	 boolean res = verifyAnchorTextMatchesStr(anchorText,url);
	 System.out.println("contains::" + res);
	 
	 url = "news.yahoo.com/sdff456_t-yahoo1";
	 List<String> resList = getURLParts(url);
	 for (Iterator iterator = resList.iterator(); iterator.hasNext();) {
		String str = (String) iterator.next();
		System.out.println(str);
	}
	 
	 
}



}