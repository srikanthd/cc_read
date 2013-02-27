package com.intuit.mapreduce;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class LinkAnalysis {
	
	
	//Detect if a HTML page has a widget emebedded and extract the URL which the widget points to.
	public static String detectWidgets(Document doc )
	{
		
		List<Element> elementsList = new ArrayList<Element>();
		
		Elements objectElements = doc.getElementsByTag("object");
		
		for(int k=0;k<objectElements.size();++k)
		{
			elementsList.add(objectElements.get(k));
		}
		//System.out.println("object elements:::" + objectElements.size());

		Elements scriptElements = doc.getElementsByTag("script");
		
		for(int k=0;k<scriptElements.size();++k)
		{
			elementsList.add(scriptElements.get(k));
		}

		//System.out.println("script elements:::" + scriptElements.size());

		
		//For each of the Script and Object elements check for any matching element with text "widget" and then return the URL pointing to.
		for(int k=0;k<elementsList.size(); ++k)
		{
			Element curElem = elementsList.get(k);
			
			if(curElem.html().toLowerCase().indexOf("widget")>=0)
			{
				
				int ind1 = curElem.html().indexOf("\"http:");
				
				if(ind1>=0)
				{
					int ind2 = curElem.html().indexOf("\"", ind1+5);
					String targetURL = curElem.html().substring(ind1+1,ind2);
					return targetURL;
				}
			}
			
		}
		
		
		return "";
		
		
		
	}

	//list of stop words to be used. these are not considered when computing relevance.
	public static Set<String> getStopWords()
	{
		String stopWords[] = {"I", "a", "about", "an", "are", "as", "at", "be", "by", "com", "for", "from","how",
				              "in", "is", "it", "of", "on", "or", "that", "the", "this","to", "was","what", "when",
				              "where","who", "will", "with", "the","www","http","com"};
		
		Set<String> resSet = new HashSet<String>();
		
		for(int k=0;k<stopWords.length; ++k)
		{
			resSet.add(stopWords[k].toLowerCase());
		}
		
		return resSet;
	}
	
	
	//Compute relevance of anchor text with rest of the page. Anchor text is relevant to the page if rest of the page has all the words that are present in the
	//anchor text. stop words are removed from consideration.
	public static String computeAnchorTextPageRelevance(String anchorText, Map< String,Integer > docTokenMap)
	{
		String res = "0";
		String tokens[] = tokenizeStr(anchorText);
		
		Set<String> stopWords = getStopWords();
		
		int nValidTokens = 0;
		for(int k=0;k<tokens.length;++k)
		{
			//ignore stop words
			if(stopWords.contains(tokens[k])) continue;
			
			String curToken = tokens[k];
			
			
			nValidTokens++;
			if(!docTokenMap.containsKey(curToken) || (docTokenMap.containsKey(curToken) && docTokenMap.get(curToken)<2) )
			{
				return "0";   //this particular token of the anchor text doesnt exist in the rest of the page.
			}
			
			
		}
	
	    //if all the words in anchor text matches the rest of the web page.
		if(nValidTokens>0)
		{
			return "1";
		}
		
		
		return "0";
		
	}
	
	
	//compute anchor text and link text relevance and return the output as combined string.
	public static String computePageRelevanceStr(String anchorText,String linkText, Map<String,Integer> docTokenMap)
	{
		
		String anchorTextRelevance = computeAnchorTextPageRelevance(anchorText,docTokenMap);
		
		String linkTextRelevance = computeLinkTextPageRelevance(linkText,anchorText,docTokenMap);

		String res = "";
		
		res = anchorTextRelevance + "#" + linkTextRelevance;
		
		return res;
		
	}
	
	
	
	//Compute relevance of hyper link text with rest of the page.Number of words that match the rest of the page are tracked.
	public static String computeLinkTextPageRelevance(String linkText,String anchorText, Map< String,Integer > docTokenMap)
	{
		String tokens[] = tokenizeStr(linkText);
		
		String anchorTextTokens[] = tokenizeStr(anchorText);
		Set<String> anchorTextTokenSet = new HashSet<String>();
		for(int m=0;m<anchorTextTokens.length ; ++m)
		{
			anchorTextTokenSet.add(anchorTextTokens[m].toLowerCase());
		}
		
		Set<String> stopWords = getStopWords();
		
		int nMatchedTokens = 0;
		
		//Histogram for matched word counts of 1,2,3,4,5,>5
	    String linkTextRelevanceWordHistogram[] = {"0","0","0","0","0","0"} ;
		
		for(int k=0;k<tokens.length;++k)
		{
			//ignore stop words
			if(stopWords.contains(tokens[k])) continue;
			
			String curToken = tokens[k];
			
			if(anchorTextTokenSet.contains(curToken))
			{
				if(docTokenMap.containsKey(curToken) && docTokenMap.get(curToken)>=2)
				{
					nMatchedTokens++;
				}
			}
			else
			{
				if(docTokenMap.containsKey(curToken))
				{
					nMatchedTokens++;
				}
			}
			
	    }
  	    
	    if(nMatchedTokens>=1 && nMatchedTokens<=5)
	    {
	    	linkTextRelevanceWordHistogram[nMatchedTokens-1] = "1";
	    }
	    else if(nMatchedTokens>5)
	    {
	    	linkTextRelevanceWordHistogram[5] = "1";
	    }
	    
	    String linkTextRelevanceWordHistogramStr = "";
	   
	    for(int m=0 ; m<linkTextRelevanceWordHistogram.length;++m)
	    {
	    	if(linkTextRelevanceWordHistogramStr.length()>0) linkTextRelevanceWordHistogramStr += "#";
	    	
	    	linkTextRelevanceWordHistogramStr += linkTextRelevanceWordHistogram[m];
	    }
		
		return linkTextRelevanceWordHistogramStr;
		
	}
	
	
	//tokenize the html document and return unique tokens and their counts. tokens are delimited by non-alpha numeric characters.
	public static Map< String ,Integer > tokenizeHTMLDocument(Document doc)
	{
		Map<String,Integer > docTokenMap = new HashMap< String,Integer >();
		
		String textStr = doc.body().text();
		//System.out.println(textStr);
		
		String tokens[] = tokenizeStr(textStr);
		
		for(int k=0;k<tokens.length ; ++k)
		{
			tokens[k] = tokens[k].trim();
			if(tokens[k].equals(" ") || tokens[k].length()==0 ) continue;
			//System.out.println(tokens[k] + "\t" + tokens[k].length());
			
			if(docTokenMap.containsKey(tokens[k]))
			{
				int curVal = docTokenMap.get(tokens[k]);
				docTokenMap.put(tokens[k],curVal+1);
			}
			else
			{
				docTokenMap.put(tokens[k],1);
			}
		}

		if(WebpageProcessor.DEBUG)
		{
			for (Iterator iterator = docTokenMap.keySet().iterator(); iterator.hasNext();) {
				String curToken = (String) iterator.next();
				System.out.println(curToken + "\t" + docTokenMap.get(curToken));
			}
		}
		
		return docTokenMap;
	}
	
	
	//function to tokenize string. delimited by non-alpha numeric characters.
	public static String[] tokenizeStr(String str)
	{
		//System.out.println(str);

		str = str.toLowerCase();
		str = str.replaceAll("[^a-zA-Z0-9\\s]", " ");
		str = str.trim();
		String tokens[] = str.split(" ");
		
		
		//for(int k=0;k<tokens.length;++k)
		//{
		//	System.out.println("token:::" + tokens[k]);
		//}

		return tokens;
	}
	
	
	public static void main(String[] args)
	{
		
		try {
			
		//	String url = "http://www.donatofurlani.it/category/webdesign/31/from_html_to_xhtml";
		//	String url = "http://www.apparelsearch.com/fashion/widgets/Amazon_Widget.htm";
		//	String url = "http://cnn.com";
			
			String url = "http://thefloridahoosier.com/2010/04/08/king-tut.aspx";
			Document doc = Jsoup.connect(url).get();
			
			Map<String, Integer> docTokenMap = tokenizeHTMLDocument(doc);
			
			Elements links = doc.getElementsByTag("a");
			
			for(int k=0;k<links.size(); ++k)
			{
			    	
			    Element curLinkElem = links.get(k);
			    
			    String curLink	= curLinkElem.attr("href");
			    String curAnchorText = curLinkElem.text();
			    
			    System.out.println(curAnchorText+ "\t" + curLink);
			
			    String relevanceStr = computePageRelevanceStr(curAnchorText,curLink,docTokenMap);
			    System.out.println("relevance str::" + relevanceStr);
			    
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		
	}
		
	
}
