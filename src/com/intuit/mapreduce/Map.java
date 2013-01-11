
package com.intuit.mapreduce;
 

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
 

public class Map extends Mapper<LongWritable, Text, Text, Text> {
  
    static String line = null;
    static String c_url = null;   
 
    Text curlText = new Text();
    Text resText = new Text();
 
    String cwurldom;
    String cwurlhost;
  
    int outindomcnt = 0;
    int outsubdomcnt = 0;
    int totoutboundcnt = 0;
    int jslinkscnt = 0;
    int indomaincnt = 0;
    String title = "";
    String h1 = "";
    String text = "";
    String anc = "";
   
    String[] temp;
   
    // String regex_links = "<a\\s[^>]*href\\s*=\\s*\"([^\"]*)\"[^>]*>(.*?)</a>";
    Pattern linkpattern = Pattern.compile("href=\"([^\"]*)\"");
    Pattern titlepattern = Pattern.compile("\\<title>(.*)\\</title>");
    Pattern h1pattern = Pattern.compile("\\<h1>(.*)\\</h1>");
    Pattern ahrefpattern = Pattern.compile("<a href=\"#(\\w+)\">([\\w\\s]+)</a>");
 
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
                
    static int i, j;
  
    public static boolean getSubString(String anc, String title) {
                  
        String a[] = anc.split(".");
        String t[] = title.split(".");

        for (i = 0; i < a.length; i++) {
            for (j = 0; j < t.length; j++) {
                if (a[i].equalsIgnoreCase(t[j])) {
                    return true;
                }                                                            
            }
        }
        return false;
    }
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String content = value.toString();
            ByteArrayInputStream in = new ByteArrayInputStream(
                    content.getBytes());
            BufferedReader br = new BufferedReader(new InputStreamReader(in)); 
             
            while ((line = br.readLine()) != null) {
                
                if (line.contains("x_commoncrawl_OriginalURL")) {
                    if (c_url != null) {
                        text = "title:"+title + "**h1:" + h1;
                        curlText.set(c_url);                      
                        resText.set(
                                indomaincnt + "///" + totoutboundcnt + "///"
                                + outindomcnt + "///" + outsubdomcnt + "///"
                                + jslinkscnt + "///" + text);
                        outindomcnt = 0;
                        outsubdomcnt = 0;
                        totoutboundcnt = 0;
                        jslinkscnt = 0;
                        indomaincnt = 0;
                        c_url = null;
                        title = "";
                        h1 = "";
                        text = "";
                        context.write(curlText, resText);
                    }
                    temp = line.split(":");
                    c_url = temp[1] + ":" + temp[2];
                    cwurlhost = getHost(c_url);
                    cwurldom = getBaseDomain(c_url);
                    temp = null;
                } else {
                   
                    if (line.contains("<a href=")) {
                       
                        Matcher mat = ahrefpattern.matcher(line);

                        while (mat.find()) {
                            anc = mat.group(2);   
                            anc = anc + "**";
                        }
                        Matcher m = linkpattern.matcher(line);                        

                        while (m.find()) {                            
                            totoutboundcnt++;                     
                            if (m.group(1).contains(cwurlhost)) {
                                outindomcnt++;
                            } else {
                                if (getBaseDomain(m.group(1)).equals(cwurldom)) {
                                    outsubdomcnt++;
                                } else {
                                    if (m.group(1).startsWith("/")
                                            || m.group(1).startsWith("../")
                                            || m.group(1).startsWith("#")) {
                                        outindomcnt++;
                                    } else if ((m.group(1).startsWith("http:"))
                                            && (!m.group(1).contains(cwurlhost))) {
                                        curlText.set(m.group(1));
                                        resText.set(
                                                "1///0///0///0///0" + "///"
                                                + "anctext:"+anc);
                                        context.write(curlText, resText);
                                    } else if (m.group(1).startsWith(
                                            "javascript:")) {
                                        jslinkscnt++;
                                    }
                                }
                            }
                        }
                    }
                    if (line.contains("<title>")) {                             
                        Matcher matcher = titlepattern.matcher(line);

                        while (matcher.find()) {
                            title = matcher.group(1).replaceAll("[\\s\\<>]+", " ").trim();                           
                        }     
                    }
                    if (line.contains("<h1>")) {
                        Matcher matcher = h1pattern.matcher(line);

                        while (matcher.find()) {
                            h1 = matcher.group(1).replaceAll("[\\s\\<>]+", " ").trim();                                 
                        }
                    }
                }
 
            }       
        } catch (Exception e) {}
    }
 
}
