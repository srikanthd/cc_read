package com.intuit.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Used to connect to S3 and generate the complete list of arc.gz files for 2010 and
 * 2012 commoncrawl data sets.
 * 
 * 
 * @author rgrandhi
 *
 */


public class S3Sample {
	
	//Read the valid segment ids for 2012 data.
	public static Set<String> readValidSegments()
	{
		String file = "C:\\Users\\rgrandhi\\workspace\\crawlproj\\src\\com\\intuit\\util\\valid_segments.txt";	
		
		Set<String> validSegments = new HashSet<String>();
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = "";
			
			while((line=reader.readLine())!=null)
			{
				validSegments.add(line);
				
				
			}
			
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return validSegments;
		
		
		
	}

	
	//Generate 2010 list of arc.gz files.
	public static void generate2010ArcFiles()
	{

		String prefix = "common-crawl/crawl-002/2010/";
    	String fileName =  "C:\\Users\\rgrandhi\\workspace\\crawlproj\\src\\com\\intuit\\util\\" + "2010_arcfiles.txt";
    	
    	
		List<S3ObjectSummary> keyList = extractPrefixFiles(prefix);
    	PrintWriter writer;
		try {
			writer = new PrintWriter(new FileWriter(fileName));
	    	for (Iterator iterator = keyList.iterator(); iterator.hasNext();) {
				S3ObjectSummary s3ObjectSummary = (S3ObjectSummary) iterator
						.next();
				String arcFileName = s3ObjectSummary.getKey();
							
				if(arcFileName.indexOf("arc.gz")>=0)
				{
					writer.println(arcFileName);
				}
				
		} 
	    	
			writer.close();

		}	catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	

		
		}
    	
    
	
	//Generate 2012 list of arc.gz files using valid segment ids.
	public static void generate2012ArcFiles()
	{

		String prefix = "common-crawl/parse-output/segment/";
    	String fileName =  "C:\\Users\\rgrandhi\\workspace\\crawlproj\\src\\com\\intuit\\util\\" + "2012_arcfiles.txt";
    	Set<String> validSegments = readValidSegments();
    	List<S3ObjectSummary> keyList = new ArrayList<S3ObjectSummary>();
    	
    	//Iterate through each of the paths of valid segments and generate the list of arc.gz files within each path.
    	for (Iterator iterator = validSegments.iterator(); iterator.hasNext();) {
			String segID = (String) iterator.next();
			String curPrefix = prefix + segID + "/";
			System.out.println("Processing...." + curPrefix);
					
			List<S3ObjectSummary> curKeyList = extractPrefixFiles(curPrefix);
			keyList.addAll(curKeyList);
    	}
    	
    	//print the list of arc.gz files into the output file.
    	PrintWriter writer;
		try {
			writer = new PrintWriter(new FileWriter(fileName));
	    	for (Iterator iter = keyList.iterator(); iter.hasNext();) {
				S3ObjectSummary s3ObjectSummary = (S3ObjectSummary) iter
						.next();
				String arcFileName = s3ObjectSummary.getKey();
							
				if(arcFileName.indexOf("arc.gz")>=0)
				{
					writer.println(arcFileName);
				}
			
				
		} 
	    	
			writer.close();

		}	catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	

		
		}
    	
		
	
	//Given a prefix path, generate the list of all files in the S3 which has that path as prefix.
	
	public static List<S3ObjectSummary> extractPrefixFiles(String prefix)
	{
		
        AmazonS3 s3=null;
        List<S3ObjectSummary> keyList = null;
		try {
			s3 = new AmazonS3Client(new PropertiesCredentials(
			        S3Sample.class.getResourceAsStream("AwsCredentials.properties")));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

       
        try {
           
        	String bucketName = "aws-publicdatasets";
        				
        	ObjectListing current = s3.listObjects(bucketName,prefix);
        	keyList = current.getObjectSummaries();
        	
        	System.out.println(keyList.size());
        
        	
        	ObjectListing next = s3.listNextBatchOfObjects(current);
        	keyList.addAll(next.getObjectSummaries());

        	//Iterate through the set of batch objects until no new files exist with the given prefix.
        	while (next.isTruncated()) {
        	   current=s3.listNextBatchOfObjects(next);
        	   keyList.addAll(current.getObjectSummaries());
        	   next =s3.listNextBatchOfObjects(current);
        	   System.out.println("number of entries:::" + keyList.size());
        	}
        	keyList.addAll(next.getObjectSummaries());
        	
           
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }

		
		
		return keyList;
		
		
	}
	
    public static void main(String[] args) throws IOException {
    	
    	//generate2010ArcFiles();
    	generate2012ArcFiles();
    }

}
