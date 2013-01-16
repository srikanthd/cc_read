package com.intuit.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class RandomFileGenerator {
	
	//Generate a random sub list of arc.gz files given the whole list of arc.gz files.
	
	public static void generateRandomFiles(int randN,String randFile)
	{
  //  	String fileName =  "C:\\Users\\rgrandhi\\workspace\\crawlproj\\src\\com\\intuit\\util\\2010_arcfiles.txt";	
    	String fileName =  "C:\\Users\\rgrandhi\\workspace\\crawlproj\\src\\com\\intuit\\util\\2012_arcfiles.txt";	
	
		List<String> fileList = new ArrayList<String>();
		
		//Load all the files from the given list.
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(fileName));
			
			String line= "";
			
			while((line=reader.readLine())!=null)
			{
				fileList.add(line);
				
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	
		Set<Integer> selectedInts = new HashSet<Integer>();
		
		Random rand = new Random();
		//Select random indices from the list making sure that we pick unique keys.
		while(selectedInts.size()<randN){
		     int selectedInd = rand.nextInt(fileList.size());
		        if(!selectedInts.contains(selectedInd)) selectedInts.add(selectedInd);
		 }
		
		
		System.out.println("number of selected ints:::" + selectedInts.size());
		
		//output the selected random files into a new file.
		try {
			PrintWriter writer = new PrintWriter(new FileWriter(randFile));
			
			for (Iterator iterator = selectedInts.iterator(); iterator
					.hasNext();) {
				Integer curInd = (Integer) iterator.next();
				writer.println("s3n://aws-publicdatasets/" + fileList.get(curInd));
				
				
			}
			
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
	
	public static void main(String[] args)
	{
		
	 //	String randFile = 	"C:\\Users\\rgrandhi\\workspace\\crawlproj\\src\\com\\intuit\\util\\2010_rand_1400_1.txt";	
	
		String randFile = 	"C:\\Users\\rgrandhi\\workspace\\crawlproj\\src\\com\\intuit\\util\\2012_rand_27800_1.txt";	

		generateRandomFiles(27800,randFile);
		
	}
	
	
	

}
