package com.intuit.mapreduce;


import java.io.*;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;


public class Reduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long sum1 = 0, sum2 = 0, sum3 = 0, sum4 = 0, sum5 = 0;
        String text = "";
        Iterator iter = values.iterator();
	
        // Summates all word counts for this word.
        while (iter.hasNext()) {
		
            String s[] = (iter.next().toString()).split("///");

            sum1 += Integer.parseInt(s[0]);
            sum2 += Integer.parseInt(s[1]);
            sum3 += Integer.parseInt(s[2]);
            sum4 += Integer.parseInt(s[3]);
            sum5 += Integer.parseInt(s[4]);
            if (s.length > 5) {
                text = text + s[5];
            }
        }
        context.write(key,
                new Text(
                sum1 + "///" + sum2 + "///" + sum3 + "///" + sum4 + "///" + sum5
                + "///" + text));
    }
}