package com.abhay;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.io.IOException;
import java.util.Iterator;

public class TP_Reducer extends MapReduceBase implements Reducer<IntWritable, TextArrayWritable, IntWritable, Text> {

    public void reduce(IntWritable key, Iterator<TextArrayWritable> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
        StringBuilder result = new StringBuilder();
        long maxTimestamp = Long.MIN_VALUE;
        String maxWord = "";
        while (values.hasNext()) {
            Writable[] currentArray = values.next().get();
//            for (Writable w : currentArray) {
////                if (result.length() > 0) {
////                    result.append("; ");
////                }
////                result.append(w.toString());
//                String value = w.toString();
//                String[] parts = value.split(",", 2);
                long timestamp = Long.parseLong(currentArray[0].toString().replaceAll("[()]", "").trim());
////                String word = parts[1].trim();
//
                if (timestamp > maxTimestamp) {
                    maxTimestamp = timestamp;
////                    maxWord = word;
                    maxWord = currentArray[1].toString().replaceAll("[()]", "").trim();
                }
//            }
//            result.append(currentArray.toString());
        }

        output.collect(key, new Text("( "+maxTimestamp+ ","+maxWord+")"));
    }
}
