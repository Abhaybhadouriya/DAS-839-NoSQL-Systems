package com.abhay;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class TP_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, TextArrayWritable> {

    public void map(LongWritable key, Text value, OutputCollector<IntWritable, TextArrayWritable> output, Reporter reporter) throws IOException {
        String[] parts = value.toString().split("\t");  // Split index and (documentId, word)

        if (parts.length < 2) return;

        IntWritable index = new IntWritable(Integer.parseInt(parts[0]));
        String[] wordData = parts[1].replaceAll("[()]", "").split(", "); // Remove brackets and split

        if (wordData.length < 2) return;

        Text docId = new Text(wordData[0]);
        Text word = new Text(wordData[1]);

        Text[] outputArray = {docId, word};
        TextArrayWritable arrayWritable = new TextArrayWritable(outputArray);

        output.collect(index, arrayWritable);
    }
}
