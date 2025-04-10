package com.abhay;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileSplit;

public class WC_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        // Get the document ID (filename)
        FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
        String[] fileName = fileSplit.getPath().getName().split("\\.", 2);


        // Read the line and split into words
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        int index = 0;  // Initialize index as an integer

        // Emit (Document-ID, (Index, Word))
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken();
            output.collect(new Text(fileName[0]), new Text(index + "," + word));
            index++; // Increment index
        }
    }
}
