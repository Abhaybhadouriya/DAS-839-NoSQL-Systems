package com.abhay;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.io.IOException;
import java.util.*;

public class TP_Combiner extends MapReduceBase implements Reducer<IntWritable, TextArrayWritable, IntWritable, TextArrayWritable> {

    public void reduce(IntWritable key, Iterator<TextArrayWritable> values, OutputCollector<IntWritable, TextArrayWritable> output, Reporter reporter) throws IOException {
        List<Text> collectedValues = new ArrayList<>();

        while (values.hasNext()) {
            Writable[] currentArray = values.next().get();
            for (Writable w : currentArray) {
                collectedValues.add((Text) w);
            }
        }

        TextArrayWritable resultArray = new TextArrayWritable(collectedValues.toArray(new Text[0]));

        output.collect(key, resultArray);
    }
}
