package com.abhay;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WC_Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        while (values.hasNext()) {
            String value = values.next().toString(); // "index,word"

            // Split values
            String[] parts = value.split(",", 2);
            if (parts.length == 2) {
                String index = parts[0]; // Keep index as a string
                String word = parts[1];

                // Emit (Document-ID, (Index, Word))
                output.collect(new Text(index), new Text("(" + key.toString() + ", " + word + ")"));
            }
        }
    }
}
