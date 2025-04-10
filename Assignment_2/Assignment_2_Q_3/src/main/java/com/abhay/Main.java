package com.abhay;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(Main.class);
        conf.setJobName("WikipediaWordProcessor");

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(TextArrayWritable.class);  // ✅ Use custom Writable

        conf.setMapperClass(TP_Mapper.class);
        conf.setCombinerClass(TP_Combiner.class);
        conf.setReducerClass(TP_Reducer.class);

        conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
        conf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
