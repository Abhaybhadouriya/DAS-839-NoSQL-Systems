package com.abhay;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top50MatrixBuild {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Set<String> frequentWords = new HashSet<>();
        private IntWritable one = new IntWritable(1);
        private int d;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            d = conf.getInt("windowSize", 1); // Read window size from configuration

            FileSystem fs = FileSystem.get(conf);
            Path frequentWordsPath = new Path("hdfs://localhost:9000/outputforQ4P1OP1/part-r-00000");

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(frequentWordsPath)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    frequentWords.add(parts[0].trim());
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            int size = tokenizer.countTokens();
            String[] words = new String[size];

            for (int i = 0; i < size; i++) {
                words[i] = tokenizer.nextToken().toLowerCase();
            }

            for (int i = 0; i < size; i++) {
                if (!frequentWords.contains(words[i])) continue;

                for (int j = i + 1; j < Math.min(i + d + 1, size); j++) {
                    if (!frequentWords.contains(words[j])) continue;
                    String pair = words[i] + "," + words[j];
                    context.write(new Text(pair), one);
                }
            }
// Input:  "data science is great for machine learning"
//Frequent Words: {data, science, machine, learning}
//Pairs emitted:
//(data, science) → 1
//(science, machine) → 1
//(machine, learning) → 1
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
//        (data, science) → 12
//(science, machine) → 8
//(machine, learning) → 15
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis(); // Start time
        Configuration conf = new Configuration();
        conf.setInt("windowSize", Integer.parseInt(args[2])); // Pass d as third argument

        Job job = Job.getInstance(conf, "Co-Occurrence Matrix Pairs");

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis(); // End time
        long elapsedTime = (endTime - startTime) / 1000; // Convert to seconds

        System.out.println("Execution time for d=" + args[2]);
        System.exit(success ? 0 : 1);
    }
}
