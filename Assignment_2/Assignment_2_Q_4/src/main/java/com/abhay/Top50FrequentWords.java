package com.abhay;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Comparator;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.stemmer.PorterStemmer;

public class Top50FrequentWords {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path stopWordsPath = new Path("hdfs://localhost:9000/user/abhay/assignment2/stopword/stopwords.txt"); // HDFS Path

            // Read stopwords file from HDFS
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(stopWordsPath)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim());

                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            PorterStemmer stemmer = new PorterStemmer();
            String line = value.toString();
            if (!line.isEmpty()) {
                SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
                String[] tokenizedLine = tokenizer.tokenize(line);

                for (String s : tokenizedLine) {
//                    System.out.println("Stopwords: " + s);

                    if (!stopWords.contains(s)) {
                        //                   below code if for capital letter also
                        //                    if (!stopWords.contains(s.toLowerCase())) {
//                        System.out.println("-"+s+"-");
                        String stemmedWord = stemmer.stem(s.toLowerCase());
                        word.set(stemmedWord);
                        context.write(word, one);


                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String, Integer> wordCountMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            wordCountMap.put(key.toString(), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Sort words by frequency in descending order
            TreeMap<String, Integer> sortedMap = new TreeMap<>(new Comparator<String>() {
                public int compare(String a, String b) {
                    return wordCountMap.get(b).compareTo(wordCountMap.get(a));
                }
            });

            sortedMap.putAll(wordCountMap);
            int count = 0;

            // Output only top 50 words
            for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
                if (count < 50) {
                    context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
                    count++;
                } else {
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 50 Frequent Words");

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
