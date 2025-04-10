package com.abhay;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

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

public class DocumentFrequency {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text docid = new Text();
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //Executed once per Mapper task before processing any lines
//            used to load stop words from HDFS
            stopWords = new HashSet<>(); // Initialize stopWords set

            // Get HDFS configuration
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path stopWordsPath = new Path("hdfs://localhost:9000/user/abhay/assignment2/stopword/stopwords.txt"); // HDFS Path

            // Read stopwords file from HDFS
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(stopWordsPath)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim()); // Add stopwords to set
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            PorterStemmer stemmer = new PorterStemmer();
            String line = value.toString();
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            docid.set(fileName);

            if (!line.isEmpty()) {
                SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
                String tokenizedLine[] = tokenizer.tokenize(line); // Tokenize line
                //If line is not empty, tokenize it using Apache OpenNLP's SimpleTokenize
                for (String s : tokenizedLine) {
                    if (!stopWords.contains(s)) {
                        String stemmedOutput = stemmer.stem(s.toLowerCase());
                        word.set(stemmedOutput);
                        context.write(word, docid);
                        //(word, docid) → (word, filename)
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, IntWritable> {
        // input 1 text word
        //        2 document id
        // output   1 word
        //          2 #unque documents apear
        private IntWritable result = new IntWritable();
        // document frequency count

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //Runs once per unique word
            //Counts how many unique documents contain this word
            Set<String> docids = new HashSet<>();

            // Checking for unique documents
            for (Text val : values) {
                docids.add(val.toString());
            }
            result.set(docids.size());
            context.write(key, result);
            // word --> #times
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "document frequency");

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
