package com.abhay;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.security.KeyStore.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;

import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.stemmer.PorterStemmer;

public class DocumentFrequencyTop100 {
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
// input - line number --- line text
        // out put-- key and map data
        private Set<String> stopWords = new HashSet<>();
        PorterStemmer stemmer = new PorterStemmer();

        public void setup(Context context) throws IOException, InterruptedException {
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
//            URI[] cacheFiles = context.getCacheFiles();
//            if (cacheFiles != null && cacheFiles.length > 0) {
//                try (BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()))) {
//                    String line;
//                    while ((line = reader.readLine()) != null) {
//                        stopWords.add(line.trim());
//                    }
//                }
//            }
        }





        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //map runs for each line of input.
            String line = value.toString();
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            MapWritable tfMap = new MapWritable();
            //instead of HashMap<String, Integer>
            //because Hadoop's serialization system requires Writable objects
//		    Map<String, Integer> tfMap = new HashMap<>();
            // wrong


            if (line != null) {
                SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
                String tokenizedLine[] = tokenizer.tokenize(line); //Tokenize line
                //Why OpenNLP's SimpleTokenizer?
                //    Preserves contractions ("don't" → ["don't"]).
                //    Keeps punctuation separate ("hello." → ["hello", "."])

                for(String s: tokenizedLine) {
                    if (!stopWords.contains(s)){
                        String word = stemmer.stem(s.toLowerCase());
                        if(tfMap.containsKey(new Text(word))) {
                            IntWritable temp =(IntWritable) tfMap.get(new Text(word));
                            int x = temp.get();
                            x++;
                            temp.set(x);
                            tfMap.put(new Text(word), temp);
                            // use put to update
                        }
                        else {
                            tfMap.put(new Text(word), new IntWritable(1));
                        }

                    }
                }



                context.write(new Text(fileName), tfMap);
                // Sends (fileName, tfMap) to reducer


            }



        }
    }

    public static class IntSumReducer extends Reducer<Text, MapWritable, Text, DoubleWritable> {
        private Map<String, Integer> dfMap = new HashMap<>();
        //Stores document frequency (word → #documents it appears in)
        LinkedHashMap<String, Integer> finaldf = new LinkedHashMap<String, Integer>();
        //Stores top 100 most frequent words
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        //runs once per Reducer task before reduce()
            //reads document frequency (df) values from HDFS and selects the top 100 most frequent words

            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            Path stopWordsPath = new Path("hdfs://localhost:9000/outputforQ5P30/part-r-00000"); // HDFS Path


//
//            URI[] files = context.getCacheFiles();
//            Path dfPath = new Path(files[1].getPath());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(stopWordsPath)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    String term = parts[0];
                    int df = Integer.parseInt(parts[1]);
                    dfMap.put(term, df);

                }

                List <Map.Entry<String, Integer>> capitalList = new LinkedList<>(dfMap.entrySet());

                // call the sort() method of Collections
                Collections.sort(capitalList, (l1, l2) -> l1.getValue().compareTo(l2.getValue()));


                int i = 0;
                for (java.util.Map.Entry<String, Integer> entry : capitalList) {
                    if (i < 100) {
                        finaldf.put(entry.getKey(), entry.getValue());
                        i++;
                    }
                    else break;

                }

            }

        }
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            //Runs for each unique document name (key)
            MapWritable ret = new MapWritable();
            //Stores combined word counts across all Mapper outputs
            for (MapWritable value : values){

                for(MapWritable.Entry<Writable, Writable> e : value.entrySet()){
                    //iterates over each word’s frequency map received from Mappers
                    if (ret.containsKey(e.getKey())){
                        int i = ((IntWritable) e.getValue()).get();
                        int j = ((IntWritable) ret.get(e.getKey())).get();
                        ret.put(e.getKey(), new IntWritable(i+j));
                    } else {
                        ret.put(e.getKey(), e.getValue());
                    }
                }
            }



            //Calculating TF-IDF Scores
            //Iterates over all words in the aggregated TF map
            for (MapWritable.Entry<Writable, Writable> entry : ret.entrySet()) {
                String term = entry.getKey().toString();
                int tf = ((IntWritable) entry.getValue()).get();

                int df = finaldf.getOrDefault(term, 0);
                //Gets document frequency (df) from finaldf

                if (df == 0) continue;
                double score = tf * Math.log10(10000.0 / (df + 1));
                //10000 is the total number of documents (assumed)
                //+1 in the denominator prevents division by zero
                result.set(score);
                String k = key + "\t" + term;
                context.write(new Text(k), result);


            }



        }

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //object that stores all job-specific settings
        //Creating Configuration
        Job job = Job.getInstance(conf, "document frequency");
        //Creates a new Hadoop job instance


        //Add link to Stopwords.txt
//        job.addCacheFile(new URI("/home/abhay/Desktop/MinniProject/Mini-Project/DATA/stopword/stopwords.txt"));
//        job.addCacheFile(new URI("/home/abhay/Desktop/MinniProject/Mini-Project/DATA/stopword/part-r-00000"));
        //

//        job.addCacheFile(new Path(args[0]).toUri());
//        job.addCacheFile(new Path(args[0]).toUri());

        //Extracts words from documents
        job.setMapperClass(TokenizerMapper.class);
        // aggregates and computes document frequency
        job.setReducerClass(IntSumReducer.class);


        // key text Document name
        job.setOutputKeyClass(Text.class);
        //map like word-> freq class
        job.setOutputValueClass(MapWritable.class);
        //	    job.setOutputFormatClass(OutputFormat.class);

        // input output path in HDFS
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //Enables verbose logging
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}