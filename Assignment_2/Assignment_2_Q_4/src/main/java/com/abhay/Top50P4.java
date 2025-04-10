package com.abhay;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top50P4 {

    public static class StripeMapper extends Mapper<Object, Text, Text, MapWritable> {
        private Map<Text, MapWritable> globalStripe = new HashMap<>();
        private int windowSize = 2;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            windowSize = conf.getInt("windowSize", 2);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String[] words = new String[tokenizer.countTokens()];
            int index = 0;

            while (tokenizer.hasMoreTokens()) {
                words[index++] = tokenizer.nextToken();
            }

            for (int i = 0; i < words.length; i++) {
                Text word = new Text(words[i]);
                MapWritable stripe = globalStripe.getOrDefault(word, new MapWritable());

                for (int j = Math.max(0, i - windowSize); j < Math.min(words.length, i + windowSize + 1); j++) {
                    if (i != j) {
                        Text neighbor = new Text(words[j]);
                        IntWritable count = (IntWritable) stripe.getOrDefault(neighbor, new IntWritable(0));
                        count.set(count.get() + 1);
                        stripe.put(neighbor, count);
//                System.out.println("word: " + neighbor+" Count "+count);
                    }
                }
                globalStripe.put(word, stripe);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Text, MapWritable> entry : globalStripe.entrySet()) {
                context.write(entry.getKey(), entry.getValue());
            }
        }
    }

    public static class StripeReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable aggregatedStripe = new MapWritable();

            for (MapWritable stripe : values) {
                for (Writable neighbor : stripe.keySet()) {
                    IntWritable count = (IntWritable) stripe.get(neighbor);
                    if (aggregatedStripe.containsKey(neighbor)) {
                        IntWritable total = (IntWritable) aggregatedStripe.get(neighbor);
                        total.set(total.get() + count.get());
                    } else {
                        aggregatedStripe.put(neighbor, new IntWritable(count.get()));
                    }
                }
            }

            context.write(key, aggregatedStripe);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("windowSize", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "Co-Occurrence Stripe - Mapper Class Level");
        job.setJarByClass(Top50P4.class);
        job.setMapperClass(StripeMapper.class);
        job.setReducerClass(StripeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
