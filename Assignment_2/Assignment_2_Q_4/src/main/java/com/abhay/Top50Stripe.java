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

public class Top50Stripe {

    public static class StripeMapper extends Mapper<Object, Text, Text, MapWritable> {
        private int windowSize ; // Default window size

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            windowSize = conf.getInt("windowSize", 2); // Get window size from config
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
                MapWritable stripe = new MapWritable();

                for (int j = Math.max(0, i - windowSize); j < Math.min(words.length, i + windowSize + 1); j++) {
                    if (i != j) { // Avoid self co-occurrence
                        Text neighbor = new Text(words[j]);
                        if (stripe.containsKey(neighbor)) {
                            IntWritable count = (IntWritable) stripe.get(neighbor);
                            count.set(count.get() + 1);
                        } else {
                            stripe.put(neighbor, new IntWritable(1));
                        }
                    }
                }
                //data science is great for machine learning
                //data      → { science: 1 }
                //science   → { data: 1, is: 1 }
                //is        → { science: 1, great: 1 }
                //great     → { is: 1, for: 1 }
                //for       → { great: 1, machine: 1 }
                //machine   → { for: 1, learning: 1 }
                //learning  → { machine: 1 }
                context.write(word, stripe);
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
            //data      → { science: 1 }
            //science   → { data: 1, is: 1 }
            //science   → { data: 1, is: 1 }

            //data      → { science: 1 }
            //science   → { data: 2, is: 2 }
            //is        → { science: 1, great: 1 }
            //great     → { is: 1, for: 1 }

            context.write(key, aggregatedStripe);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("windowSize", Integer.parseInt(args[2])); // Pass d as command-line argument

        Job job = Job.getInstance(conf, "Co-Occurrence Stripe");
        job.setJarByClass(Top50Stripe.class);
        job.setMapperClass(StripeMapper.class);
        job.setReducerClass(StripeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input Path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output Path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
