package Homework2.PerTaskTally;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class WordCountPerTaskTally {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        // Declare hashmap at task level
        private HashMap<Text, IntWritable> hmap;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            HashMap<Text, IntWritable> hmap = setup();
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                Text word = new Text();
                word.set(itr.nextToken());
                // Add all "real" words to hashmap with counts
                if (Pattern.matches("[m-qM-Q].*?", word.toString())) {
                    if (hmap.containsKey(word)) {// Word is present
                        IntWritable total = hmap.get(word);
                        int updatedTotal = total.get() + 1;
                        total.set(updatedTotal); // Update total
                        hmap.put(word, total);
                    } else { // New word
                        IntWritable count = new IntWritable(1);
                        hmap.put(word, count);
                    }
                }
            }
        }

        protected void cleanup (Mapper.Context context) throws IOException, InterruptedException {
            // Emit each word in hashmap along with its count
            Iterator<HashMap.Entry<Text, IntWritable>> hMapItr = hmap.entrySet().iterator();

            while(hMapItr.hasNext()) {
                HashMap.Entry<Text, IntWritable> entry = hMapItr.next();
                context.write(entry.getKey(), entry.getValue());
            }
        }

        public HashMap<Text, IntWritable> setup () {
            if (null == hmap) {
                hmap = new HashMap<Text, IntWritable>();
            }
            return hmap;
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class customPartitioner extends Partitioner<Text, IntWritable> {
        public int getPartition (Text key, IntWritable value, int numReduceTasks) {
            if(Pattern.matches("[mM].*?", key.toString())) {
                return 0;
            }
            if(Pattern.matches("[nN].*?", key.toString())) {
                return 1;
            }
            if(Pattern.matches("[oO].*?", key.toString())) {
                return 2;
            }
            if(Pattern.matches("[pP].*?", key.toString())) {
                return 3;
            }
            // "q" or "Q"
            else
                return 4;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "word count");
        job.setJarByClass(Homework2.PerTaskTally.WordCountPerTaskTally.class);
        job.setMapperClass(Homework2.PerTaskTally.WordCountPerTaskTally.TokenizerMapper.class);
        //job.setCombinerClass(Homework2.PerTaskTally.WordCountPerTaskTally.IntSumReducer.class); // Combiner
        job.setReducerClass(Homework2.PerTaskTally.WordCountPerTaskTally.IntSumReducer.class);
        job.setNumReduceTasks(5); // Number of reduce tasks
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setPartitionerClass(Homework2.PerTaskTally.WordCountPerTaskTally.customPartitioner.class);// Custom partitioner

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

