package Homework2.SiCombiner;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.*;

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

public class WordCountSiCombiner {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (Pattern.matches("[m-qM-Q].*?", word.toString())) {
                    context.write(word, one);
                }
            }
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
        job.setJarByClass(WordCountSiCombiner.class);
        job.setMapperClass(WordCountSiCombiner.TokenizerMapper.class);
        job.setCombinerClass(WordCountSiCombiner.IntSumReducer.class); // Combiner
        job.setReducerClass(WordCountSiCombiner.IntSumReducer.class);
        job.setNumReduceTasks(5); // Number of reduce tasks
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setPartitionerClass(customPartitioner.class);// Custom partitioner

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
