package rucha_tfidf;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by rucha on 4/20/18.
 */
public class WikiWordTFIDFIndex {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // set NUMBER_OF_DOCS from args[1]
        AmazonS3 s3 = new AmazonS3Client(new BasicAWSCredentials("ss", "ss"));
        S3Object s3object = s3.getObject(new GetObjectRequest("map-reduce-course", args[1]));
        System.out.println(s3object.getObjectMetadata().getContentType());
        System.out.println(s3object.getObjectMetadata().getContentLength());

        BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
//        FileSystem fs = FileSystem.get(conf);
//        FSDataInputStream in = fs.open(new Path(args[1]));
//        String line = in.readLine();

        String line = reader.readLine();

        String[] textAndCount = line.split("\t");
        int NUMBER_OF_DOCS = Integer.parseInt(textAndCount[1]);

        // set NUMBER_OF_DOCS
        conf.setInt("NUMBER_OF_DOCS", NUMBER_OF_DOCS);

        // set MAX_NUMBER_OF_RESULTS
        conf.setInt("MAX_NUMBER_OF_RESULTS", 20);

        Job job = new Job(conf, "WikiWordTFIDFIndex");

        job.setJarByClass(WikiWordTFIDFIndex.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WikiWordInfo.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, WikiWordInfo> {

        private WikiWordInfo map_val;
        private Text map_key;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            Integer NUMBER_OF_DOCS = conf.getInt("NUMBER_OF_DOCS", 1);
            Integer MAX_NUMBER_OF_RESULTS = conf.getInt("MAX_NUMBER_OF_RESULTS", 1);
            map_val = new WikiWordInfo(NUMBER_OF_DOCS, MAX_NUMBER_OF_RESULTS);
            map_key = new Text();
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] doc_id_and_content = line.split("\t");

            if (doc_id_and_content.length == 2) {
                String _id = doc_id_and_content[0];

                Enumeration<String> t = new WikiWordTokenizer(doc_id_and_content[1]);

                HashMap<String, Integer> wordCounterMap = new HashMap<String, Integer>();

                int max_count_of_word = 0;
                while (t.hasMoreElements()) {
                    String next_word = t.nextElement();

                    Integer count = wordCounterMap.get(next_word);
                    if (count != null) {
                        count += 1;
                    } else {
                        count = 1;
                    }

                    wordCounterMap.put(next_word, count);

                    if (count > max_count_of_word) max_count_of_word = count;
                }

                for (java.util.Map.Entry<String, Integer> kv : wordCounterMap.entrySet()) {
                    String _word = kv.getKey();
                    int _freq = kv.getValue();
                    float _tf = 0.5f + 0.5f * _freq / max_count_of_word;  /* ! TF */
                    map_val.set(_id, _tf, _freq);
                    map_key.set(_word);
                    context.write(map_key, map_val);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, WikiWordInfo, Text, WikiWordInfo> {
        private WikiWordInfo map_val;
        private WikiWordInfoUpdater updater;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            Integer NUMBER_OF_DOCS = conf.getInt("NUMBER_OF_DOCS", 1);
            Integer MAX_NUMBER_OF_RESULTS = conf.getInt("MAX_NUMBER_OF_RESULTS", 1);
            map_val = new WikiWordInfo(NUMBER_OF_DOCS, MAX_NUMBER_OF_RESULTS);
            updater = new WikiWordInfoUpdater();
        }

        protected void reduce(Text key, Iterable<WikiWordInfo> values, Context context) throws IOException, InterruptedException {
            updater.reset();

            for (WikiWordInfo val : values) {
                updater.add(val);
            }

            updater.update(map_val);
            context.write(key, map_val);
        }


    }
}
