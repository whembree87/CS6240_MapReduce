package wiki_tfidf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TFIDFCalculator {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Brute-Force : Fetch total document count from S3 bucket. Requires NumberOfDocs to be run first.
        /*
        // set NUMBER_OF_DOCS from args[1]
        AmazonS3 s3 = new AmazonS3Client(new BasicAWSCredentials("----", "-----"));
        S3Object s3object = s3.getObject(new GetObjectRequest("map-reduce-course", args[1]));
        System.out.println(s3object.getObjectMetadata().getContentType());
        System.out.println(s3object.getObjectMetadata().getContentLength());
        BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
        String line = reader.readLine();
        */

        //// Brute-Force : Readh file from HDFS. Requires NumberOfDocs to be run first.
        /*
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path(args[1]));
        String line = in.readLine();
        */

        // Brute-Force : Fetch total doc. count locally and set total number of docs in conf
        /*
        String[] textAndCount = line.split("\t");
        int NUMBER_OF_DOCS = Integer.parseInt(textAndCount[1]);
        // set NUMBER_OF_DOCS
        conf.setInt("NUMBER_OF_DOCS", NUMBER_OF_DOCS);
        */

        // set MAX_NUMBER_OF_RESULTS
        conf.setInt("MAX_NUMBER_OF_RESULTS", 20);

        Job job = new Job(conf, "TFIDFCalculator");

        job.setJarByClass(TFIDFCalculator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WordInformation.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Brute-Force :
        // Arguments 1) Path to input 2) Path to total doc. count  3) output directory
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, WordInformation> {

        private WordInformation map_val;
        private Text map_key;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            // Brute-Force : Retrieve total doc. count from conf.
            /*
            Integer NUMBER_OF_DOCS = conf.getInt("NUMBER_OF_DOCS", 1);
            map_val = new WordInformation(NUMBER_OF_DOCS, MAX_NUMBER_OF_RESULTS);
            */
            Integer MAX_NUMBER_OF_RESULTS = conf.getInt("MAX_NUMBER_OF_RESULTS", 1);
            map_val = new WordInformation(0, MAX_NUMBER_OF_RESULTS);
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
                map_val.set("Count", (float) -0.1,1);
                map_key.set("##getdoccount##");
                context.write(map_key, map_val);
            }
        }
    }

    public static class Reduce extends Reducer<Text, WordInformation, Text, WordInformation> {
        private WordInformation map_val;
        private WordInformationUpdater updater;
        int docCount;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            Integer MAX_NUMBER_OF_RESULTS = conf.getInt("MAX_NUMBER_OF_RESULTS", 1);
            // Brute-Force : Retrieve total doc. count from conf.
//            map_val = new WordInformation(NUMBER_OF_DOCS, MAX_NUMBER_OF_RESULTS);
            map_val = new WordInformation(0, MAX_NUMBER_OF_RESULTS);
            updater = new WordInformationUpdater();
        }

        // Brute-Force : Would not need below key comparator
        public static class KeyComparator extends WritableComparator {
            protected KeyComparator() {
                super(Text.class, true);
            }

            public int compare(Text w1, Text w2) {
                if(w1.equals("##getdoccount##")) {
                    return -1;
                } else if(w2.equals("##getdoccount##")) {
                    return 1;
                } else {
                    return w1.compareTo(w2);
                }
            }
        }

        protected void reduce(Text key, Iterable<WordInformation> values, Context context) throws IOException, InterruptedException {
            // Brute-Force : Would not need this if condition
            if(key.equals(new Text("##getdoccount##"))) {
                for(WordInformation val:values) {
                    WordInformationEntry[] entries = val.getEntries();
                    for(WordInformationEntry entry: entries) {
                        docCount += entry.freq;
                    }
                }
                map_val.setNUMBER_OF_DOCS(docCount);
            } else {
                updater.reset();

                for (WordInformation val : values) {
                    updater.add(val);
                }

                updater.update(map_val);
                context.write(key, map_val);
            }
        }


    }
}
