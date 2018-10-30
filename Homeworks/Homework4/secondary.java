package Homework4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVParser;
import java.io.IOException;
import java.util.*;

public class secondary {

    public static class AvgDelayMapper extends
            Mapper<LongWritable, Text, FlightMonthPair, Text> {

        private CSVParser csvParser = new CSVParser(',', '"');

        protected void map(LongWritable key, Text value, Context context) throws
                IOException, InterruptedException {

            String[] line = this.csvParser.parseLine(value.toString());

            Text uniqueCarrier = new Text(line[6]);
            Text month = new Text(line[2]);
            Text arrDelayMinutes = new Text(line[37]);

            Boolean isYear2008 = line[0].equals("2008");
            Boolean isNotCancelled = !line[41].equals("1");
            Boolean isValidArrDelayMin = !arrDelayMinutes.toString().equals("");
            Boolean isValidMonth = !month.toString().equals("");
            Boolean canComputeAvgDelay = isValidArrDelayMin && isValidMonth;

            // Collect only flights from 2008 that
            // are not cancelled and
            // ignore records missing arrDelayMinutes or month attribute
            if (isYear2008 && isNotCancelled && canComputeAvgDelay) {
                context.write(new FlightMonthPair(uniqueCarrier, month),
                              new Text(month + "," + arrDelayMinutes));
            }
        }
    }

    public static class AvgDelayReducer extends
            Reducer<FlightMonthPair, Text, FlightMonthPair, Text> {

        // key   : AIR-uniqeCarrier
        // value : (month, total average delay (min))
        private HashMap<String, String> taskLevelHMap = new HashMap();
        private CSVParser csvParser = new CSVParser(',', '"');

        protected void reduce(FlightMonthPair key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {

            // key - month
            // value - (total delay time (min), total flights)
            HashMap<Integer, ArrayList<Integer>> inReducerHMap = new HashMap<>();

            for (Text v : values) {
                String[] line = csvParser.parseLine(v.toString());
                int month = Integer.parseInt(line[0]);
                int arrDelayMinutes = (int) Double.parseDouble(line[1]);

                if (inReducerHMap.containsKey(month)) {// Existing key
                    int updatedarrDelayMinutes = inReducerHMap.get(month).get(0) + arrDelayMinutes;
                    int updatedFlightTotal = inReducerHMap.get(month).get(1) + 1;

                    ArrayList<Integer> updatedValue = new ArrayList<>();

                    updatedValue.add(updatedarrDelayMinutes);
                    updatedValue.add(updatedFlightTotal);

                    inReducerHMap.put(month, updatedValue);
                } else {// New key
                    ArrayList<Integer> totalDelayAndFlight = new ArrayList<Integer>();
                    totalDelayAndFlight.add(arrDelayMinutes);
                    totalDelayAndFlight.add(1);
                    inReducerHMap.put(month, totalDelayAndFlight);
                }
            }

            String airline = "AIR-" + key.getUniqueCarrier();
            String avgDelayPerMonth = getAvgDelaysPerMonth(inReducerHMap);

            taskLevelHMap.put(airline, avgDelayPerMonth);
        }

        public static String getAvgDelaysPerMonth (HashMap inReducerHMap) {
            StringBuilder strBuilder = new StringBuilder();

            Iterator<HashMap.Entry<Integer, ArrayList<Integer>>> hMapItr = inReducerHMap.entrySet().iterator();

            while(hMapItr.hasNext()) {
                HashMap.Entry<Integer, ArrayList<Integer>> entry = hMapItr.next();
                int totalDelay = entry.getValue().get(0);
                int totalFlights = entry.getValue().get(1);
                int avg = (int)Math.ceil(totalDelay / totalFlights);
                strBuilder.append("(" + entry.getKey() + ", " + Integer.toString(avg) + "),");
            }

            return strBuilder.deleteCharAt(strBuilder.length() - 1).toString();// Remove trailing comma
        }

        protected void cleanup (Reducer.Context context) throws IOException, InterruptedException {
            Iterator<HashMap.Entry<String, String>> hMapItr = taskLevelHMap.entrySet().iterator();

            while(hMapItr.hasNext()) {
                HashMap.Entry<String, String> entry = hMapItr.next();
                context.write(entry.getKey(), entry.getValue());
            }
        }
    }

    public static class FlightPartitioner
            extends Partitioner<FlightMonthPair, Text> {

        public int getPartition(FlightMonthPair key, Text value, int numPartitions) {
            return Math.abs(key.hashCode() * 127) % numPartitions;// Partition by uniqueCarrier
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(FlightMonthPair.class, true);
        }

        public int compare(WritableComparable w1, WritableComparable w2) {
            FlightMonthPair fmp1 = (FlightMonthPair) w1;
            FlightMonthPair fmp2 = (FlightMonthPair) w2;
            return fmp1.compareTo(fmp2);// Sort by increasing order of month
        }
    }

    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(FlightMonthPair.class, true);
        }

        public int compare(WritableComparable w1, WritableComparable w2) {
            FlightMonthPair fmp1 = (FlightMonthPair) w1;
            FlightMonthPair fmp2 = (FlightMonthPair) w2;
            return fmp1.compare(fmp2);// Group by uniqueCarrier
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: secondary <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "secondary");

        job.setJarByClass(secondary.class);
        job.setMapperClass(secondary.AvgDelayMapper.class);
        job.setPartitionerClass(secondary.FlightPartitioner.class);
        job.setReducerClass(secondary.AvgDelayReducer.class);

        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);

        job.setOutputKeyClass(FlightMonthPair.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}