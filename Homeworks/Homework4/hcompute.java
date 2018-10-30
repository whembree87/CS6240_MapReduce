package Homework4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class hcompute {
    private static String TABLE_NAME = "Flights";
    private static String COLUMN_FAMILY = "FlightAttributes";
    private static String COLUMN_QUALIFIER_ATTR = "Attributes";
    private static String COLUMN_QUALIFIER_YEAR = "Year";
    private static String FLIGHT_YEAR = "2008";

    public static class hCompMapper extends
            TableMapper<Object, Text> {

        public void map(ImmutableBytesWritable row, Result value, Context context)
                throws IOException, InterruptedException {
            String[] rowKey = new String(value.getRow()).split("_");
            String[] flightAttr = new String(value.getValue(COLUMN_FAMILY.getBytes(),
                                                            COLUMN_QUALIFIER_ATTR.getBytes())).split("~~");

            Text uniqueCarrier = new Text(rowKey[1]);
            Text month = new Text(flightAttr[2]);// Could be obtained from rowKey as well
            Text arrDelayMinutes = new Text(flightAttr[37]);

            Boolean isNotCancelled = !flightAttr[41].equals("1");
            Boolean isValidArrDelayMin = !arrDelayMinutes.toString().equals("");
            Boolean isValidMonth = !month.toString().equals("");
            Boolean canComputeAvgDelay = isValidArrDelayMin && isValidMonth;

            // Collect only non-cancelled flights and
            // ignore records missing arrDelayMinutes attribute
            if (isNotCancelled && canComputeAvgDelay) {
                context.write(new FlightMonthPair(uniqueCarrier, month),
                              new Text(month + "," + arrDelayMinutes));
            }
        }
    }

    public static class hCompReducer extends
            Reducer<FlightMonthPair, Text, FlightMonthPair, Text> {

        // key   : AIR-uniqeCarrier
        // value : (month, total average delay (min))
        private HashMap<String, String> taskLevelHMap = new HashMap();
        private CSVParser csvParser = new CSVParser(',', '"');

        protected void reduce(FlightMonthPair key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {

            // key   : month
            // value : (total delay time (min), total flights)
            HashMap<Integer, ArrayList<Integer>> inReducerHmap = new HashMap<>();

            for (Text v : values) {
                String[] line = csvParser.parseLine(v.toString());
                int month = Integer.parseInt(line[0]);
                int arrDelayMinutes = (int) Double.parseDouble(line[1]);

                if (inReducerHmap.containsKey(month)) {// Existing key
                    int updatedarrDelayMinutes = inReducerHmap.get(month).get(0) + arrDelayMinutes;
                    int updatedFlightTotal = inReducerHmap.get(month).get(1) + 1;

                    ArrayList<Integer> updatedValue = new ArrayList<>();

                    updatedValue.add(updatedarrDelayMinutes);
                    updatedValue.add(updatedFlightTotal);

                    inReducerHmap.put(month, updatedValue);
                } else {// New key
                    ArrayList<Integer> totalDelayAndFlight = new ArrayList<Integer>();
                    totalDelayAndFlight.add(arrDelayMinutes);
                    totalDelayAndFlight.add(1);
                    inReducerHmap.put(month, totalDelayAndFlight);
                }
            }

            String airline = "AIR-" + key.getUniqueCarrier();
            String avgDelayPerMonth = getAvgDelaysPerMonth(inReducerHmap);

            taskLevelHMap.put(airline, avgDelayPerMonth);
        }

        public static String getAvgDelaysPerMonth (HashMap inReducerHmap) {
            StringBuilder strBuilder = new StringBuilder();

            Iterator<HashMap.Entry<Integer, ArrayList<Integer>>> hMapItr = inReducerHmap.entrySet().iterator();

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

    public static class FirstPartitioner
            extends Partitioner<FlightMonthPair, NullWritable> {

        public int getPartition(FlightMonthPair key, NullWritable value, int numPartitions) {
            return Math.abs(key.hashCode() * 127) % numPartitions;// Partition by hash of uniqueCarrier
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(FlightMonthPair.class, true);
        }

        public int compare(WritableComparable w1, WritableComparable w2) {
            FlightMonthPair fmp1 = (FlightMonthPair) w1;
            FlightMonthPair fmp2 = (FlightMonthPair) w2;
            return fmp1.compareTo(fmp2);// Sort by month
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

    public static void main (String[]args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: hcompute <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "hcompute");
        job.setJarByClass(hcompute.class);

        job.setMapperClass(hCompMapper.class);
        job.setReducerClass(hCompReducer.class);

        job.setOutputKeyClass(FlightMonthPair.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(FirstPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);

        filterTable(job, otherArgs);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void filterTable (Job job, String[] otherArgs) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        SingleColumnValueFilter yearFilter = new SingleColumnValueFilter(COLUMN_FAMILY.getBytes(),
                                                                         COLUMN_QUALIFIER_YEAR.getBytes(),
                                                                         CompareFilter.CompareOp.EQUAL,
                                                                         FLIGHT_YEAR.getBytes());
        filterList.addFilter(yearFilter);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.setFilter(filterList);

        TableMapReduceUtil.initTableMapperJob(TABLE_NAME,
                                              scan,
                                              hCompMapper.class,
                                              FlightMonthPair.class,
                                              Text.class,
                                              job);
    }
}