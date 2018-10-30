package Homework3;

import com.opencsv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PLAIN {

    public enum HadoopCounter {
        TotalDelay,
        TotalFlights
    }

    public static class AverageDelayMapper extends Mapper<Object, Text, Text, Text> {
        private CSVParser csvParser = new CSVParser(',', '"');

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // First, determine if the flight is valid
            String[] line = this.csvParser.parseLine(value.toString());

            boolean isTwoLegFlight = (!Objects.equals(line[11].toLowerCase(), "ord") || !Objects.equals(line[17].toLowerCase(), "jfk"));

            boolean isActiveFlight = ((int) Float.parseFloat(line[41]) == 0 && (int) Float.parseFloat(line[43]) == 0);

            boolean dateWithinRange = dateWithinRange(line);

            boolean isValidFlight = isTwoLegFlight && isActiveFlight && dateWithinRange;

            if (isValidFlight) {
                applyMap(line, context);
            }
        }

        public static void applyMap (String[] line, Context context) throws IOException, InterruptedException {
            String specifics = getFlightSpecifics(line);

            Text originAirport = new Text();
            Text destinationAirport = new Text();
            Text flightSpecifics = new Text();

            originAirport.set(line[11].toLowerCase());
            destinationAirport.set(line[17].toLowerCase());
            flightSpecifics.set(specifics);

            // Chicago ----F1----> Destination X (key) ----F2----> New York
            if (originAirport.toString().equals("ord")) {
                context.write(new Text(line[5] + " --- " + line[17].toLowerCase()), flightSpecifics);// F1
            } else if (destinationAirport.toString().equals("jfk")) {
                context.write(new Text(line[5] + " --- " + line[11].toLowerCase()), flightSpecifics);// F2
            }
        }

        static boolean dateWithinRange (String[] line) {
            int year = Integer.parseInt(line[0]);
            int month = Integer.parseInt(line[2]);
            int day = Integer.parseInt(line[3]);

            // Test period
            // DateTime start = new DateTime(2007, 12, 1, 0, 0, 0, 0);
            // DateTime end = new DateTime(2008, 2, 1, 0, 0, 0, 0);

            DateTime start = new DateTime(2007, 6, 1, 0, 0, 0, 0);
            DateTime end = new DateTime(2008, 6, 1, 0, 0, 0, 0);
            Interval interval = new Interval(start, end);
            DateTime range = new DateTime(year, month, day, 0, 0, 0);

            return interval.contains(range); // Excludes end date
        }

        static String getFlightSpecifics(String[] line) {
            StringBuilder flightInfo = new StringBuilder();

            // line index : array index : attribute
            // 11 : 0 : Origin
            // 17 : 1 : Destination
            // 5  : 2 : FlightDate
            // 24 : 3 : DepTime
            // 35 : 4 : ArrTime
            // 37 : 5 : ArrDelayMinutes
            int[] targetIndexes = new int[]{11, 17, 5, 24, 35, 37};

            for (int i : targetIndexes) {
                if (i == 11 || i == 17) {
                    line[i] = line[i].toLowerCase();
                }

                if (i == 37) {
                    int arrDelayMinutes = (int)Float.parseFloat(line[37]);
                    line[37] = Integer.toString(arrDelayMinutes);
                    flightInfo.append(line[37]);
                    break;
                }

                flightInfo.append(line[i]).append(",");
            }

            return flightInfo.toString();
        }
    }

    public static class AverageDelayReducer extends Reducer<Text,Text,Text,Text> {
        private CSVParser csvParser = null;
        private int totalFlights;
        private float totalDelay;

        protected void setup(Context context) throws IOException, InterruptedException {
            this.csvParser = new CSVParser(',','"');
            this.totalDelay = 0;
            this.totalFlights = 0;
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.getCounter(HadoopCounter.TotalDelay).increment((long)totalDelay);
            context.getCounter(HadoopCounter.TotalFlights).increment(totalFlights);

            long totalDelay = context.getCounter(HadoopCounter.TotalDelay).getValue();
            long totalFlights = context.getCounter(HadoopCounter.TotalFlights).getValue();
            // long avgDelay = totalDelay/totalFlights;

            context.write(new Text(Long.toString(totalFlights)), new Text(Long.toString(totalDelay)));
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Chicago ----F1----> Destination X (key) ----F2----> New York
            List<String> f1Flights = new ArrayList<String>();
            List<String> f2Flights = new ArrayList<String>();

            for (Text v : values) {
                String flight = v.toString();

                if (flight.contains("ord")) {
                    f1Flights.add(flight);
                } else if (flight.contains("jfk")) {
                    f2Flights.add(flight);
                }
            }

            for (String f1 : f1Flights) {
                for (String f2 : f2Flights) {

                    String[] flight1 = this.csvParser.parseLine(f1);
                    String[] flight2 = this.csvParser.parseLine(f2);

                    // Same flight date
                    if (isSameDate(flight1[2], flight2[2])) {
                        // F2 depart time > F1 arrival time
                        if (Integer.parseInt(flight2[3]) > Integer.parseInt(flight1[4])) {
                            this.totalDelay += Integer.parseInt(flight1[5]) + Integer.parseInt(flight2[5]);
                            this.totalFlights++;
                        }
                    }
                }
            }
        }

        static boolean isSameDate (String dt1, String dt2) {
            DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd");
            org.joda.time.LocalDate date1 = dtf.parseLocalDate(dt1);
            org.joda.time.LocalDate date2 = dtf.parseLocalDate(dt2);

            return date1.isEqual(date2);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: PLAIN <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "PLAIN");
        job.setJarByClass(PLAIN.class);
        job.setMapperClass(PLAIN.AverageDelayMapper.class);
        job.setReducerClass(PLAIN.AverageDelayReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


