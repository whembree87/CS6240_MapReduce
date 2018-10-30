package Homework4;

import com.opencsv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class hpopulate_version2 {
    private static String TABLE_NAME = "Flights";
    private static String COLUMN_FAMILY = "FlightAttributes";
    private static String COLUMN_QUALIFIER_ATTR = "Attributes";
    private static String COLUMN_QUALIFIER_YEAR = "Year";

    public static class hPopMapper extends
            Mapper<Object, Text, ImmutableBytesWritable, Writable> {
        private CSVParser csvParser = null;

        protected void setup(Context context) throws IOException {
            this.csvParser = new CSVParser(',', '"');
        }

        public void map(Object offset, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = this.csvParser.parseLine(value.toString());

            StringBuilder rowKey = new StringBuilder();
            // Unique row-key : FlightDate_UniqueCarrier_Origin_FlightNum
            rowKey.append(line[5] + "_" + line[6] + "_" + line[11] + "_" + line[10]);

            StringBuilder flightAttr = new StringBuilder();
            for (String str : line) {
                flightAttr.append(str + "~~");// ~~ as a separator
            }

            Put record = new Put(Bytes.toBytes(rowKey.toString()));
            // Add year
            record.add(COLUMN_FAMILY.getBytes(),
                    COLUMN_QUALIFIER_YEAR.getBytes(),
                    line[0].getBytes()
            );
            // Add all flight attributes
            record.add(COLUMN_FAMILY.getBytes(),
                    COLUMN_QUALIFIER_ATTR.getBytes(),
                    flightAttr.substring(0, flightAttr.length() - 1).getBytes());// Remove trailing ~~

            try (Connection connection = ConnectionFactory.createConnection()) {
                Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
                table.put(record);
                table.close();
            }
        }
    }

    public static void main (String[]args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: hpopulate <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "hpopulate");
        job.setJarByClass(hpopulate.class);

        job.setMapperClass(hPopMapper.class);

        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // HBase Setup
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {

            HTableDescriptor table =
                    new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(COLUMN_FAMILY));

            if (!admin.tableExists(table.getTableName())) {
                System.out.print("Creating table. ");
                admin.createTable(table);
                System.out.println(" Done.");
            }
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}