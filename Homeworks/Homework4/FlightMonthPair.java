package Homework4;

import java.io.*;
import org.apache.hadoop.io.*;

public class FlightMonthPair implements WritableComparable {

    private Text uniqueCarrier;
    private Text month;

    public FlightMonthPair() {
    }

    public FlightMonthPair(Text uniqueCarrier, Text month) {
        set(uniqueCarrier, month);
    }

    public void set(Text uniqueCarrier, Text month) {
        this.uniqueCarrier = uniqueCarrier;
        this.month = month;
    }

    public Text getUniqueCarrier() {
        return uniqueCarrier;
    }

    public Text getMonth() {
        return month;
    }

    public int hashCode() {
        return this.uniqueCarrier.hashCode();
    }

    public void write(DataOutput out) throws IOException {
        this.uniqueCarrier.write(out);
        this.month.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        if (this.uniqueCarrier == null)
            this.uniqueCarrier = new Text();

        if (this.month == null)
            this.month = new Text();

        this.uniqueCarrier.readFields(in);
        this.month.readFields(in);
    }

    public int compareTo(Object object) {
        FlightMonthPair fmp2 = (FlightMonthPair) object;
        int cmp = getUniqueCarrier().compareTo(fmp2.getUniqueCarrier());
        if (cmp != 0) {
            return cmp;
        }
        int m1 = Integer.parseInt(getMonth().toString());
        int m2 = Integer.parseInt(fmp2.getMonth().toString());
        return m1 == m2 ? 0 : (m1 < m2 ? -1 : 1);
    }

    public int compare(Object object){
        FlightMonthPair flight2 = (FlightMonthPair) object;
        return getUniqueCarrier().compareTo(flight2.getUniqueCarrier());
    }
}