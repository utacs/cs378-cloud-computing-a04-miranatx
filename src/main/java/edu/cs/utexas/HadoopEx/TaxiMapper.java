package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaxiMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private final static IntWritable errorcount = new IntWritable(1);
    private IntWritable hourOfDay = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");

        if (fields.length == 17) {
            boolean valid = true;

            try {
                float fare = Float.parseFloat(fields[11]);
                float surcharge = Float.parseFloat(fields[12]);
                float tax = Float.parseFloat(fields[13]);
                float tip = Float.parseFloat(fields[14]);
                float tolls = Float.parseFloat(fields[15]);
                float totalAmount = Float.parseFloat(fields[16]);

                float compare = surcharge + tax + tip + tolls + fare;

                if (Math.abs(totalAmount - compare) >= 0.001f) {
                    valid = false; 
                }

                int seconds = Integer.parseInt(fields[4]);
                if (seconds == 0) {
                    valid = false;
                }

            } catch (NumberFormatException e) {
                valid = false;  
            }

            if (valid) {
                try {
                    String pickupDateTime = fields[2].trim();  
                    String hourString = pickupDateTime.substring(11, 13);  
                    int hour = Integer.parseInt(hourString); 
                    hourOfDay.set(hour);

                    float plong = Float.parseFloat(fields[6].trim());  //pickup longitude
                    float plat = Float.parseFloat(fields[7].trim());   //pickup latitude
                    float dlong = Float.parseFloat(fields[8].trim());  //dropoff longitude
                    float dlat = Float.parseFloat(fields[9].trim());   //dropoff latitude

                    if (plong == 0 || plat == 0 || dlong == 0 || dlat == 0) {
                        context.write(hourOfDay, errorcount);  // Emit hour and error count
                    }

                } catch (NumberFormatException e) {
                    context.write(hourOfDay, errorcount);
                }
            }
        }
    }
}
