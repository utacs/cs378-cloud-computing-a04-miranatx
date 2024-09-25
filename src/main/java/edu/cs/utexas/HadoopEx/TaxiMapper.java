package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaxiMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private IntWritable errorCount = new IntWritable(1);
    private IntWritable hourOfDay = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");
        // boolean valid = fields.length == 17;

        // try {
        // float fare = Float.parseFloat(fields[11]);
        // float surcharge = Float.parseFloat(fields[12]);
        // float tax = Float.parseFloat(fields[13]);
        // float tip = Float.parseFloat(fields[14]);
        // float tolls = Float.parseFloat(fields[15]);
        // float totalAmount = Float.parseFloat(fields[16]);

        // float compare = surcharge + tax + tip + tolls + fare;

        // if (Math.abs(totalAmount - compare) >= 0.001f) {
        // valid = false;
        // }

        // int seconds = Integer.parseInt(fields[4]);
        // if (seconds == 0) {
        // valid = false;
        // }

        // } catch (NumberFormatException e) {
        // valid = false;
        // }

        if (fields.length == 17) {
            try {
                // Pickup GPS Error Check
                float plong = Float.parseFloat(fields[6].trim()); // pickup longitude
                float plat = Float.parseFloat(fields[7].trim()); // pickup latitude

                if (fields[6].trim().isEmpty() || fields[7].trim().isEmpty() || plong == 0 || plat == 0) {
                    // Extract pickup hour and emit
                    String pickupDateTime = fields[2].trim();
                    String hourString = pickupDateTime.substring(11, 13);
                    int pickupHour = Integer.parseInt(hourString); // extract hour from pickup time
                    if (pickupHour == 0)
                        pickupHour = 24; // adjust to 1-24 scale
                    hourOfDay.set(pickupHour);
                    errorCount.set(1);
                    context.write(hourOfDay, errorCount); // Emit an error for the pickup
                }
            } catch (NumberFormatException e) {
                // Handle parsing error for pickup fields
                String pickupDateTime = fields[2].trim();
                String hourString = pickupDateTime.substring(11, 13);
                int pickupHour = Integer.parseInt(hourString);
                if (pickupHour == 0)
                    pickupHour = 24;
                hourOfDay.set(pickupHour);
                errorCount.set(1);
                context.write(hourOfDay, errorCount); // Emit an error for the pickup
            }

            try {
                // Dropoff GPS Error Check
                float dlong = Float.parseFloat(fields[8].trim()); // dropoff longitude
                float dlat = Float.parseFloat(fields[9].trim()); // dropoff latitude

                if (fields[8].trim().isEmpty() || fields[9].trim().isEmpty() || dlong == 0 || dlat == 0) {
                    // Extract dropoff hour and emit
                    String dropoffDateTime = fields[3].trim();
                    String hourString = dropoffDateTime.substring(11, 13);
                    int dropoffHour = Integer.parseInt(hourString); // extract hour from dropoff time
                    if (dropoffHour == 0)
                        dropoffHour = 24; // adjust to 1-24 scale
                    hourOfDay.set(dropoffHour);
                    errorCount.set(1);

                    context.write(hourOfDay, errorCount); // Emit an error for the dropoff
                }
            } catch (NumberFormatException e) {
                // Handle parsing error for dropoff fields
                String dropoffDateTime = fields[3].trim();
                String hourString = dropoffDateTime.substring(11, 13);
                int dropoffHour = Integer.parseInt(hourString);
                if (dropoffHour == 0)
                    dropoffHour = 24;
                hourOfDay.set(dropoffHour);
                errorCount.set(1);

                context.write(hourOfDay, errorCount); // Emit an error for the dropoff
            }
        }
    }
}
