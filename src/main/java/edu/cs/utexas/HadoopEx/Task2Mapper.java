package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Task2Mapper extends Mapper<Object, Text, Text, IntArrayWritable> {

  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

    IntWritable errorcount = new IntWritable(1);
    IntWritable total_rides = new IntWritable(1);

    String[] fields = value.toString().split(",");
    boolean valid = true;

    if (fields.length == 17) {
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
    }

    if (valid == true) {
      String medallion = fields[0].trim();

      try {
        float plong = Float.parseFloat(fields[6]); // pickup longitude
        float plat = Float.parseFloat(fields[7]); // pickup latitude
        float dlong = Float.parseFloat(fields[8]); // dropoff longitude
        float dlat = Float.parseFloat(fields[9]); // dropoff latitude

        if (plong == 0 || plat == 0 || dlong == 0 || dlat == 0) {
          context.write(new Text(medallion),
            new IntArrayWritable(new IntWritable[] { new IntWritable(1), new IntWritable(0) }));
        }
        // System.out.println("Error count: " + errorcount.get());

      } catch (NumberFormatException e) {
        context.write(new Text(medallion),
            new IntArrayWritable(new IntWritable[] { new IntWritable(1), new IntWritable(0) }));
      }

      try {
        total_rides.set(total_rides.get() + 1);

        if (errorcount.get() > 2) {
          System.out.println("Error count: " + errorcount.get());
        }

        context.write(new Text(medallion),
            new IntArrayWritable(new IntWritable[] { new IntWritable(0), new IntWritable(1) }));
      } catch (NumberFormatException e) {
        System.out.println("Error parsing hour");
      }

    }
  }

}
