package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.FloatWritable;


public class Task3Reducer extends Reducer<Text, FloatArrayWritable, Text, FloatArrayWritable> {

  public void reduce(Text license, Iterable<FloatArrayWritable> values, Context context)
      throws IOException, InterruptedException {

    int totalAmount = 0;
    int seconds = 0;

    // Iterate through the values to sum error counts and total rides
    for (ArrayWritable value : values) {
      Writable[] writableArray = value.get();
      totalAmount += ((FloatWritable) writableArray[0]).get();
      seconds += ((FloatWritable) writableArray[1]).get();
    }

    // System.out.println("License: " + license.toString() + " Total amount: " + totalAmount + " Seconds: " + seconds);

    // Output the final result for each license
    FloatWritable[] result = new FloatWritable[] { new FloatWritable(totalAmount), new FloatWritable(seconds) };
    context.write(license, new FloatArrayWritable(result));
  }
}
