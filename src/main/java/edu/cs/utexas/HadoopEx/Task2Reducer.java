package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Reducer extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {

  public void reduce(Text medallion, Iterable<IntArrayWritable> values, Context context)
      throws IOException, InterruptedException {

    int errorcount = 0;
    int total_rides = 0;

    // Iterate through the values to sum error counts and total rides
    for (ArrayWritable value : values) {
      Writable[] writableArray = value.get();
      errorcount += ((IntWritable) writableArray[0]).get();
      total_rides += ((IntWritable) writableArray[1]).get();
    }

    //System.out.println("Medallion: " + medallion.toString() + " Error count: " + errorcount + " Total rides: " + total_rides);

    // Output the final result for each medallion
    IntWritable[] result = new IntWritable[] { new IntWritable(errorcount), new IntWritable(total_rides) };
    context.write(medallion, new IntArrayWritable(result));
  }
}
