package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.util.PriorityQueue;

public class Task2Reducer extends  Reducer<Text, TupleWritable, Text, TupleWritable> {


   public void reduce(Text medallion,  Iterable<TupleWritable> values, Context context)
           throws IOException, InterruptedException {

       int errorcount = 0;
       int total_rides = 0;

       for (TupleWritable value : values) {
           errorcount += ((IntWritable)value.get(0)).get();
           total_rides += ((IntWritable) value.get(1)).get();
       }

       context.write(new Text(medallion), new TupleWritable(new IntWritable[] { new IntWritable(errorcount), new IntWritable(total_rides)}));
   }
}