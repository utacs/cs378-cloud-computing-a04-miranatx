package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;

//CODE ON THAT THANG
public class TaskTUAHMapper extends Mapper<Text, TupleWritable, Text, FloatWritable> {
    
    

	private PriorityQueue<TaxiErrorRate> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();
	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, TupleWritable value, Context context)
			throws IOException, InterruptedException {


    IntWritable errorCount = (IntWritable) value.get(0);
    IntWritable totalRides = (IntWritable) value.get(1);

    if (totalRides.get() != 0){
      float errorRate = (float) errorCount.get() / totalRides.get();
      context.write(key, new FloatWritable(errorRate));
      pq.add(new TaxiErrorRate(key, new FloatWritable(errorRate)));
    }
    
    if (pq.size() > 5) {
      pq.poll();
    }
}

	public void cleanup(Context context) throws IOException, InterruptedException {
		while (pq.size() > 0) {
			TaxiErrorRate taxi = pq.poll();
			context.write(taxi.getTaxi(), taxi.getErrorRate());
		}
	}

}