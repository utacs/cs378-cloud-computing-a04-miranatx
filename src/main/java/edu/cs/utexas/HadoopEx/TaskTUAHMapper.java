package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.HashSet;

import org.apache.log4j.Logger;

//CODE ON THAT THANG
public class TaskTUAHMapper extends Mapper<Text, IntArrayWritable, Text, FloatWritable> {

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
	public void map(Text key, IntArrayWritable value, Context context)
			throws IOException, InterruptedException {

		Writable[] writableArray = value.get();
		IntWritable errorCount = ((IntWritable) writableArray[0]);
		IntWritable totalRides = ((IntWritable) writableArray[1]);

		if (totalRides.get() != 0) {
			float errorRate = (float) errorCount.get() / totalRides.get();
			if(errorRate == 2.0f){
				System.out.println("Error count: " + errorCount.get());
				System.out.println("Total rides: " + totalRides.get());
			}
			pq.offer(new TaxiErrorRate(new Text(key.toString()), new FloatWritable(errorRate)));
		} else {
			pq.offer(new TaxiErrorRate(new Text(key.toString()), new FloatWritable(0f)));
		}

		if (pq.size() > 5) {
			// System.out.println(pq);
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		// System.out.println(pq);
		while (pq.size() > 0) {
			TaxiErrorRate taxi = pq.poll();
			// System.out.println(taxi.toString());
			context.write(taxi.getTaxi(), taxi.getErrorRate());
		}
	}

}