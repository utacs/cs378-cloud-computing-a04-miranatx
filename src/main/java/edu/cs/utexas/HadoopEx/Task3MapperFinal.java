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
public class Task3MapperFinal extends Mapper<Text, FloatArrayWritable, Text, FloatWritable> {

	private PriorityQueue<TaxiEarnings> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();
	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, FloatArrayWritable value, Context context)
			throws IOException, InterruptedException {

		Writable[] writableArray = value.get();
		FloatWritable total_amount = ((FloatWritable) writableArray[0]);
		FloatWritable seconds = ((FloatWritable) writableArray[1]);

		float earnings = (float) total_amount.get() / (seconds.get() / 60f);
		earnings = Math.round(earnings * 100f) / 100f;
		pq.offer(new TaxiEarnings(new Text(key.toString()), new FloatWritable(earnings)));

		if (pq.size() > 10) {
			// System.out.println(pq);
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		// System.out.println(pq);
		while (pq.size() > 0) {
			TaxiEarnings taxi = pq.poll();
			// System.out.println(taxi.toString());
			context.write(taxi.getTaxi(), taxi.getEarningsRate());
		}
	}

}