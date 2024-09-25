package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Comparator;

public class TaskTUAHReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    private PriorityQueue<TaxiErrorRate> pq = new PriorityQueue<>();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
            throws IOException, InterruptedException {

        //average the rate
        float avgRate = 0;
        int count = 0;
        for (FloatWritable value : values) {
            avgRate += value.get();
            count++;
        }
        avgRate /= count;

        //add to priority queue
        pq.add(new TaxiErrorRate(key, new FloatWritable(avgRate)));

        //if the queue is too big, remove the smallest element
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