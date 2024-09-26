package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Task3Mapper extends Mapper<Object, Text, Text, FloatArrayWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");
        boolean valid = true;
        float totalAmount = 0f;
        float seconds = 0f;

        if (fields.length == 17) {
            try {
                float fare = Float.parseFloat(fields[11]);
                float surcharge = Float.parseFloat(fields[12]);
                float tax = Float.parseFloat(fields[13]);
                float tip = Float.parseFloat(fields[14]);
                float tolls = Float.parseFloat(fields[15]);
                totalAmount = Float.parseFloat(fields[16]);

                float compare = surcharge + tax + tip + tolls + fare;

                if(totalAmount >= 500){
                    valid = false;
                }

                if (Math.abs(totalAmount - compare) >= 0.001f) {
                    valid = false;
                }

                if(totalAmount == 0){
                    valid = false;
                }

                seconds = Float.parseFloat(fields[4]);
                if (seconds == 0) {
                    valid = false;
                }

            } catch (NumberFormatException e) {
                valid = false;
            }

            if (valid == true) {
                String license = fields[1].trim();
    
                context.write(new Text(license),
                        new FloatArrayWritable(new FloatWritable[] {new FloatWritable(totalAmount),new FloatWritable(seconds)}));
            }
        }

        
    }

}
