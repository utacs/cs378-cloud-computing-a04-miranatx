package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String args[]) {
		try {
			Configuration conf = new Configuration();

			// Job job = new Job(conf, "Task1");
			// job.setJarByClass(Driver.class);

			// // specify a Mapper
			// job.setMapperClass(TaxiMapper.class);

			// // specify a Reducer
			// job.setReducerClass(TaxiReducer.class);

			// // specify output types
			// job.setOutputKeyClass(IntWritable.class);
			// job.setOutputValueClass(IntWritable.class);

			// // specify input and output directories
			// FileInputFormat.addInputPath(job, new Path(args[0]));
			// job.setInputFormatClass(TextInputFormat.class);

			// FileOutputFormat.setOutputPath(job, new Path(args[1]));
			// job.setOutputFormatClass(TextOutputFormat.class);

			// Job 1: First Mapper -> First Reducer
			Job job1 = Job.getInstance(conf, "Job1");
			job1.setJarByClass(Driver.class);

			job1.setMapperClass(Task2Mapper.class);
			job1.setReducerClass(Task2Reducer.class);

			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(TupleWritable.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(TupleWritable.class);

			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path("intermediate_output"));

			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(SequenceFileOutputFormat.class);

			if (!job1.waitForCompletion(true)) {
					System.exit(1);
			}

			// Job 2: Second Mapper -> Second Reducer
			Job job2 = Job.getInstance(conf, "Job2");
			job2.setJarByClass(Driver.class);

			job2.setMapperClass(TaskTUAHMapper.class);
			job2.setReducerClass(TaskTUAHReducer.class);

			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(FloatWritable.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(FloatWritable.class);

			FileInputFormat.addInputPath(job2, new Path("intermediate_output"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]));

			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			return job2.waitForCompletion(true) ? 0 : 1;

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}
}
