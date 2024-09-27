package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
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

			Job job = new Job(conf, "Task1");
			job.setJarByClass(Driver.class);

			// specify a Mapper
			job.setMapperClass(TaxiMapper.class);

			// specify a Reducer
			job.setReducerClass(TaxiReducer.class);

			// specify output types
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);

			// specify input and output directories
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(args[1] + "/output_task1"));
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setNumReduceTasks(1);

			if (!job.waitForCompletion(true)) {
				System.exit(1);
		}

			// Job 1: First Mapper -> First Reducer
			Job job1 = Job.getInstance(conf, "Task2");
			job1.setJarByClass(Driver.class);

			job1.setMapperClass(Task2Mapper.class);
			job1.setReducerClass(Task2Reducer.class);

			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(IntArrayWritable.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntArrayWritable.class);

			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/output_task2"));

			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(SequenceFileOutputFormat.class);

			//job1.setNumReduceTasks(1);

			if (!job1.waitForCompletion(true)) {
					System.exit(1);
			}

			// Job 2: Second Mapper -> Second Reducer
			Job job2 = Job.getInstance(conf, "TaskTUAH");
			job2.setJarByClass(Driver.class);

			job2.setMapperClass(TaskTUAHMapper.class);
			job2.setReducerClass(TaskTUAHReducer.class);

			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(FloatWritable.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(FloatWritable.class);

			FileInputFormat.addInputPath(job2, new Path(args[1] + "/output_task2"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/output_taskTUAH"));

			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			job2.setNumReduceTasks(1);

			if (!job2.waitForCompletion(true)) {
				System.exit(1);
			}

			// Job 3: First Mapper -> First Reducer
			Job job3 = Job.getInstance(conf, "Task3mid");
			job3.setJarByClass(Driver.class);

			job3.setMapperClass(Task3Mapper.class);
			job3.setReducerClass(Task3Reducer.class);

			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(FloatArrayWritable.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(FloatArrayWritable.class);

			FileInputFormat.addInputPath(job3, new Path(args[0]));
			FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/output_task3_mid"));

			job3.setInputFormatClass(TextInputFormat.class);
			job3.setOutputFormatClass(SequenceFileOutputFormat.class);

			//job3.setNumReduceTasks(1);

			if (!job3.waitForCompletion(true)) {
					System.exit(1);
			}

			// Job 4: Second Mapper -> Second Reducer
			Job job4 = Job.getInstance(conf, "Task3final");
			job4.setJarByClass(Driver.class);

			job4.setMapperClass(Task3MapperFinal.class);
			job4.setReducerClass(Task3ReducerFinal.class);

			job4.setMapOutputKeyClass(Text.class);
			job4.setMapOutputValueClass(FloatWritable.class);
			job4.setOutputKeyClass(Text.class);
			job4.setOutputValueClass(FloatWritable.class);

			FileInputFormat.addInputPath(job4, new Path(args[1] + "/output_task3_mid"));
			FileOutputFormat.setOutputPath(job4, new Path(args[1] + "/output_task3final"));

			job4.setInputFormatClass(SequenceFileInputFormat.class);
			job4.setOutputFormatClass(TextOutputFormat.class);

			job4.setNumReduceTasks(1);

			return job4.waitForCompletion(true) ? 0 : 1;

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}
}
