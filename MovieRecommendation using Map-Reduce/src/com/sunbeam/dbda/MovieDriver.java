package com.sunbeam.dbda;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


// create map reduce job, initialize/configure it and submit to hadoop cluster
public class MovieDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		// use GenericOptionsParser to parse command line arguments of the job
		GenericOptionsParser parser = new GenericOptionsParser(args);
		// it create new Configuration from generic options provided (e.g. -fs, -jt, -conf, -D, ...)
		Configuration conf = parser.getConfiguration();
		// execute implemented Tool.run() using ToolRunner
		MovieDriver driver = new MovieDriver();
		int ret = ToolRunner.run(conf, driver, args);
			// internally calls setConf() on driver object.
			// calls Tool.run() method with actual command line args (skipping generic args).
			// collect return value of Tool.run().
		System.exit(ret);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// program need two args i.e. input path and output path.
		if(args.length != 3) {
			System.err.println("Input, Aux output and output path must be given on command line arguments.");
			System.exit(1);
		}
		
		// get the configuration from driver object
		Configuration conf = this.getConf();
		// create MR job object and assign name to it.
		Job job1 = Job.getInstance(conf, "MovieR-Job-1");
		// set jar for the MR job (which contains mapper & reducer class)
		job1.setJarByClass(MovieDriver.class);
		// set up mapper class and its output key-value types
		job1.setMapperClass(MovieMapper.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(MovieRatingWritable.class);
		
		// set up reducer class and its output key-value types
		job1.setReducerClass(MovieReducer.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);
		
		// set Text input format
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		// specify input directory/file hdfs path
		FileInputFormat.addInputPaths(job1, args[0]); 
		// specify output directory path
		FileOutputFormat.setOutputPath(job1, new Path(args[1])); 
		// submit the job to the cluser and wait for its completion
		job1.submit();
		boolean sucess = job1.waitForCompletion(true);
		
		if(!sucess)
			return 1;
		
		// create MR job object and assign name to it.
				Job job2 = Job.getInstance(conf, "MaxTemperature");
				// set jar for the MR job (which contains mapper & reducer class)
				job2.setJarByClass(MovieDriver.class);
				// set up mapper class and its output key-value types
				job2.setMapperClass(MovieCorrMapper2.class);
				job2.setMapOutputKeyClass(Text.class);
				job2.setMapOutputValueClass(Rating12Writable.class);
				// set up reducer class and its output key-value types
				job2.setReducerClass(MovieCorrReducer2.class);
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);
				// set key-value input format for the job
				job2.setInputFormatClass(KeyValueTextInputFormat.class);
				job2.setOutputValueClass(TextOutputFormat.class);
				// specify input directory/file hdfs path
				FileInputFormat.addInputPaths(job2, args[1]); 
				// specify output directory path
				FileOutputFormat.setOutputPath(job2, new Path(args[2])); 
				// submit the job to the cluster and wait for its completion
				job2.submit();
				sucess = job2.waitForCompletion(true);
				
				int ret = sucess ? 0 : 1;
				// exit the program
				return ret;
	}
}



