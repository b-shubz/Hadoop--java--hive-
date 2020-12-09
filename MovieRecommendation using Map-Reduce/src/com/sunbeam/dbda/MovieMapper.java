package com.sunbeam.dbda;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<LongWritable, Text, IntWritable, MovieRatingWritable> {
	
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, MovieRatingWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		IntWritable userIDWr=new IntWritable();
		MovieRatingWritable movieRating=new MovieRatingWritable();
		
		try {
			
			String []reviewLine=line.split(",");
			int userID=Integer.parseInt(reviewLine[0]);
			userIDWr.set(userID);
			movieRating.setMovieID(Integer.parseInt(reviewLine[1]));
			movieRating.setRating(Integer.parseInt(reviewLine[2]));
			
			context.write(userIDWr, movieRating);
			
		}
		catch(Exception e) {
			System.out.println(line);
		}
			
		
	}
}


