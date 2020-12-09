package com.sunbeam.dbda;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieReducer extends Reducer< IntWritable, MovieRatingWritable, IntWritable, Text> {
	@Override
	protected void reduce(IntWritable key, Iterable<MovieRatingWritable> values,
			Reducer< IntWritable, MovieRatingWritable, IntWritable, Text>.Context context) throws IOException, InterruptedException {
		
		
		String movieratings="";
		
		for (MovieRatingWritable mRatingWr : values) {
			movieratings+=(mRatingWr.getMovieID()+","+mRatingWr.getRating())+",";
		}
		Text mrsWr=new Text();
		mrsWr.set(movieratings);
		context.write(key, mrsWr);
		
	}
}
