package com.sunbeam.dbda;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MovieRatingWritable implements Writable {
	private int movieID;
	private double rating;
	
	public MovieRatingWritable() {
		this(0,0);
	}

	public MovieRatingWritable(int movieID, double rating) {
		this.movieID = movieID;
		this.rating = rating;
	}

	public int getMovieID() {
		return movieID;
	}

	public void setMovieID(int movieID) {
		this.movieID = movieID;
	}

	public double getRating() {
		return rating;
	}

	public void setRating(double rating) {
		this.rating = rating;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(movieID);
		out.writeDouble(rating);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.movieID=in.readInt();
		this.rating=in.readDouble();
		
	}

	@Override
	public String toString() {
		return "movieID=" + movieID + ", rating=" + rating ;
	}
	
	
		

	
}
