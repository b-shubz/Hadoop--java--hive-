package com.sunbeam.dbda;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Rating12Writable  implements Writable{
	private double rating1;
	private double rating2;
	
	
	
	public Rating12Writable() {
		this(0,0);
	}

	public Rating12Writable(double rating1, double rating2) {
		this.rating1 = rating1;
		this.rating2 = rating2;
	}

	public double getRating1() {
		return rating1;
	}

	public void setRating1(double rating1) {
		this.rating1 = rating1;
	}

	public double getRating2() {
		return rating2;
	}

	public void setRating2(double rating2) {
		this.rating2 = rating2;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(rating1);
		out.writeDouble(rating2);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.rating1=in.readDouble();
		this.rating2=in.readDouble();
	}

	@Override
	public String toString() {
		return "rating1=" + rating1 + ", rating2=" + rating2;
	}

	
	
	
}
