package com.sunbeam.dbda;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class MovieCorrReducer2 extends Reducer<Text, Rating12Writable, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Rating12Writable> values, Reducer<Text, Rating12Writable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		ArrayList<Double> Xarr=new ArrayList<>();
		ArrayList<Double> Yarr=new ArrayList<>();
		
		int c=0;
		for ( Rating12Writable ratingsWr: values) {
			Xarr.add(ratingsWr.getRating1());
			Yarr.add(ratingsWr.getRating2());
			c+=1;
		}
			
		
		 double sum_X = 0, sum_Y = 0, sum_XY = 0; 
	     double squareSum_X = 0, squareSum_Y = 0; 
	      
	        
	        Iterator<Double> itrX= Xarr.iterator();
	        Iterator<Double> itrY=Yarr.iterator();	
	        double xi;
	        double yi;
	        while(itrX.hasNext() && itrY.hasNext()) {
	        	xi =itrX.next();
	        	yi =itrY.next();
	        	
	        	// sum of elements of array X. 
	            sum_X = sum_X + xi; 
	       
	            // sum of elements of array Y. 
	            sum_Y = sum_Y + yi; 
	       
	            // sum of X[i] * Y[i]. 
	            sum_XY = sum_XY + xi * yi; 
	       
	            // sum of square of array elements. 
	            squareSum_X = squareSum_X + xi * xi; 
	            squareSum_Y = squareSum_Y + yi * yi; 
	        }
	        
	        // use formula for calculating correlation  
	        // coefficient. 
	        double corr = 0;
	        		corr= (double)(c * sum_XY - sum_X * sum_Y)/ 
	                     (double)(Math.sqrt((c * squareSum_X - 
	                     sum_X * sum_X) * (c * squareSum_Y -  
	                     sum_Y * sum_Y))); 
	        
	       Text corrWr=new Text();
	        String corrStr=c+","+corr;
	        corrWr.set(corrStr);
	        context.write(key, corrWr);
	       
		
	}
}
