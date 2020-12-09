package com.sunbeam.dbda;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
Mapper:
	Input: key=year(string), value=average temperature(string)
	Output: null, Line(string)	--> year	tab		average temperature
*/

public class MovieCorrMapper2 extends Mapper<Text, Text, Text, Rating12Writable> {
	private Text MoviePairs = new Text();
	private Rating12Writable RatingPairs = new Rating12Writable();
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Rating12Writable>.Context context)
			throws IOException, InterruptedException {
		String line =value.toString();
		
		try {
			String[] parts=line.split(",");
			String m1m2;
			
			for(int i=0;i<parts.length;i=i+2)
			{
				for(int j=i+2;j<parts.length;j=j+2)
				{
					int movie1=Integer.parseInt(parts[i]);
					int movie2=Integer.parseInt(parts[j]);
					
					if(movie1<movie2)
					{
						m1m2=parts[i]+","+parts[j];
						MoviePairs.set(m1m2);
						RatingPairs.setRating1(Double.parseDouble(parts[i+1]));
						RatingPairs.setRating2(Double.parseDouble(parts[j+1]));
						context.write(MoviePairs, RatingPairs);
					}
					else {
						m1m2=parts[j]+","+parts[i];
						MoviePairs.set(m1m2);
						RatingPairs.setRating1(Double.parseDouble(parts[j+1]));
						RatingPairs.setRating2(Double.parseDouble(parts[i+1]));
						context.write(MoviePairs, RatingPairs);
					}
					
					
				}
			}
		} catch (Exception e) {
			System.out.println(line);
		}
		
		
	}
}
