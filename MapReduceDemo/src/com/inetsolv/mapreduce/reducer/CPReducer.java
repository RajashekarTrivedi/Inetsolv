package com.inetsolv.mapreduce.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CPReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	}
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
	{
		//key ---RIVER
		//value --{1,1,1}
		
		int sum=0;
		for (IntWritable intWritable : values) {
			sum=sum+intWritable.get();
		}
		context.write(key, new IntWritable(sum));
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	}

}
