package com.inetsolv.mapreduce.mapper;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.inetsolv.constants.MRConstants;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
	}
	
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, 
	InterruptedException {
		
		//key -- 0
		//value -- Deer Bear River
		
		final String inputValue=value.toString();
		
		final IntWritable ONE = new IntWritable(1);
		
		if(!StringUtils.isEmpty(inputValue))
		{
			final String[] words = StringUtils.splitPreserveAllTokens(inputValue, 
					MRConstants.EMPTY.getValue());
			for (String word : words) {
				context.write(new Text(word), ONE);
				
			}
		}
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
	}
}
