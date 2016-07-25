package com.inetsolv.mapreduce.mapper;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.inetsolv.constants.MRConstants;

public class GrepMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private String SEARCH_STR=null;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		SEARCH_STR=context.getConfiguration().get(MRConstants.GREP_SEARCH_STR.getValue());
		
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
				if(word.equalsIgnoreCase(SEARCH_STR)){
				context.write(new Text(word), ONE);
				}
				
			}
		}
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
	}
}
