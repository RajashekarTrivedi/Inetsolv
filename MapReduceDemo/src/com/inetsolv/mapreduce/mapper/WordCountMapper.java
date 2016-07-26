package com.inetsolv.mapreduce.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	protected void setup(
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {
	};

	@Override
	protected void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException
			{
		final String inputValue=value.toString();
		final IntWritable ONE=new IntWritable(1);
		if(StringUtils.isEmpty(inputValue))
		{
			final String[] Words=StringUtils.splitPreserveAllTokens(inputValue, " ");
			for(String Word:Words)
			{
				context.write(new Text(Word),ONE);
			}
			
		}
	};
}
