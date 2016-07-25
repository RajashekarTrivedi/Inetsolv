package com.inetsolv.mapreduce.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomerMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	protected void map(LongWritable key,Text value,Context context) throws java.io.IOException, InterruptedException 
	{
		//key -- 0
		//value --4000001,Kristina,Chung,55,Pilot
		final String input=value.toString();
		if(!StringUtils.isEmpty(input))
		{
			
			final String[] tokens=StringUtils.splitPreserveAllTokens(input, ",");
			
			context.write(new LongWritable(Long.parseLong(tokens[0])), new Text("CUSTS"+"\t"+tokens[1]));
			
			
		}
	};

}
