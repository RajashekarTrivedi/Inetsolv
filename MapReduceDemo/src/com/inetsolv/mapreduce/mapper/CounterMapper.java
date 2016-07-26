package com.inetsolv.mapreduce.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.inetsolv.constants.MRConstants;

public class CounterMapper extends Mapper<LongWritable, Text, Text, Text> {

	
	String fileName = null;

	protected void setup(
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		fileName = fileSplit.getPath().getName();
	};

	@Override
	protected void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {

		// key -- 0
		// value -- 27516 20150101 2.424 -156.61 71.32 -18.3 -21.8 -20.0 -19.9
		// 0.0 0.00 C -19.2 -24.5 -21.9
		// 83.9 73.7 77.9 -99.000 -99.000 -99.000 -99.000 -99.000 -9999.0
		// -9999.0 -9999.0 -9999.0 -9999.0

		final String input = value.toString();
		String max_temp = null;
		String date = null;
		if (!StringUtils.isEmpty(input)) {
			max_temp = input.substring(38, 45).trim();
			date = input.substring(6, 14).trim();
		}
		if (!StringUtils.equalsIgnoreCase("-9999.0", max_temp) && !StringUtils.isEmpty(max_temp)) {
			context.getCounter(MRConstants.GOOD).increment(1);
			context.write(new Text(fileName), new Text(date + "_" + max_temp));
			
		}else{
			context.getCounter(MRConstants.BAD).increment(1);
			
		}

	};
}
