package com.inetsolv.mapreduce.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> {


	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) 
	{
		if(key.toString().equalsIgnoreCase("hadoop")){
			return 0;
		}
		if(key.toString().equalsIgnoreCase("data")){
			return 1;
		}else{
			return 2;
		}
		
	}

}
