package com.inetsolv.mapreduce.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.inetsolv.mapreduce.mapper.CustomerMapper;
import com.inetsolv.mapreduce.mapper.TxnMapper;
import com.inetsolv.mapreduce.reducer.ReducerJoinReducer;

public class ReducerJoinJob extends Configured implements Tool {

	
	public static void main(String[] args) 
	{
		if(args.length<3)
		{
			System.out.println("Java Usage "+ReducerJoinJob.class.getName()+" /hdfs/path/to/cust /hdfs/path/to/txns /hdfs/path/to/output/dir");
			return;
		}
		Configuration conf=new Configuration(Boolean.TRUE);
		conf.set("fs.defaultFS", "hdfs://localhost.localdomain:8020");
		
		try {
			int i=ToolRunner.run(conf, new ReducerJoinJob(), args);
			if(i==0){
				System.out.println("SUCCESS");
			}else{
				System.out.println("Failed");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		
		//Configuration set by the ToolRunner.run
		Configuration conf=super.getConf();
		
		//Creating job instance to get the job information after completing job
		Job reducerJoinJob=Job.getInstance(conf, ReducerJoinJob.class.getName());
		
		//set Classpath
		reducerJoinJob.setJarByClass(ReducerJoinJob.class);
		
		//setting customer input
		final String custInput=args[0];
		final Path hdfsCustInputPath=new Path(custInput);
		
		MultipleInputs.addInputPath(reducerJoinJob, hdfsCustInputPath, TextInputFormat.class, CustomerMapper.class);
		
		//setting txn input
		final String txnInput=args[1];
		final Path hdfsTxnInputPath=new Path(txnInput);
		
		MultipleInputs.addInputPath(reducerJoinJob, hdfsTxnInputPath, TextInputFormat.class, TxnMapper.class);
		
		
		//setting output
		final String output=args[2];
		final Path hdfsOutputPath=new Path(output);
		
		reducerJoinJob.setReducerClass(ReducerJoinReducer.class);
		TextOutputFormat.setOutputPath(reducerJoinJob, hdfsOutputPath);
		reducerJoinJob.setOutputFormatClass(TextOutputFormat.class);
		
		reducerJoinJob.setMapOutputKeyClass(LongWritable.class);
		reducerJoinJob.setMapOutputValueClass(Text.class);
		
		
		reducerJoinJob.setOutputKeyClass(Text.class);
		reducerJoinJob.setOutputValueClass(Text.class);
		
		reducerJoinJob.waitForCompletion(Boolean.TRUE);
		
		
		return 0;
	}
	

}
