package com.inetsolv.mapreduce.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.inetsolv.constants.MRConstants;
import com.inetsolv.mapreduce.mapper.CounterMapper;
import com.inetsolv.mapreduce.reducer.CounterReducer;

public class CounterJob extends Configured implements Tool {

	
	public static void main(String[] args) 
	{
		if(args.length<2)
		{
			System.out.println("Java Usage "+CounterJob.class.getName()+" /hdfs/path/to/input /hdfs/path/to/output/dir");
			return;
		}
		Configuration conf=new Configuration(Boolean.TRUE);
		conf.set("fs.defaultFS", "hdfs://localhost.localdomain:8020");
		
		try {
			int i=ToolRunner.run(conf, new CounterJob(), args);
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
		Job counterJob=Job.getInstance(conf, CounterJob.class.getName());
		
		//set Classpath
		counterJob.setJarByClass(CounterJob.class);
		
		//setting input
		final String inputPath=args[0];
		final Path hdfsInputPath=new Path(inputPath);
		
		TextInputFormat.addInputPath(counterJob, hdfsInputPath);
		counterJob.setInputFormatClass(TextInputFormat.class);
		
		final String outputPath=args[1];
		final Path hdfsOutputPath=new Path(outputPath);
		
		TextOutputFormat.setOutputPath(counterJob, hdfsOutputPath);
		counterJob.setOutputFormatClass(TextOutputFormat.class);
		
		counterJob.setMapperClass(CounterMapper.class);
		counterJob.setMapOutputKeyClass(Text.class);
		counterJob.setMapOutputValueClass(Text.class);
//		weatherJob.setNumReduceTasks(0);
		counterJob.setReducerClass(CounterReducer.class);
		counterJob.setOutputKeyClass(Text.class);
		counterJob.setOutputValueClass(Text.class);
		
		counterJob.waitForCompletion(Boolean.TRUE);
		
		System.out.println("MapReduce Program is Completed");
		
		Counters counters=counterJob.getCounters();
		Counter goodCounter=counters.findCounter(MRConstants.GOOD);
		System.out.println("Good Records:"+goodCounter.getValue());
		Counter  badCounter=counters.findCounter(MRConstants.BAD);
		System.out.println("Bad Records:"+badCounter.getValue());
		
		return 0;
	}
	

}
