package com.inetsolv.mapreduce.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.inetsolv.constants.MRConstants;
import com.inetsolv.mapreduce.mapper.GrepMapper;
import com.inetsolv.mapreduce.reducer.GrepReducer;

public class GrepJob extends Configured implements Tool {

	
	public static void main(String[] args) 
	{
		if(args.length<3)
		{
			System.out.println("Java Usage "+GrepJob.class.getName()+" /hdfs/path/to/input /hdfs/path/to/output/dir SEARCH_STR");
			return;
		}
		Configuration conf=new Configuration(Boolean.TRUE);
		conf.set("fs.defaultFS", "hdfs://localhost.localdomain:8020");
		
		try {
			int i=ToolRunner.run(conf, new GrepJob(), args);
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
		
		
		final String SEARCH_STR=args[2];
		conf.set(MRConstants.GREP_SEARCH_STR.getValue(), SEARCH_STR);
		//Creating job instance to get the job information after completing job
		Job grepJob=Job.getInstance(conf, GrepJob.class.getName());
		
		//set Classpath
		grepJob.setJarByClass(GrepJob.class);
		
		//setting input
		final String inputPath=args[0];
		final Path hdfsInputPath=new Path(inputPath);
		
		TextInputFormat.addInputPath(grepJob, hdfsInputPath);
		grepJob.setInputFormatClass(TextInputFormat.class);
		
		final String outputPath=args[1];
		final Path hdfsOutputPath=new Path(outputPath);
		
		TextOutputFormat.setOutputPath(grepJob, hdfsOutputPath);
		grepJob.setOutputFormatClass(TextOutputFormat.class);
		
		grepJob.setMapperClass(GrepMapper.class);
		grepJob.setMapOutputKeyClass(Text.class);
		grepJob.setMapOutputValueClass(IntWritable.class);
		
		grepJob.setReducerClass(GrepReducer.class);
		grepJob.setOutputKeyClass(Text.class);
		grepJob.setOutputValueClass(LongWritable.class);
		
		grepJob.waitForCompletion(Boolean.TRUE);
		
		
		return 0;
	}
	

}
