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

import com.inetsolv.mapreduce.mapper.WordCountMapper;
import com.inetsolv.mapreduce.reducer.WordCountReducer;

public class WordCountJob extends Configured implements Tool {

	
	public static void main(String[] args) 
	{
		if(args.length<2)
		{
			System.out.println("Java Usage "+WordCountJob.class.getName()+" /hdfs/path/to/input /hdfs/path/to/output/dir");
			return;
		}
		Configuration conf=new Configuration(Boolean.TRUE);
		conf.set("fs.defaultFS", "hdfs://localhost.localdomain:8020");
		
		try {
			int i=ToolRunner.run(conf, new WordCountJob(), args);
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
		Job wordCountJob=Job.getInstance(conf, WordCountJob.class.getName());
		
		//set Classpath
		wordCountJob.setJarByClass(WordCountJob.class);
		
		//setting input
		final String inputPath=args[0];
		final Path hdfsInputPath=new Path(inputPath);
		
		TextInputFormat.addInputPath(wordCountJob, hdfsInputPath);
		wordCountJob.setInputFormatClass(TextInputFormat.class);
		
		final String outputPath=args[1];
		final Path hdfsOutputPath=new Path(outputPath);
		
		TextOutputFormat.setOutputPath(wordCountJob, hdfsOutputPath);
		wordCountJob.setOutputFormatClass(TextOutputFormat.class);
		
		wordCountJob.setMapperClass(WordCountMapper.class);
		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(IntWritable.class);
		
		wordCountJob.setReducerClass(WordCountReducer.class);
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(LongWritable.class);
		
		wordCountJob.waitForCompletion(Boolean.TRUE);
		
		
		return 0;
	}
	

}
