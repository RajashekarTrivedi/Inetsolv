package com.inetsolv.mapreduce.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.inetsolv.mapreduce.mapper.WeatherMapper;
import com.inetsolv.mapreduce.reducer.WeatherReducer;

public class WeatherJob extends Configured implements Tool {

	
	public static void main(String[] args) 
	{
		if(args.length<2)
		{
			System.out.println("Java Usage "+WeatherJob.class.getName()+" /hdfs/path/to/input /hdfs/path/to/output/dir");
			return;
		}
		Configuration conf=new Configuration(Boolean.TRUE);
		conf.set("fs.defaultFS", "hdfs://localhost.localdomain:8020");
		
		try {
			int i=ToolRunner.run(conf, new WeatherJob(), args);
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
		Job weatherJob=Job.getInstance(conf, WeatherJob.class.getName());
		
		//set Classpath
		weatherJob.setJarByClass(WeatherJob.class);
		
		//setting input
		final String inputPath=args[0];
		final Path hdfsInputPath=new Path(inputPath);
		
		TextInputFormat.addInputPath(weatherJob, hdfsInputPath);
		weatherJob.setInputFormatClass(TextInputFormat.class);
		
		final String outputPath=args[1];
		final Path hdfsOutputPath=new Path(outputPath);
		
		TextOutputFormat.setOutputPath(weatherJob, hdfsOutputPath);
		weatherJob.setOutputFormatClass(TextOutputFormat.class);
		
		weatherJob.setMapperClass(WeatherMapper.class);
		weatherJob.setMapOutputKeyClass(Text.class);
		weatherJob.setMapOutputValueClass(Text.class);
//		weatherJob.setNumReduceTasks(0);
		weatherJob.setReducerClass(WeatherReducer.class);
		weatherJob.setOutputKeyClass(Text.class);
		weatherJob.setOutputValueClass(Text.class);
		
		weatherJob.waitForCompletion(Boolean.TRUE);
		
		
		return 0;
	}
	

}
