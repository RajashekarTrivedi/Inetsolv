package com.inetsolv.mapreduce.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.inetsolv.mapreduce.mapper.CPMapper;
import com.inetsolv.mapreduce.partitioner.MyPartitioner;
import com.inetsolv.mapreduce.reducer.CPReducer;

public class CPJob extends Configured implements Tool {

	
	public static void main(String[] args) 
	{
		if(args.length<2)
		{
			System.out.println("Java Usage "+CPJob.class.getName()+" /hdfs/path/to/input /hdfs/path/to/output/dir");
			return;
		}
		Configuration conf=new Configuration(Boolean.TRUE);
		conf.set("fs.defaultFS", "hdfs://localhost.localdomain:8020");
		
		try {
			int i=ToolRunner.run(conf, new CPJob(), args);
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
		Job cpJob=Job.getInstance(conf, CPJob.class.getName());
		
		//set Classpath
		cpJob.setJarByClass(CPJob.class);
		
		//setting input
		final String inputPath=args[0];
		final Path hdfsInputPath=new Path(inputPath);
		
		TextInputFormat.addInputPath(cpJob, hdfsInputPath);
		cpJob.setInputFormatClass(TextInputFormat.class);
		
		final String outputPath=args[1];
		final Path hdfsOutputPath=new Path(outputPath);
		
		TextOutputFormat.setOutputPath(cpJob, hdfsOutputPath);
		cpJob.setOutputFormatClass(TextOutputFormat.class);
		
		cpJob.setMapperClass(CPMapper.class);
		cpJob.setMapOutputKeyClass(Text.class);
		cpJob.setMapOutputValueClass(IntWritable.class);
		
		//setting combiner
		cpJob.setCombinerClass(CPReducer.class);
		cpJob.setNumReduceTasks(3);
		cpJob.setPartitionerClass(MyPartitioner.class);
		
		cpJob.setReducerClass(CPReducer.class);
		cpJob.setOutputKeyClass(Text.class);
		cpJob.setOutputValueClass(IntWritable.class);
		
		cpJob.waitForCompletion(Boolean.TRUE);
		
		
		return 0;
	}
	

}
