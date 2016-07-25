package com.inetsolv.hdfs;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.inetsolv.hdfs.util.HdfsUtil;

public class HdfsService extends Configured implements Tool {

	
	public static void main(String[] args) {

		//Logic -1
		
		if(args.length<2)
		{
			System.out.println("Java Usage HdfsService /path/to/local/input /path/to/hdfs/output");
			return;
		}
		
		
		Configuration conf=new Configuration(Boolean.TRUE);
		conf.set("fs.defaultFS", "hdfs://localhost.localdomain:8020");
		
		
		System.out.println("In MAIN METHOD");
		try {
			int i=ToolRunner.run(conf, new HdfsService(), args);
			if(i==0){
				System.out.println("SUCCESS");
			}else{
				System.out.println("FAILURE");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		printConf(conf);

	}

	@Override
	public int run(String[] args) throws Exception 
	{
		
		//Logic -2
		
		Configuration conf=super.getConf();
		System.out.println("IN RUN METHOD");
		
		final String input=args[0];
		final String output=args[1];
		
		final Path hdfsPath=new Path(output+"/"+input);
		
		
		final InputStream is=new FileInputStream(input);
		
		FileSystem hdfs=FileSystem.get(conf);
		
		FSDataOutputStream out=hdfs.create(hdfsPath);
		
		
		IOUtils.copyBytes(is, out, conf);
		
//		HdfsUtil.copyLocalFileToHDFS(input, output, conf);
		
		return 0;
	}
	
	public static void printConf(final Configuration conf)
	{
		for (Map.Entry<String, String> entry : conf) {
			System.out.println(entry.getKey()+"="+entry.getValue());
		}
	}
}
