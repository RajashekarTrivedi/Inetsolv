package com.inetsolv.hdfs.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsUtil {
	
	
	public static boolean copyLocalFileToHDFS(final String input,final String hdfsOutputDir, final Configuration conf) throws IOException
	{
		
		System.out.println("IN copyLocalFileToHDFS");
		
		
		final Path hdfsPath=new Path(hdfsOutputDir+"/"+input);
		
		
		final InputStream is=new FileInputStream(input);
		
		FileSystem hdfs=FileSystem.get(conf);
		
		FSDataOutputStream out=hdfs.create(hdfsPath);
		
		
		IOUtils.copyBytes(is, out, conf);
		
		
		return Boolean.TRUE;
		
	}

}
