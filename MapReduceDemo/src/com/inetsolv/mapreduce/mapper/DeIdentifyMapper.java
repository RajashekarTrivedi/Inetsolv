package com.inetsolv.mapreduce.mapper;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeIdentifyMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {
	
	private static final String key = "Bar12345Bar12345";
	final int[] encryptCols={2,3,4,5,8,9};

	@Override
	protected void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		// key -- 0
		// value
		// --11111,aaa1,12/10/1950,1234567890,aaa1@xxx.com,1111111111,M,Diabetes,78

		final String input = value.toString();
		final StringBuffer buffer=new StringBuffer();
		
		if (!StringUtils.isEmpty(input)) {

			final String[] tokens=StringUtils.splitPreserveAllTokens(input, ",");
			
			for (int iColIdx = 0; iColIdx < tokens.length; iColIdx++) {
				if(isColumnEncrpyt(iColIdx)){
					try {
						String ecryptedValue=encrypt(tokens[iColIdx]);
						buffer.append(ecryptedValue);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}else{
					buffer.append(tokens[iColIdx]);
				}
				buffer.append(",");
			}
		}
		context.write(new Text(buffer.toString()), NullWritable.get());

	};
	
	private boolean isColumnEncrpyt(int iColIdx)
	{
		for (int i = 0; i < encryptCols.length; i++) {
			if(encryptCols[i]==iColIdx)
			{
				return Boolean.TRUE;
			}
		}
		return Boolean.FALSE;
	}
	
	
	public String encrypt(final String columnValue) throws Exception
	{
		IvParameterSpec iv = new IvParameterSpec("RandomInitVector".getBytes("UTF-8"));
        SecretKeySpec skeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES");

		
		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
        cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);

        byte[] encrypted = cipher.doFinal(columnValue.getBytes());
//        System.out.println("encrypted string: "+ Base64.encodeBase64String(encrypted));

        return Base64.encodeBase64String(encrypted);
	}
}
