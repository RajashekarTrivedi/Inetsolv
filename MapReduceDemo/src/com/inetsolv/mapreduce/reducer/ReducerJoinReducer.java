package com.inetsolv.mapreduce.reducer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJoinReducer extends Reducer<LongWritable, Text, Text, Text> {
	protected void reduce(LongWritable key,java.lang.Iterable<Text> values,Context context) 
			throws java.io.IOException, InterruptedException 
	{
		//key - customer id
		//values -- customer name, txn amount
		
		
		//4000001,Kristina,Chung,55,Pilot
		//00047933,06-16-2011,4000001,032.52,Racquet Sports,Tennis,Los Angeles,California,credit
//		00044658,10-11-2011,4000001,141.61,Water Sports,Windsurfing,Orange,California,credit
		
		
//		4000002,Paige,Chen,74,Teacher
//		00043872,01-23-2011,4000002,091.78,Gymnastics,Vaulting Horses,Montgomery,Alabama,credit
		
		//{4000001, TXNS	032.52,TXNS	141.61,CUSTS	Kristina}
		//{4000002,CUSTS	Paige,TXNS	091.78}
		String name=null;
		float amount=0;
		int iCount=0;
		for (Text text : values) {
			final String[] tokens=StringUtils.splitPreserveAllTokens(text.toString(), "\t");
			if(StringUtils.equalsIgnoreCase(tokens[0], "CUSTS")){
				name=tokens[1];
			}else if(StringUtils.equalsIgnoreCase(tokens[0], "TXNS"))
			{
				amount=amount+Float.parseFloat(tokens[1]);
				iCount++;
			}
		}
		
		if(!StringUtils.isEmpty(name))
		{
		context.write(new Text(name), new Text(amount+"\t"+iCount));
		}
	}
}
