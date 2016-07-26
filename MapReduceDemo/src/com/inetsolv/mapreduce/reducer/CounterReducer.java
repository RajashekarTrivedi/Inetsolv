package com.inetsolv.mapreduce.reducer;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CounterReducer extends Reducer<Text, Text, Text, Text> 
{
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
		//key ---CRND0103-2015-AK_Barrow_4_ENE.txt
		//value --{20150101_  -18.3,20150102_  -20.9,20150103_  -23.2,20150101_-91.34,
		
		/**
		 * CRND0103-2015-AK_Deadhorse_3_S.txt	20150101_  -18.3
CRND0103-2015-AK_Deadhorse_3_S.txt	20150102_  -20.9
CRND0103-2015-AK_Deadhorse_3_S.txt	20150103_  -23.2
CRND0103-2015-AK_Deadhorse_3_S.txt	20150104_  -24.7
CRND0103-2015-AK_Deadhorse_3_S.txt	20150105_  -11.5
CRND0103-2015-AK_Deadhorse_3_S.txt	20150106_  -11.8

		 */
		
		float tempTemp=Float.MIN_VALUE;
		String maxDate=null;
		for (Text date_maxTemp : values) {
			final String[] tokens=StringUtils.splitPreserveAllTokens(date_maxTemp.toString(), "_");
			//tokens[0] --date
			//tokens[1] --temp
			float currentTemp=Float.parseFloat(tokens[1]);//-18.3
			if(tempTemp<currentTemp){
				tempTemp=currentTemp;
				maxDate=tokens[0];
			}
			
		}
		
		context.write(key, new Text(maxDate+"\t"+tempTemp));
		
	}
	
}
