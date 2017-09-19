package com.bao.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class IpCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	
	private Logger logger = Logger.getLogger(IpCountReducer.class);
	
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
		logger.info("Reduce 当前key: " + key);
		int sum = 0;
		
		for (IntWritable v : values)
		{
			sum += v.get();
		}
		
		context.write(key, new IntWritable(sum));
	};
}
