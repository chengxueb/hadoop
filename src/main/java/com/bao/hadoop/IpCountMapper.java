package com.bao.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class IpCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private Logger logger = Logger.getLogger(IpCountMapper.class);
	
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws IOException, InterruptedException {
		
		logger.info("当前行: " + value.toString());
		String[] words = value.toString().split(" ");
		
		if (words.length > 0)
		{
			context.write(new Text(words[0]), new IntWritable(1));
		}
	};
}
