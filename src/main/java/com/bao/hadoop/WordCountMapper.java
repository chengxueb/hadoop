package com.bao.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

		Logger logger = Logger.getLogger(WordCountMapper.class);
		logger.info("Mapper信息, value: " + value.toString());
		logger.info("Mapper信息, writeable" + key.toString());
		
		String line = value.toString();
		String[] words = line.split(" ");

		for (String w : words) {
			context.write(new Text(w), new IntWritable(1));
		}

	};
}
