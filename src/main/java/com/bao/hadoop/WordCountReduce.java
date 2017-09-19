package com.bao.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReduce extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws java.io.IOException, InterruptedException {
		int sum = 0;
		
		for (IntWritable v : values)
		{
			sum += v.get();
		}
		
		context.write(key, new IntWritable(sum));
	};
}
