package com.bao.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

public class WordCount {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		Logger logger = Logger.getLogger(WordCount.class);

		Configuration cfg = new Configuration();
		Job job = Job.getInstance(cfg, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);
		job.setCombinerClass(WordCountReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		String[] realargs = new GenericOptionsParser(cfg, args)
				.getRemainingArgs();
		
		for (String s : realargs)
		{
			logger.info("参数: " + s);
		}

		FileInputFormat.addInputPath(job, new Path(
				realargs[realargs.length - 2]));
		FileOutputFormat.setOutputPath(job, new Path(
				realargs[realargs.length - 1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
