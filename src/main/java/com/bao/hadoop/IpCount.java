package com.bao.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IpCount {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Ip统计");

		job.setJarByClass(IpCount.class);
		job.setMapperClass(IpCountMapper.class);
		job.setReducerClass(IpCountReducer.class);
		job.setCombinerClass(IpCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setSortComparatorClass(IntWritableSort.class);

		FileInputFormat.addInputPath(job, new Path(
				"/input/cms.btime.cn-access.log"));
		FileOutputFormat.setOutputPath(job, new Path("/output"));

		job.waitForCompletion(true);
	}
}
