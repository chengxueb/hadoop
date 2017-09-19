package com.bao.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Read {
	
	public static void main(String[] args) throws IOException,
			URISyntaxException {

		if (args.length == 0) {
			System.out.println("参数错误");
		}

		String uri = args[0];
		Configuration configuration = new Configuration();

		FileSystem fs = FileSystem.get(new URI(uri), configuration);
		FSDataInputStream in = fs.open(new Path(uri));
		
		IOUtils.copyBytes(in, System.out, 1024, false);
		IOUtils.closeStream(in);
	}
}