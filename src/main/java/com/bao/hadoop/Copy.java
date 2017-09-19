package com.bao.hadoop;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class Copy {

	public static void main(String[] args) {

		if (args.length != 2) {
			System.out.println("参数不够");
			return;
		}

		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(args[1]), conf);
			OutputStream out = fs.create(new Path(args[1]), new Progressable() {
				public void progress() {
					System.out.println(".");
				}
			});

			InputStream in = new FileInputStream(args[0]);
			IOUtils.copyBytes(in, out, 1024, false);
			in.close();
			out.close();

		} catch (IOException e) {
		}
	}

}
