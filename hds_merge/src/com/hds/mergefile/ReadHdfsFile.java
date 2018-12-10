package com.hds.mergefile;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadHdfsFile {
	private static final String path = "hdfs://39.107.79.26:9000/pcbi/original/server/order";
	
	public static void ReadFile(String path) throws IOException {
		System.setProperty("hadoop.home.dir", "F:\\hadoop2.6.0\\hadoop-2.6.0");
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileFileSystem");
		FileSystem fs = null;
		FileStatus[] status = null;
		try {
			fs = FileSystem.get(URI.create(path), conf);
			status = fs.listStatus(new Path(path));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for (FileStatus file : status) {
			if (!file.getPath().getName().endsWith(".txt")) {
				continue;
			}
			FSDataInputStream fsInput = fs.open(file.getPath());
			byte[] buffer = new byte[1024];
			int readLine = fsInput.read(buffer);
			while (readLine != -1) {
				readLine = fsInput.read(buffer);
			}
			fsInput.close();
		}
	}
	
	public static void main(String[] args) throws IOException {
		ReadFile(path);
	}
}
