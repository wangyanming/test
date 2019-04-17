package com.hds.cn.hdfs;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.hds.cn.util.DateUtil;

public class FileMerge {

	private static final Log logger = LogFactory.getLog(FileMerge.class);

	private static final String path = "hdfs://192.168.0.29:8022";

	public static boolean fileMerge(FileSystem hdfs, String srcDir, String fileName, Configuration conf)
			throws Exception {
		if (!hdfs.exists(new Path(srcDir))) {
			logger.info("路径'" + srcDir + "'" + "不存在!!!");

			logger.info("开始创建路径：'" + srcDir + "'");

			hdfs.mkdirs(new Path(srcDir));

			logger.info("路径：'" + srcDir + "'" + "创建成功");
			return false;
		}

		if (!hdfs.isDirectory(new Path(srcDir))) {
			logger.error(srcDir + "为文件，不是目录");
			return false;
		}
		
		FileStatus[] fileStatus = hdfs.listStatus(new Path(srcDir));
		
		OutputStream out = hdfs.create(new Path(srcDir + "/" + fileName));
		
		for (FileStatus fileStatu : fileStatus) {
			Path filePath = fileStatu.getPath();
			String file = fileStatu.getPath().getName();
			
			logger.info("开始合并文件:" + file);
			
			if (!file.equals(fileName)) {
				InputStream in = hdfs.open(filePath);
		        IOUtils.copyBytes(in, out, 4096, false);
		        if (!file.equals(fileName)) {
		          hdfs.delete(filePath, true);
		        }
		        in.close();
			}
			logger.info("文件:" + file + ",合并完成");
		}
		
		if (out != null) {
		      out.close();
	    }
		
		String today = DateUtil.dateStampToDay(System.currentTimeMillis());
		String file = "/home/hdsbi/shell/run.sh";
	    if (!new File(file).exists()){
	    	logger.error("文件：" + file + ",不存在");
	    	return false;
	    }
	    String cmd = "sh /home/hdsbi/shell/run.sh " + today;
	    
	    Process ps = Runtime.getRuntime().exec(cmd);
	    ps.waitFor();
	    try {
	    	if (ps.exitValue() != 0) {
	    		logger.error("call shell failed. error code is :" + ps.exitValue());
	    	}
	    }
	    catch (Exception e) {
	    	logger.error("call shell failed. " + e);
	    }
	    return true;
	}

	/*public static void main(String[] args) throws Exception {
		logger.info("-----------" + args[0] + "合并开始，开始时间为：" + DateUtil.dateStampToDate(System.currentTimeMillis()));
		String yestoday = DateUtil.dateDiff(DateUtil.dateStampToDay(System.currentTimeMillis()));
		String srcPath = "/pcbi/original/server/" + args[0] + "/dt=" + yestoday;
		String fileName = "events-" + args[0] + "." + yestoday + ".log";
		Configuration conf = new Configuration();

		conf.set("fs.defaultFS", path);
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		FileSystem hdfs = FileSystem.get(URI.create(path), conf);
		fileMerge(hdfs, srcPath, fileName, conf);
		logger.info("-----------" + args[0] + "合并结束，结束时间为：" + DateUtil.dateStampToDate(System.currentTimeMillis()));
	}*/
}
