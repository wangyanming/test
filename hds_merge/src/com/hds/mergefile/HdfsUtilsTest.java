package com.hds.mergefile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtilsTest {
	public static void fileMergeFunc(String srcDir, String targetDir, String localDir, String targetName, String regStr)
			throws IOException {
		Configuration conf = new Configuration();

		Path localPath = new Path(localDir);
		Path srcPath = new Path(srcDir);
		// 整合后的目录
		Path targetPathFile = new Path(targetDir + "/" + targetName);
		// fs是HDFS文件系统
		FileSystem hadoopFs = srcPath.getFileSystem(conf);
		// 本地文件系统
		FileSystem localFs = FileSystem.getLocal(conf);

		if (localFs.exists(localPath)) {
			System.out.println("Deleting local directory...");
			localFs.delete(localPath, true);
		}
		localFs.mkdirs(localPath);

		FileStatus[] HdfsStatus = hadoopFs.listStatus(srcPath);
		for (FileStatus st : HdfsStatus) {
			Path tmpPath = st.getPath();
			if (Pattern.matches(srcDir + regStr, tmpPath.toString())) {
				System.out.println("Coping hadoop files " + st.getPath() + " to local directory...");
				hadoopFs.copyToLocalFile(tmpPath, new Path(localDir + "/" + st.getPath().getName()));
				hadoopFs.delete(tmpPath, false);
			}
		}

		FileStatus[] status = localFs.listStatus(localPath); // 得到输入目录
		FSDataOutputStream out = hadoopFs.create(targetPathFile); // 在HDFS上创建输出文件
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, "UTF-8")); // 创建写入流
		StringBuffer sb = new StringBuffer();
		String line;
		System.out.println("Merge Files...");
		for (FileStatus st : status) {
			Path temp = st.getPath();
			FSDataInputStream in = localFs.open(temp);
			BufferedReader bf = new BufferedReader(new InputStreamReader(in, "UTF-8"));
			while ((line = bf.readLine()) != null) {
				if (line != "\n") {
					sb.append(line);
				}
			}
			// IOUtils.copyBytes(in, out, 4096, false); //读取in流中的内容放入out
			bf.close();
			in.close(); // 完成后，关闭当前文件输入流
		}
		bw.write(sb.toString());
		out.close();
		bw.close();
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 6) {
			System.out.println("入参格式不正确，请修正，请检查...");
		} else {
			SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
			SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHH");
			Calendar cal1 = Calendar.getInstance();
			cal1.add(Calendar.HOUR, Integer.parseInt(args[5]));

			Calendar cal2 = Calendar.getInstance();
			cal2.add(Calendar.HOUR, Integer.parseInt(args[5]));
			String dt1 = sdf1.format(cal1.getTime());
			String dt2 = sdf2.format(cal2.getTime());

			String srcDir = args[0];
			String targetDir = args[1] + dt1;
			String localDir = args[2];
			String targetName = args[3] + "_" + dt2;
			String regStr = args[4].replace("{DATE_TIME}", dt2);

			System.out.println(srcDir);
			System.out.println(targetDir);
			System.out.println(localDir);
			System.out.println(targetName);
			System.out.println(regStr);
			fileMergeFunc(srcDir, targetDir, localDir, targetName, regStr);
		}
	}
}
