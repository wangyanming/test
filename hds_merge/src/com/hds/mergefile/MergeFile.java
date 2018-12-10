package com.hds.mergefile;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;

public class MergeFile {
	private static FileSystem fs = null;
	private static FileSystem local = null;

	/**
	 * @function main
	 * @param args
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void main(String[] args) throws IOException, URISyntaxException {
		list();
	}

	/**
	 * 
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void list() throws IOException, URISyntaxException {
		// ��ȡhadoop�ļ�ϵͳ������
		Configuration conf = new Configuration();
		// �ļ�ϵͳ���ʽӿ�
		URI uri = new URI("hdfs://39.107.79.26:9000");
		// ����FileSystem����
		fs = FileSystem.get(uri, conf);
		// ��ñ����ļ�ϵͳ
		local = FileSystem.getLocal(conf);
		// ����Ŀ¼�µ� svn
		// �ļ���globStatus�ӵ�һ������ͨ����ϵ��ļ����޳�����ڶ����������������ΪPathFilter��accept��return!
		FileStatus[] dirstatus = local.globStatus(new Path("D://data/73/*"), new RegexExcludePathFilter("^.*svn$"));
		// ��ȡ73Ŀ¼�µ������ļ�·����ע��FIleUtil��stat2Paths()��ʹ�ã�����һ��FileStatus��������ת��ΪPath�������顣
		Path[] dirs = FileUtil.stat2Paths(dirstatus);
		FSDataOutputStream out = null;
		FSDataInputStream in = null;
		for (Path dir : dirs) {
			String fileName = dir.getName().replace("-", "");// �ļ�����
			// ֻ��������Ŀ¼�µ�.txt�ļ���^ƥ�������ַ����Ŀ�ʼλ��,$ƥ�������ַ����Ľ���λ��,*ƥ��0�������ַ���
			FileStatus[] localStatus = local.globStatus(new Path(dir + "/*"), new RegexAcceptPathFilter("^.*txt$"));
			// �������Ŀ¼�µ������ļ�
			Path[] listedPaths = FileUtil.stat2Paths(localStatus);
			// ���·��
			Path block = new Path("hdfs://39.107.79.26:9000/middle/tv/" + fileName + ".txt");
			// �������
			out = fs.create(block);
			for (Path p : listedPaths) {
				in = local.open(p);// ��������
				IOUtils.copyBytes(in, out, 4096, false); // �������ݣ�IOUtils.copyBytes���Է���ؽ�����д�뵽�ļ�������Ҫ�Լ�ȥ���ƻ�������Ҳ�����Լ�ȥѭ����ȡ����Դ��false��ʾ���Զ��ر�����������ô���ֶ��رա�
				// �ر�������
				in.close();
			}
			if (out != null) {
				// �ر������
				out.close();
			}
		}

	}

	/**
	 * 
	 * @function ���� regex ��ʽ���ļ�
	 *
	 */
	public static class RegexExcludePathFilter implements PathFilter {
		public RegexExcludePathFilter(String regex) {
		}

		public boolean accept(Path arg0) {
			return false;
		}
	}

	/**
	 * 
	 * @function ���� regex ��ʽ���ļ�
	 *
	 */
	public static class RegexAcceptPathFilter implements PathFilter {
		public RegexAcceptPathFilter(String regex) {
		}
		
		public boolean accept(Path arg0) {
			return false;
		}
	}
}
