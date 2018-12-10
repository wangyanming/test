package com.hds.mergefile;

import org.apache.hadoop.conf.Configuration;

public class HDFSConf {
	
	public static Configuration conf = null;
	public static Configuration getConf() {
		if (conf == null){
			conf = new Configuration();
			//String path  = Constant.getSysEnv("HADOOP_HOME")+"/etc/hadoop/";
			String path  = "/opt/cloudera/parcels/CDH/lib/hadoop/etc/hadoop";
			// hdfs conf
			conf.addResource(path+"core-site.xml");
			conf.addResource(path+"hdfs-site.xml");
			conf.addResource(path+"mapred-site.xml");
			conf.addResource(path+"yarn-site.xml");
		}
		return conf;
	}
}
