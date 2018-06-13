package com.bigdata.hadoop.hdfs.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class HdfsInit {

	public static void main(String[] args) {
		FileSystem filesystem = null;
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		try {
			filesystem = FileSystem.get(conf);
			FileStatus[] fs = filesystem.listStatus(new Path("/"));  // 斜杠是根目录
			Path[] paths = FileUtil.stat2Paths(fs);
			for (Path p : paths)
				System.out.println(p);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
