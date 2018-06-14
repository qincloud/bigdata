package com.bigdata.hadoop.hdfs.demo;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.bigdata.hadoop.hdfs.utils.HdfsUtils;

public class UtilsTest {

	@Test
	public void test1() {
		List<Path> paths = HdfsUtils.getFiles("/");
		for (Path path : paths) {
			System.out.println(path);
		}
	}

	@Test
	public void testmkdir() {
		System.setProperty("HADOOP_USER_NAME", "root");
		HdfsUtils.mkdir("/upload");
	}

	@Test
	public void showAllConf() {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://huabingood01:9000");
		Iterator<Map.Entry<String, String>> it = conf.iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> entry = it.next();
			System.out.println(entry.getKey() + "=" + entry.getValue());
		}
	}
}
