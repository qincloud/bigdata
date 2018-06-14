package com.bigdata.hadoop.hdfs.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * hdfs工具类
 */
public class HdfsUtils {

	/**
	 * 获得初始化hdfs
	 */
	public static FileSystem getHdfs() {
		FileSystem filesystem = null;
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000"); // 必须要和hadoop的hdfs配置一致
		try {
			filesystem = FileSystem.get(conf);
			return filesystem;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 获取目录下有子目录或子文件
	 * 
	 * @param path:目录路径
	 */
	public static List<Path> getFiles(String path) {
		if (path.startsWith("/")) { // 必须已斜杠开头
			List<Path> lists = new ArrayList<Path>();
			FileSystem filesystem = getHdfs();
			try {
				FileStatus[] fs = filesystem.listStatus(new Path(path)); // 斜杠是根目录
				Path[] paths = FileUtil.stat2Paths(fs);
				for (Path p : paths) {
					lists.add(p);
				}
				return lists;
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			} finally {
				try {
					filesystem.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	/**
	 * 上传一个文件，到hdfs
	 */
	public static void CreateFile(File file, String path) {
		if (path.startsWith("/")) {
			InputStream in = null;
			FileSystem filesystem = getHdfs();
			try {
				FSDataOutputStream out = filesystem.create(new Path(path));
				in = new BufferedInputStream(new FileInputStream(file));
				IOUtils.copyBytes(in, out, 4096, false);
				out.hsync();
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				IOUtils.closeStream(in);
				try {
					filesystem.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 上传一个文件，到hdfs
	 */
	public static void CreateFile(File file, Path path) {
		InputStream in = null;
		FileSystem filesystem = getHdfs();
		try {
			FSDataOutputStream out = filesystem.create(path);
			in = new BufferedInputStream(new FileInputStream(file));
			IOUtils.copyBytes(in, out, 4096, false);
			out.hsync();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
			try {
				filesystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 创建一个文件夹
	 */
	public static boolean mkdir(String dir) {
		FileSystem filesystem = getHdfs();
		try {
			if (!filesystem.exists(new Path(dir))) {
				filesystem.mkdirs(new Path(dir));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				filesystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return true;
	}

	/**
	 * 创建一个文件夹
	 */
	public static boolean mkdir(Path dir) {
		FileSystem filesystem = getHdfs();
		try {
			if (!filesystem.exists(dir)) {
				filesystem.mkdirs(dir);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				filesystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return true;
	}

	/**
	 * 删除一个文件
	 */
	public static boolean delete(String file) {
		boolean b = false;
		FileSystem filesystem = getHdfs();
		try {
			Path path = new Path(file);
			b = filesystem.delete(path, true);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				filesystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return b;
	}

	/**
	 * 删除一个文件
	 */
	public static boolean delete(Path path) {
		boolean b = false;
		FileSystem filesystem = getHdfs();
		try {
			b = filesystem.delete(path, true);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				filesystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return b;
	}

	/**
	 * 重命名文件
	 */
	public static boolean rename(String oldfile, String newfile) {
		boolean b = false;
		FileSystem filesystem = getHdfs();
		try {
			Path oldpath = new Path(oldfile);
			Path newpath = new Path(newfile);
			b = filesystem.rename(oldpath, newpath);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				filesystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return b;
	}

	/**
	 * 重命名文件
	 */
	public static boolean rename(Path oldpath, Path newpath) {
		boolean b = false;
		FileSystem filesystem = getHdfs();
		try {
			b = filesystem.rename(oldpath, newpath);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				filesystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return b;
	}

	/**
	 * 判断是否存在
	 */
	public static boolean isExists(String str) {
		boolean b = false;
		FileSystem filesystem = getHdfs();
		try {
			Path path = new Path(str);
			b = filesystem.exists(path);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				filesystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return b;
	}

	/**
	 * 判断是否是文件
	 */
	public static boolean isFile(String str) {
		boolean b = false;
		FileSystem filesystem = getHdfs();
		try {
			Path path = new Path(str);
			b = filesystem.isFile(path);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				filesystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return b;
	}

	/**
	 * 判断是否是文件夹
	 */
	public static boolean isDictionary(String str) {
		boolean b = false;
		FileSystem filesystem = getHdfs();
		try {
			Path path = new Path(str);
			b = filesystem.isDirectory(path);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				filesystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return b;
	}
	
	/**
	 * 本地文件上传到hdfs
	 * */


}
