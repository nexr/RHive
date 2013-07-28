package com.nexr.rhive.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtils {
	private static final Configuration conf = new Configuration();
	
	public static void copyFromLocal(String src, String dst, boolean delSrc, boolean overwrite) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		fs.copyFromLocalFile(delSrc, true, new Path(src), new Path(dst));
	}
}
