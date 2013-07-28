package com.nexr.rhive.hadoop;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.DFSck;


public class FSUtils {
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	 
	public static boolean checkFileSystem(String defaultFS) throws IOException {
		Configuration conf = getConf(defaultFS);
		try {
			FileSystem.get(conf);
		} catch (Exception e) {
			return false;
		}
		
		return true;
	}
	
	public static String[][] ls(String src, String defaultFS) throws IOException {
		Configuration conf = getConf(defaultFS);
		
		Path srcPath = new Path(src);
		FileSystem fs = null;

		List<FileStatus> items = new ArrayList<FileStatus>();
		try {
			fs = srcPath.getFileSystem(conf);
			FileStatus[] stats = fs.globStatus(srcPath);
			if (stats != null) {
				for (FileStatus stat : stats) {
					if (!stat.isDir()) {
						items.add(stat);
					} else {
						Path path = stat.getPath();
						FileStatus files[] = fs.listStatus(path);
						if (files != null) {
							for (FileStatus file : files) {
								items.add(file);
							}
						}
					}
				}
			}
		} finally {
			closeFileSystem(fs);
		}
		
		String[][] rlist = new String[6][items.size()];
		for (int i = 0; i < 6; i++) {
			rlist[i] = new String[items.size()];
		}
		
		for (int i = 0; i < 6; i++) {
			for (int j = 0; j < items.size(); j++) {
				FileStatus item = items.get(j);
				
				rlist[0][j] = item.getPermission().toString();
				rlist[1][j] = item.getOwner();
				rlist[2][j] = item.getGroup();
				rlist[3][j] = String.valueOf(item.getLen());
				
				Date date = new Date(item.getModificationTime());
				rlist[4][j] = dateFormat.format(date);
				rlist[5][j] = item.getPath().toUri().getPath();
			}
		}
			
	
		return rlist;
	}

	private static int ls(FileStatus src, FileSystem srcFs) throws IOException {
		FileStatus items[] = listStatus(srcFs, src);

		int numOfErrors = 0;

		for (int i = 0; i < items.length; i++) {
			FileStatus stat = items[i];

			int replication = String.valueOf(stat.getReplication()).length();
			int len = String.valueOf(stat.getLen()).length();
			int owner = String.valueOf(stat.getOwner()).length();
			int group = String.valueOf(stat.getGroup()).length();
		}

		for (int i = 0; i < items.length; i++) {
			FileStatus item = items[i];
			item.getPermission().toString();
			item.getOwner();
			item.getGroup();
			String.valueOf(item.getLen());
			
			Date date = new Date(item.getModificationTime());
			dateFormat.format(date);
			item.getPath().toUri().getPath();
		}

		return numOfErrors;
	}

	private static FileStatus[] listStatus(FileSystem srcFs, FileStatus src) throws IOException {
		if (!src.isDir()) {
			FileStatus files[] = { src };
			return files;
		}
		
		Path path = src.getPath();
		FileStatus files[] = srcFs.listStatus(path);
		if (files == null) {
			files = new FileStatus[0];
		}
	
		return files;
	}  

	public static String[][] du(String src, String defaultFS) throws IOException {
		Configuration conf = getConf(defaultFS);
		
		Path srcPath = new Path(src);
		FileSystem fs = null;
		FileStatus items[] = null;
		
		long length[] = null;
		try {
			fs = srcPath.getFileSystem(conf);
			Path pathItems[] = FileUtil.stat2Paths(fs.globStatus(srcPath), srcPath);
			items = fs.listStatus(pathItems);
			if (items == null) {
				items = new FileStatus[0];
			}
			
			length = new long[items.length];
			for (int i = 0; i < items.length; i++) {
				length[i] = items[i].isDir() ? fs.getContentSummary(items[i].getPath()).getLength() : items[i].getLen();
			}
 
		} finally {
			closeFileSystem(fs);
		}
		
		String[][] rlist = new String[2][items.length];
		for (int i = 0; i < 2; i++) {
			rlist[i] = new String[items.length];
		}
		
		for (int i = 0; i < 2; i++) {
			for (int j = 0; j < items.length; j++) {
				FileStatus item = items[j];
				
				rlist[0][j] = String.valueOf(length[j]);
				rlist[1][j] = item.getPath().toUri().getPath();
			}
		}
			
		return rlist;
	}

	public static String[][] dus(String src, String defaultFS) throws IOException {
		Configuration conf = getConf(defaultFS);
		
		Path srcPath = new Path(src);
		FileSystem fs = null;
		FileStatus status[] = null;
		
		long length[] = null;
		try {
			fs = srcPath.getFileSystem(conf);
			status = fs.globStatus(new Path(src));
			if (status == null) {
				status = new FileStatus[0];
			}

			length = new long[status.length];
			for (int i = 0; i < status.length; i++) {
				length[i] = fs.getContentSummary(status[i].getPath()).getLength();
			}
 
		} finally {
			closeFileSystem(fs);
		}
		
		String[][] rlist = new String[2][status.length];
		for (int i = 0; i < 2; i++) {
			rlist[i] = new String[status.length];
		}
		
		for (int i = 0; i < 2; i++) {
			for (int j = 0; j < status.length; j++) {
				FileStatus item = status[j];
				
				rlist[0][j] = String.valueOf(length[j]);
				rlist[1][j] = item.getPath().toUri().getPath();
			}
		}
			
		return rlist;
	}

	public static void copyFromLocalFile(boolean delSrc, boolean overwrite, String src, String dst, String defaultFS) throws IOException {
		Configuration conf = getConf(defaultFS);
		
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			fs.copyFromLocalFile(delSrc, overwrite, new Path(src), new Path(dst));
		} finally {
			closeFileSystem(fs);
		}
	}

	public static void copyToLocalFile(boolean delSrc, String src, String dst, String defaultFS) throws IOException {
		Configuration conf = getConf(defaultFS); 
		
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			fs.copyToLocalFile(delSrc, new Path(src), new Path(dst));
		} finally {
			closeFileSystem(fs);
		}
	}
	
	public static boolean delete(String file, String defaultFS) throws IOException {
		Configuration conf = getConf(defaultFS); 
		
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			return fs.delete(new Path(file), true);
		} finally {
			closeFileSystem(fs);
		}
	}
	
	public static boolean rename(String src, String dst, String defaultFS) throws IOException {
		Configuration conf = getConf(defaultFS); 
		
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			return fs.rename(new Path(src), new Path(dst));
		} finally {
			closeFileSystem(fs);
		}
	}
	
	public static boolean exists(String file, String defaultFS) throws IOException {
		Configuration conf = getConf(defaultFS); 
		
		Path path = new Path(file);
		FileSystem fs = null;
		try {
			fs = path.getFileSystem(conf);
			return fs.exists(path);
		} finally {
			closeFileSystem(fs);
		}
	}

	public static boolean mkdirs(String file, String defaultFS) throws IOException {
		Configuration conf = getConf(defaultFS); 
		
		Path path = new Path(file);
		FileSystem fs = null;
		try {
			fs = path.getFileSystem(conf);
			return fs.mkdirs(path);
		} finally {
			closeFileSystem(fs);
		}
	}
	
	public static void cat(String src, String defaultFS) throws Exception {
		Configuration conf = getConf(defaultFS); 
		
		FsShell fsShell = new FsShell(conf);
		fsShell.run(new String[] { "-cat", src });
	}
	
	public static void tail(String src, String defaultFS) throws Exception {
		Configuration conf = getConf(defaultFS); 
		
		FsShell fsShell = new FsShell(conf);
		fsShell.run(new String[] { "-tail", src });
	}

	public static void chmod(String src, String option, boolean recursive, String defaultFS) throws Exception {
		Configuration conf = getConf(defaultFS); 
		
		FsShell fsShell = new FsShell(conf);
		
		if (recursive) {
			fsShell.run(new String[] { "-chmod", "-R", option, src });
		} else {
			fsShell.run(new String[] { "-chmod", option, src });
		}
	}

	public static void chown(String src, String option, boolean recursive, String defaultFS) throws Exception {
		Configuration conf = getConf(defaultFS); 
		
		FsShell fsShell = new FsShell(conf);
		
		if (recursive) {
			fsShell.run(new String[] { "-chown", "-R", option, src });
		} else {
			fsShell.run(new String[] { "-chown", option, src });
		}
	}

	public static void chgrp(String src, String option, boolean recursive, String defaultFS) throws Exception {
		Configuration conf = getConf(defaultFS); 
		
		FsShell fsShell = new FsShell(conf);
		
		if (recursive) {
			fsShell.run(new String[] { "-chgrp", "-R", option, src });
		} else {
			fsShell.run(new String[] { "-chgrp", option, src });
		}
	}
	
	public static void info(String src) throws IOException {
		Configuration conf = getConf(null); 

		DFSck dfsCk = new DFSck(conf);
		dfsCk.run(new String[] { src });
	}
	
	
	private static void closeFileSystem(FileSystem fs) {
		try {
			if (fs != null) {
				fs.close();
			}
		} catch (IOException e) { }
	}
	
	
	private static Configuration getConf(String defaultFS) {
		Configuration conf = new Configuration();
		
		if (defaultFS != null) {
			FileSystem.setDefaultUri(conf, defaultFS);
		}
		
		return conf;
	}
}