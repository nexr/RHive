package com.nexr.rhive.hive.udf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;


public class UDFUtils {
	private static final String DEFAULT_UDF_DIR = "/rhive/udf";
	private static final String RDATA_FILE_EXT = ".RData";
	
	private static final Configuration conf = new Configuration();

	public static Path getPath(String name) {
		return new Path(getBaseDirectory(), getFileName(name));
	}
	
	public static void export(String name, String src) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		
		boolean delSrc = true;
		boolean overwrite = true;
		Path dst = getPath(name);
		fs.copyFromLocalFile(delSrc, overwrite, new Path(src), dst);
	}

	public static String getBaseDirectory() {
		String base = System.getProperty("RHIVE_UDF_DIR");
		if (base != null) {
			return base;
		}
		
		base = System.getenv("RHIVE_UDF_DIR");
		if (base != null) {
			return base;
		}
		
		return DEFAULT_UDF_DIR;
	}
	
	public static String getFileName(String name) {
		return name + getRDataFileExtension();
	}
	
	public static String[] list() throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] listStatus = fs.listStatus(new Path(getBaseDirectory()), RDataPathFilter.instance);
		
		String[] names = new String[listStatus.length];
		for (int i = 0; i < names.length; i++) {
			FileStatus status = listStatus[i];
			Path path = status.getPath();
			String name = path.getName();
			name = name.substring(0, name.length() - getRDataFileExtension().length());
			names[i] = name;
		}
		
		return names;
	}
	
	public static boolean delete(String name) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] listStatus = fs.listStatus(new Path(getBaseDirectory()), RDataPathFilter.instance);

		String fileName = name + getRDataFileExtension();
		for(FileStatus status : listStatus) {
			Path path = status.getPath();
			
			if (fileName.equals(path.getName())) {
				return fs.delete(path, true);
			}
		}
		
		return false;
	}
	
	private static String getRDataFileExtension() {
		return RDATA_FILE_EXT;
	}
	
	private static class RDataPathFilter implements PathFilter {
		private static RDataPathFilter instance = new RDataPathFilter();
		
		@Override
		public boolean accept(Path path) {
			String name = path.getName();
			return name.endsWith(getRDataFileExtension());
		}
	}
}