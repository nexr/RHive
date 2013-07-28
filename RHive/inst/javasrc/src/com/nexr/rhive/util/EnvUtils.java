package com.nexr.rhive.util;

import java.io.File;

public class EnvUtils {
	
	public static String getUserName() {
		return System.getProperty("user.name");
	}
	
	public static String getUserHome() {
		return System.getProperty("user.home");
	}
	
	public static String getTempDirectory() {
		String userName = getUserName();
		String tempDir = System.getProperty("java.io.tmpdir");
		
		return String.format("%s%s%s", tempDir, File.separator, userName);
	}
}
