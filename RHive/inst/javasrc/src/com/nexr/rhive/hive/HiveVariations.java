package com.nexr.rhive.hive;

import org.apache.commons.lang.ClassUtils;

public class HiveVariations {
	public static Class serdeConstants = null;
	
	static {
		try {
			serdeConstants = ClassUtils.getClass("org.apache.hadoop.hive.serde.serdeConstants");
		} catch (ClassNotFoundException e1) {
			try {
				serdeConstants = ClassUtils.getClass("org.apache.hadoop.hive.serde.Constants");
			} catch (ClassNotFoundException e2) { 
				throw new ExceptionInInitializerError();
			}
		}
	}
	
	public static Object getFieldValue(Class clss, String fieldName) throws IllegalArgumentException, SecurityException, IllegalAccessException, NoSuchFieldException {
		return clss.getField(fieldName).get(null);
	}
}
