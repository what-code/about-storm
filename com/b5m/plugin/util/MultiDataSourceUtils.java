package com.b5m.plugin.util;

public class MultiDataSourceUtils {
	private static ThreadLocal<String> dataSourceLocal = new ThreadLocal<String>();

	public static void setDataSourceName(String name) {
		dataSourceLocal.set(name);
	}

	public static String getDataSourceName() {
		return dataSourceLocal.get();
	}
	public static void clear(){
		dataSourceLocal.remove();
		dataSourceLocal.set(null);
	}
}
