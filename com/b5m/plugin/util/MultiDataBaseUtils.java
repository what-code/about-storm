package com.b5m.plugin.util;

public class MultiDataBaseUtils {
	private static ThreadLocal<String> dataBaseLocal = new ThreadLocal<String>();

	public static void setDataBaseName(String name) {
		dataBaseLocal.set(name);
	}

	public static String getDataBaseName() {
		return dataBaseLocal.get();
	}
	
	public static void clear(){
		dataBaseLocal.remove();
		dataBaseLocal.set(null);
	}
}
