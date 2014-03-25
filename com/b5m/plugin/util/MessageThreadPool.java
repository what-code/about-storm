package com.b5m.plugin.util;

import java.util.concurrent.ThreadPoolExecutor;

public enum MessageThreadPool {
	instace;
	public ThreadPoolExecutor pool=ThreadPoolFactory.newMessageThreadPool(Runtime.getRuntime().availableProcessors(),Runtime.getRuntime().availableProcessors()*3, 10000);
	 
}
