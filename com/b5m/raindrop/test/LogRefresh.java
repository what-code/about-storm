package com.b5m.raindrop.test;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.tuple.Tuple;


/**
 * 用于调试，当调用了refresh之后，将相关信息刷新到log4j中。
 * @author jacky
 *
 */
public class LogRefresh implements IRefreshHandler {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1613875673095124120L;

	private transient Logger log;
	
	private Logger getLog(){
		if(null == log)
			log = Logger.getLogger(this.getClass());
		return log;
	}
	
	@Override
	public void refresh(Map<String, Long> counter, Tuple input) {
		if(getLog().isInfoEnabled()){
			getLog().info(new StringBuilder("refreshed >> \n").append(counter).toString());
		}
	}

}
