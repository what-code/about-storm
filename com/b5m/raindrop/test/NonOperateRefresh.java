package com.b5m.raindrop.test;

import java.util.Map;

import backtype.storm.tuple.Tuple;


/**
 * 此刷新操作不做任何处理
 * @author jacky
 *
 */
public class NonOperateRefresh implements IRefreshHandler {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2161751803189742556L;

	@Override
	public void refresh(Map<String, Long> counter, Tuple input) {
		
	}

}
