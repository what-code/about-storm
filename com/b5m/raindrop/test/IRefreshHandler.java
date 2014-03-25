package com.b5m.raindrop.test;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.tuple.Tuple;

/**
 * 在{@link CounterBolt}中处理refresh的操作
 * @author jacky
 *
 */
public interface IRefreshHandler extends Serializable{

	/**
	 * 处理刷新的逻辑，这个方法将由{@link CounterBolt}触发。注意，在处理完成之后，
	 * 不要调用collector.ack方法，在{@link CounterBolt}中会调用这个方法。
	 * @param counter
	 * @param input
	 */
	public void refresh(Map<String, Long> counter, Tuple input);
}
