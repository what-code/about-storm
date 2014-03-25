package com.b5m.storm.hw;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.b5m.plugin.spout.CounterBean;

/**
 * Title:ExclamationBolt2.java
 * 
 * Description:ExclamationBolt2.java
 * 
 * Copyright: Copyright (c) 2014-3-25
 * 
 * Company: IZENE Software(Shanghai) Co., Ltd.
 * 
 * @author Shengjie Guo
 * 
 * @version 1.0
 */
public class ExclamationBolt2  extends BaseRichBolt{
	private static  ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<String, Integer>();
	OutputCollector collector;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		String category = tuple.getString(0);
		String componentId = tuple.getSourceComponent();
		if("bolt1".equals(componentId)){
			category = tuple.getString(1);
			System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%-->" + componentId + "--category----->" + category);
			Integer count = map.get(category);
			if (count == null) {
				map.put(category, 1);
			} else {
				map.put(category, count + 1);
			}
		}
		if (componentId.equals("test_signal")) {
			System.out
					.println("----**************************************exclaim2-->"
							+ category
							+ "---taskid--->"
							+ tuple
							+ "----map--->" + map);
		}
		collector.emit(tuple, new Values(category));
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("cat"));
	}

}
