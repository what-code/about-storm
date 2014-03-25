package com.b5m.plugin.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.b5m.plugin.spout.CounterBean;
import com.b5m.plugin.spout.LogSpout;
import com.b5m.plugin.util.MongoUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LogMetaQBolt extends BaseBasicBolt {

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("gid","source"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		System.out.println("--=============LogMetaQBolt==prepare-compId======-->" + context.getThisComponentId() + "----->" + context.getThisTaskId());
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if(input.getValue(0) instanceof CounterBean && input.getValue(0) != null){
			CounterBean counter = (CounterBean)input.getValue(0);
			String gid = counter.getId();
			String cat = counter.getSource();
			String compId = input.getSourceComponent();
			String taskId = input.getSourceTask() + "";
			System.out.println("--?????????????????????????##############################compId-->" + compId + "---task--->" + taskId + "----->" + cat);
			collector.emit("log_metaq_bolit", new Values(gid,cat));
		}
	}

	@Override
	public void cleanup() {

	}
	
	public static void main(String[] args){
		String temp = "\"test\"";
		Map map = new HashMap();
		map.put("a1", "a1");
		map.put("a2", "a2");
		
		System.out.println(map);
	}

}
