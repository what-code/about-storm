package com.b5m.plugin.bolt;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.b5m.plugin.spout.PluginSpout;
import com.b5m.plugin.util.MongoUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class PluginCountBolt implements IBasicBolt{
	private int count = 0;
	public static Logger log = Logger.getLogger(PluginCountBolt.class);
	private Map<String, Integer> resultMap = new HashMap<String, Integer>();
	private MongoTemplate mt;
	private DBObject bd;
	String collectionName = "alermsum";
	Query query;
	Update update;
	final AtomicInteger counter1 = new AtomicInteger(0);
	
	int taskId;
	String componentId;
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("plugin_count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.taskId = context.getThisTaskId();
		this.componentId = context.getThisComponentId();
		log.info("plugin_count_init--->" + componentId + "---" + taskId);
		mt = MongoUtils.getTemplateByDatabaseNameByURL("mongodb://10.10.99.136:5586", "test");
		update = new Update();
		bd = new BasicDBObject();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String msg = (String)input.getValueByField("metaq_spout");
		long msg1 = (Long)input.getValueByField("mis");
		log.info("plugin_count--->" + msg + "---" + msg1 + "----" + input.getSourceTask() + "---" + Thread.activeCount() + "---" + mt);
		List list = input.getValues();
		String[] arr = msg.split(",");
		String key = arr[2];
		
		query = new Query(Criteria.where("chn").is(key));
		update.inc("count",1);
		mt.upsert(query, update, collectionName);
		query = null;
		counter1.incrementAndGet();
	}

	@Override
	public void cleanup() {
	
	}
}
