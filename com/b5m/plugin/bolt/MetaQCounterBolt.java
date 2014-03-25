package com.b5m.plugin.bolt;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.b5m.plugin.spout.LogSpout;
import com.b5m.plugin.util.MongoUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MetaQCounterBolt extends BaseBasicBolt {

	ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<String, Integer>();

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("cat", "count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		System.out.println("--=============MetaQCounterBolt==prepare-compId======-->" + context.getThisComponentId() + "----->" + context.getThisTaskId());
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		//String cat = (String)input.getString(0);
		String signals = input.getSourceComponent();
		System.out.println("--####################***********************signals***********-->" + signals + "---cat--->");
		/*Integer count = map.get(cat);
		if("signals".equals(input.getSourceStreamId())){
			System.out.println("---^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^map-->" + map);
		}else{
			if(count == null){
				map.put(cat, 1);
			}else{
				map.put(cat, count + 1);
			}
		}*/
		//collector.emit(new Values(cat, 0));
	}

	@Override
	public void cleanup() {

	}

}
