package com.b5m.plugin.bolt;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.b5m.plugin.util.MongoUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class PluginMapBolt implements IBasicBolt{
	public static Logger log = Logger.getLogger(PluginCountBolt.class);
	private MongoTemplate mt;
	private DBObject bd;
	private String collectionName = "alermlog";
	final AtomicInteger counter1 = new AtomicInteger(0);
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		mt = MongoUtils.getTemplateByDatabaseNameByURL("mongodb://10.10.99.136:5586", "test");
		bd = new BasicDBObject();
		/*new Thread() {
			@Override
			public void run() {
				while (true) {
					try {
						Thread.sleep(2000);
						System.out.println("map_bolt--->" + counter1.get());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}.start();*/
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String msg = (String)input.getValueByField("metaq_spout");
		String[] arr = msg.split(",");
		String collectionName = "alermlog";
		//更新最新的uuid与原始uuid的对应关系
		Query query = new Query(Criteria.where("nid").is(arr[0]));
		List list = mt.find(query, bd.getClass(), collectionName);
		if(list.size() == 0){
			bd.put("oid",arr[0]);
			bd.put("nid",arr[1]);
			mt.save(bd,collectionName);
		}else{
			Update update = new Update();
			update.set("nid",arr[1]);
			mt.upsert(query, update, collectionName);
		}
		counter1.incrementAndGet();
		//log.info("plugin_map---->" + msg + "----" + arr[0] + "---" + arr[1]);
	}

	@Override
	public void cleanup() {
		
	}

	/*public static void main(String[] args) {
		String collectionName = "alermlog";
		//TODO 更新最新的uuid与原始uuid的对应关系
		MongoTemplate mt = MongoUtils.getTemplateByDatabaseNameByURL("mongodb://10.10.99.136:5586", "test");
		DBObject bd = new BasicDBObject();
		Query query = new Query(Criteria.where("nid").is("3"));
		List list = mt.find(query, bd.getClass(), collectionName);
		System.out.println("list--->" + list.size());
		if(list.size() == 0){
			bd.put("oid","1");
			bd.put("nid","2");
			mt.save(bd, collectionName);
		}else{
			Update update = new Update();
			update.set("nid","4");
			mt.upsert(query, update, collectionName);
		}
		
	}*/
}
