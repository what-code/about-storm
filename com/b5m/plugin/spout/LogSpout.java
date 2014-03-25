package com.b5m.plugin.spout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.b5m.plugin.bolt.PluginCountBolt;
import com.b5m.plugin.util.MongoUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

public class LogSpout implements IRichSpout{

	private MongoTemplate mt;
	private DBObject bd;
	String collectionName = "alermsum";
	Query query;
	Update update;
	final AtomicInteger counter1 = new AtomicInteger(0);
	public static Logger log = Logger.getLogger(LogSpout.class);
	private String tableName = "";
	private DBCursor cursor;
	private static String NEW_COLLECTION = "_new";
	
	private static String NEW_COLLECTION1 = "_error";
	
	private static String PLUGIN_VERSION_REPORT = "plugin_version_report";
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		log.info("plugin_count_init--->");
		mt = MongoUtils.getTemplateByDatabaseNameByURL("mongodb://10.10.99.183:5586", "b5m_plugin_log_20130426");
		cursor = mt.execute(tableName,
				new CollectionCallback<DBCursor>() {
					public DBCursor doInCollection(DBCollection collection)
							throws MongoException, DataAccessException {
						DBCursor dbCursor = collection.find(
								query.getQueryObject(), query.getFieldsObject());
						return dbCursor;
					}
				});
		update = new Update();
		bd = new BasicDBObject();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		String method = tableName.split("_")[1];
		Map<String, Integer> uuidSummarys = new HashMap<String, Integer>();
		int nullVersion = 0;
		int illegalVersion = 0;
		String collectionName = tableName + NEW_COLLECTION;
		String collectionName1 = tableName + NEW_COLLECTION1;
		final Query query = new Query();
		query.fields().include("version").exclude("_id").include("uuid");
		
		int i = 0;
		List list = new ArrayList();
		BasicDBObject bd = new BasicDBObject();
		long mis = System.currentTimeMillis();
		while (cursor.hasNext()) {
				boolean flag = true;
				DBObject obj = cursor.next();
		}
		
	}

	@Override
	public void ack(Object msgId) {
		
	}

	@Override
	public void fail(Object msgId) {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	private String toString(Object obj) {
		return obj == null ? null : obj.toString();
	}

}
