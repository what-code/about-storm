package com.b5m.plugin.bolt;

import java.util.Map;
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
import backtype.storm.tuple.Tuple;

public class LogBolt implements IBasicBolt {

	private MongoTemplate mt;
	private DBObject bd;
	Query query;
	Update update;
	final AtomicInteger counter1 = new AtomicInteger(0);
	public static Logger log = Logger.getLogger(LogSpout.class);
	private String tableName = "";
	private DBCursor cursor;
	private static String NEW_COLLECTION = "_new";

	private static String NEW_COLLECTION1 = "_error";

	private static String PLUGIN_VERSION_REPORT = "plugin_version_report";

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		log.info("plugin_count_init--->");
		mt = MongoUtils.getTemplateByDatabaseNameByURL(
				"mongodb://10.10.99.136:5586", "b5m_plugin_analysis_jslog");
		cursor = mt.execute(tableName, new CollectionCallback<DBCursor>() {
			public DBCursor doInCollection(DBCollection collection)
					throws MongoException, DataAccessException {
				DBCursor dbCursor = collection.find(query.getQueryObject(),
						query.getFieldsObject());
				return dbCursor;
			}
		});
		update = new Update();
		bd = new BasicDBObject();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
