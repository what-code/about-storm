package group.test;


import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.IndexOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.b5m.plugin.util.MongoUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

public class MinuteGeneratorNew {
	private Log loger = LogFactory.getLog(MinuteGeneratorNew.class);
	private MongoTemplate template136;

	private MongoTemplate template138;

	private MongoTemplate template138_analysis;

	private static ThreadPoolExecutor executor;
	
	private static String NEW_COLLECTION = "_new";
	
	private static String NEW_COLLECTION1 = "_error";
	
	private static String PLUGIN_VERSION_REPORT = "plugin_version_report";
	
	enum AggregateFields {
		index, install, install2, js, search, fastparity, recommend, installbymac, sexysearch, sexyrecommend, tackrecommend, sexyall
	}
	
	{
		int N_CPUS = Runtime.getRuntime().availableProcessors();
		executor = new ThreadPoolExecutor(N_CPUS, N_CPUS + 30, 1L,
				TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(100000),
				new ThreadPoolExecutor.CallerRunsPolicy());
	}
	
	public MinuteGeneratorNew(String sn) throws Exception {
		template138 = MongoUtils.getTemplateByDatabaseNameByURL(
				"mongodb://10.10.99.183:5586", "b5m_plugin_log_" + sn);
		template136 = MongoUtils.getTemplateByDatabaseNameByURL(
				"mongodb://10.10.99.181:6686", "b5m_app_report" + sn);

		template138_analysis = MongoUtils.getTemplateByDatabaseNameByURL(
				"mongodb://10.10.99.136:5586", "b5m_plugin_analysis_jslog");
		IndexOperations ops = template136.indexOps("pluginLogSummary");
		ops.ensureIndex(new IndexDefinition() {
			@Override
			public DBObject getIndexOptions() {
				return BasicDBObjectBuilder.start().add("background", true)
						.get();
			}

			@Override
			public DBObject getIndexKeys() {
				return BasicDBObjectBuilder.start().add("chnl", 1)
						.add("uuid", 1).add("sn", 1).get();
			}
		});
	}

	/**
	 * 统计对uuid去重后的版本计数
	 * @param table
	 * @param snn
	 */
	public void getUniqueUuidCount(String table, String date) {
		String newTable = table + NEW_COLLECTION;
		String method = table.split("_")[1];
		//uuid去重后的数据
		Map<String, Integer> uniqueUuidSummarys = new HashMap<String, Integer>();
		final Query query = new Query();
		query.fields().include("version").exclude("_id");
		DBCursor cursor = template138.execute(newTable,
				new CollectionCallback<DBCursor>() {
					public DBCursor doInCollection(DBCollection collection)
							throws MongoException, DataAccessException {
						DBCursor dbCursor = collection.find(
								query.getQueryObject(), query.getFieldsObject());
						return dbCursor;
					}
				});
		long mis = System.currentTimeMillis();
		
		while (cursor.hasNext()) {
			DBObject obj = cursor.next();
			String version = toString(obj.get("version"));
			//所有数据
			Integer verSum = uniqueUuidSummarys.get(version);
			if (verSum == null) {
				uniqueUuidSummarys.put(version, 1);
			} else {
				uniqueUuidSummarys.put(version, verSum + 1);
			}
		}
		loger.info("---getVersion cost:" + (System.currentTimeMillis()-mis) + "   ms");
		//所有
		for (Entry<String, Integer> entry : uniqueUuidSummarys.entrySet()) {
			String version = entry.getKey();
			Integer aggregateValues = entry.getValue();
			final Query qu = Query.query(Criteria.where("version").is(version)
					.and("sn").is(date));
			final Update update = new Update();
			update.set(method, aggregateValues);
			//0:1分别表示 所有：对uuid去重后的数据
			update.set("type", 1);
			template138_analysis.upsert(qu, update,PLUGIN_VERSION_REPORT);
		}
	}
	/**
	 * 将非法version、version为null去掉,然后根据uuid去重入库,并统计出为去重的版本号计数
	 * @param table
	 * @param date
	 */
	public void getSummaryData(String table, String date) {
		String method = table.split("_")[1];
		Map<String, Integer> uuidSummarys = new HashMap<String, Integer>();
		int nullVersion = 0;
		int illegalVersion = 0;
		String collectionName = table + NEW_COLLECTION;
		String collectionName1 = table + NEW_COLLECTION1;
		final Query query = new Query();
		query.fields().include("version").exclude("_id").include("uuid");
		DBCursor cursor = template138.execute(table,
				new CollectionCallback<DBCursor>() {
					public DBCursor doInCollection(DBCollection collection)
							throws MongoException, DataAccessException {
						DBCursor dbCursor = collection.find(
								query.getQueryObject(), query.getFieldsObject());
						return dbCursor;
					}
				});
		int i = 0;
		List list = new ArrayList();
		BasicDBObject bd = new BasicDBObject();
		long mis = System.currentTimeMillis();
		//uuid去重
		//while (cursor.hasNext()) {
		while (i<10000) {
			i++;
			boolean flag = true;
			DBObject obj = cursor.next();
			String version = toString(obj.get("version"));
			String uuid = toString(obj.get("uuid"));
			//查看新表数据是否存在
			final Query qu = Query.query(Criteria.where("uuid").is(uuid)
					.and("sn").is(date));
			list = template138_analysis.find(qu,bd.getClass(),collectionName);
			//所有数据
			Integer verSum = uuidSummarys.get(version);
			if (verSum == null) {
				uuidSummarys.put(version, 1);
			} else {
				uuidSummarys.put(version, verSum + 1);
			}
			//version为null数据
			if(version == null){
				nullVersion++;
				flag = false;
			}
			//非法version数据
			if(version.length() < 5 || version.split("\\.").length < 3){
				illegalVersion++;
				flag = false;
			}
			//保存入新表
			if(flag && list.size()==0){
				template138_analysis.save(obj, collectionName);
			}
			
		}
		loger.info("---updateLogWithUniqueUuid cost:" + (System.currentTimeMillis()-mis) + "   ms");
		//将数据入库(内容为：版本号---计数，未对uuid去重)
		for (Entry<String, Integer> entry : uuidSummarys.entrySet()) {
			String version = entry.getKey();
			Integer aggregateValues = entry.getValue();
			final Query qu = Query.query(Criteria.where("version").is(version)
					.and("sn").is(date));
			final Update update = new Update();
			update.set(method, aggregateValues);
			//0:1分别表示 所有：对uuid去重后的数据
			update.set("type", 0);
			template138_analysis.upsert(qu, update,PLUGIN_VERSION_REPORT);		
		}
		
		//保存非法version及null version的统计结果入库
		bd.put("date",nullVersion);
		bd.put("null_version",nullVersion);
		bd.put("illegal_version",illegalVersion);
		template138_analysis.save(bd, collectionName1);
		
		//统计去重后的uuid
		getUniqueUuidCount(table,date);
		
		loger.info("---getSummaryData finish in:" + (System.currentTimeMillis()-mis) + "   ms");
	}

	//test
	public void getVersionNew1(String table, String date) {
		String method = table.split("_")[1];
		Map<String, Integer> uuidSummarys = new HashMap<String, Integer>();
		int nullVersion = 0;
		int illegalVersion = 0;
		String collectionName = table + NEW_COLLECTION;
		String collectionName1 = table + NEW_COLLECTION1;
		final Query query = new Query();
		query.fields().include("version").exclude("_id").include("uuid");
		DBCursor cursor = template138.execute(table,
				new CollectionCallback<DBCursor>() {
					public DBCursor doInCollection(DBCollection collection)
							throws MongoException, DataAccessException {
						DBCursor dbCursor = collection.find(
								query.getQueryObject(), query.getFieldsObject());
						return dbCursor;
					}
				});
		int i = 0;
		List list = new ArrayList();
		BasicDBObject bd = new BasicDBObject();
		long mis = System.currentTimeMillis();
		//template138_analysis.createCollection(collectionName);
		//uuid去重
		while (i < 10000) {
			i++;
			boolean flag = true;
			DBObject obj = cursor.next();
			String version = toString(obj.get("version"));
			String uuid = toString(obj.get("uuid"));
			//查看新表数据是否存在
			final Query qu = Query.query(Criteria.where("uuid").is(uuid)
					.and("sn").is(date));
			list = template138_analysis.find(qu,bd.getClass(),collectionName);
			//所有数据
			Integer verSum = uuidSummarys.get(version);
			if (verSum == null) {
				uuidSummarys.put(version, 1);
			} else {
				uuidSummarys.put(version, verSum + 1);
			}
			//version为null数据
			if(version == null){
				nullVersion++;
				flag = false;
			}
			//非法version数据
			if(version.length() < 5 || version.split("\\.").length < 3){
				illegalVersion++;
				flag = false;
			}
			//保存入新表
			if(flag && list.size()==0){
				template138_analysis.save(obj, collectionName);
			}
			
		}
		loger.info("---updateLogWithUniqueUuid cost:" + (System.currentTimeMillis()-mis));
		//将数据入库(内容为：版本号---计数，未对uuid去重)
		for (Entry<String, Integer> entry : uuidSummarys.entrySet()) {
			String version = entry.getKey();
			Integer aggregateValues = entry.getValue();
			final Query qu = Query.query(Criteria.where("version").is(version)
					.and("sn").is(date));
			final Update update = new Update();
			update.set(method, aggregateValues);
			//0:1分别表示 所有：对uuid去重后的数据
			update.set("type", 0);
			template138_analysis.upsert(qu, update,PLUGIN_VERSION_REPORT);		
		}
		
		//保存非法version及null version的统计结果入库
		bd.put("date",nullVersion);
		bd.put("null_version",nullVersion);
		bd.put("illegal_version",illegalVersion);
		template138_analysis.save(bd, collectionName1);
		getUniqueUuidCount(table,date);
		loger.info("---updateLogWithUniqueUuid cost:" + (System.currentTimeMillis()-mis));
	}
	
	private String toString(Object obj) {
		return obj == null ? null : obj.toString();
	}
	
	public static void main(String[] args){
		try {
			new MinuteGeneratorNew("20130426").getSummaryData("pluginLog_js", "2013-04-26");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
