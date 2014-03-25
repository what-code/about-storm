package com.b5m.plugin.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.b5m.plugin.util.MessageHelper;
import com.b5m.plugin.util.MessageListenerTemplate;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class PluginSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	public static Logger log = Logger.getLogger(PluginSpout.class);
	private MessageConsumer consumer;
	private MessageSessionFactory sessionFactory;
	final MetaClientConfig metaClientConfig = new MetaClientConfig();
	private String MESSAGE = "";
	int count = 0,count0 = 0;
	List bufferList = new Vector();
	final String topic = "test0";
	final String group = "group_gsj";
	final AtomicInteger counter1 = new AtomicInteger(0);
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		final ZKConfig zkConfig = new ZKConfig();
		// 设置zookeeper地址
		zkConfig.zkConnect = "10.10.99.167:2181";
		metaClientConfig.setZkConfig(zkConfig);
		// New session factory,强烈建议使用单例
		try {
			sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
		} catch (MetaClientException e1) {
			e1.printStackTrace();
		}
		// create consumer,强烈建议使用单例
		consumer = sessionFactory.createConsumer(new ConsumerConfig(group));
		try {
			getMsgFromMetaQ(topic, 1024 * 1024);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void close() {

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
		if (bufferList.size() > 0) {
				String temp = (String) bufferList.get(0);
				List<Object> tuple = new ArrayList<Object>();
				//tuple 里面的值对应fields中的2个属性
				tuple.add(temp);
				tuple.add(System.currentTimeMillis());
				tuple.add("123");
				List list = collector.emit(tuple, "meta_tuple");
				bufferList.remove(temp);
				counter1.incrementAndGet();
			//log.info("---next_tuple_temp02--->" + count);
		} else {
			try {
				Utils.sleep(1000);
			} catch (Exception e) {
				log.info("---next_tuple_null_sleep_fail---");
			}
			//log.info("---next_tuple_null---");
		}
	}

	@Override
	public void ack(Object msgId) {
		//log.info("---ack00---" + msgId);
	}

	@Override
	public void fail(Object msgId) {
		//log.info("---fail00---" + msgId);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("metaq_spout","mis","test"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	// 获取数据
	public void getMsgFromMetaQ(String topic, int size) throws Exception {
		String group = "group_gsj";
		// subscribe topic
		final AtomicInteger counter = new AtomicInteger(0);
		/*consumer.subscribe(topic, size,new MessageListener() {
			public void recieveMessages(Message message) {
				counter.incrementAndGet();
				MESSAGE = new String(message.getData());
				bufferList.add(MESSAGE);
				//log.info("Receive message:" + MESSAGE + "-->" + message.getPartition() + ":" + count0);
			}

			public Executor getExecutor() {
				return null;
			}
		});
		consumer.completeSubscribe();*/
		MessageHelper helper=new MessageHelper("10.10.99.167:2181");
		helper.Subscription(topic,group, new MessageListenerTemplate<Object>() {
			@Override
			public void recieveData(Message message, Object data) {
				counter.incrementAndGet();
				MESSAGE = data.toString();
				bufferList.add(MESSAGE);
				log.info("Receive message:" + MESSAGE + "-->" + message.getPartition() + ":" + count0);
			}
		});
		new Thread() {
			@Override
			public void run() {
				while (true) {
					try {
						//Thread.sleep(2000);
						//System.out.println("spout--->" + counter.get() + "---"  + counter1.get() + "---" + bufferList.size());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}.start();
	}
}
