package com.b5m.plugin.util;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

/**
 * 
 * @author xiao.zhao
 * 
 */
public class MessageConfig {
	/**
	 * 消息服务器地址
	 */
	private String serverUrl;
	/**
	 * 消费者
	 */
	private ConcurrentHashMap<String, MessageConsumer> groupMessageConsumer = new ConcurrentHashMap<String, MessageConsumer>();
	/**
	 * 会话工厂
	 */
	private MessageSessionFactory sessionFactory;
	/**
	 * 生产者
	 */
	private MessageProducer producer;

	public MessageConfig(String serverUrl) {
		this.serverUrl = serverUrl;
	}

	public MessageSessionFactory getMessageSessionFactory() throws Exception {
		return this.getMessageSessionFactory(serverUrl);
	}

	public MessageSessionFactory getMessageSessionFactory(String url)
			throws Exception {
		if (sessionFactory != null) {
			return sessionFactory;
		}
		if (StringUtils.isBlank(url)) {
			return null;
		}
		final MetaClientConfig metaClientConfig = new MetaClientConfig();

		final ZKConfig zkConfig = new ZKConfig();

		zkConfig.zkConnect = serverUrl;

		metaClientConfig.setZkConfig(zkConfig);

		sessionFactory = new MetaMessageSessionFactory(metaClientConfig);

		return sessionFactory;

	}

	public MessageConsumer getMessageConsumer(String group) {
		if (StringUtils.isBlank(group)) {
			return null;
		}
		MessageConsumer consumer = groupMessageConsumer.get(group);
		if (consumer != null) {
			return consumer;
		}
		try {
			sessionFactory = this.getMessageSessionFactory();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		consumer = sessionFactory.createConsumer(new ConsumerConfig(group));
		return consumer;
	}

	public MessageProducer getMessageProducer() throws Exception {
		if (producer != null) {
			return producer;
		}
		sessionFactory = this.getMessageSessionFactory();
		if (sessionFactory == null) {
			return null;
		}
		producer = sessionFactory.createProducer();

		return producer;

	}
}
