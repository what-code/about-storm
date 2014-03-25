package com.b5m.plugin.util;

import org.apache.commons.lang.StringUtils;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.exception.MetaClientException;

public class MessageHelper {
	
	private MessageConfig config;
	
	public MessageHelper(String server){
		this.config = new MessageConfig(server);
	}

	public <T extends Object> void sendMessage(final String topic, final T msg)
			throws Exception {

		if (StringUtils.isBlank(topic) || null == msg) {
			return;
		}
		final MessageProducer producer = config.getMessageProducer();

		producer.publish(topic);

		MessageThreadPool.instace.pool.execute(new Runnable() {
			@Override
			public void run() {
				try {
					String data = SerializerUtils.Serialization(msg);
					producer.sendMessage(new Message(topic, data.getBytes()));
				} catch (MetaClientException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		});
	}

	public <T extends Object> void Subscription(String topic,String group,
			MessageListenerTemplate<T> listener) {
		MessageConsumer consumer = config.getMessageConsumer(group);
		// subscribe topic
		try {
			consumer.subscribe(topic, 1024 * 1024, listener);
			// complete subscribe
			consumer.completeSubscribe();
		} catch (MetaClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
