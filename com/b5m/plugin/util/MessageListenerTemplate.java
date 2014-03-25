package com.b5m.plugin.util;

import java.util.concurrent.Executor;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.consumer.MessageListener;

public abstract class MessageListenerTemplate<T extends Object> implements
		MessageListener {
	public abstract void recieveData(Message Message, T data);

	public void recieveMessages(Message message) {
		String data = new String(message.getData());
		T objData = SerializerUtils.Deserialization(data);
		recieveData(message, objData);
	}

	public Executor getExecutor() {
		// Thread pool to process messages,maybe null.
		return null;
	}
}
