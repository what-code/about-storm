package com.b5m.plugin.util;

import com.taobao.metamorphosis.Message;

public interface IB5mMessageListener<T extends Object> {
	public void recieveMessages(Message message,T data);
}
