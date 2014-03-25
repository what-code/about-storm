package com.b5m.plugin.util;

import group.test.Bean;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import com.taobao.metamorphosis.Message;


public class AsyncConsumer {
	public static void main(String[] args) throws Exception {
		MessageHelper helper = new MessageHelper("10.10.100.1:12181");
		final AtomicInteger counter = new AtomicInteger(0);
		String topic = "test_hotel_count";
		String group = "group_g0sj001";
		helper.Subscription(topic,group,new MessageListenerTemplate<Object>() {
			@Override
			public void recieveData(Message Message, Object data) {
				counter.incrementAndGet();
				//Message.getData();
				System.out.println("--->" + data);
			}
		});
		new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(5000);
                        System.out.println("--->" + counter.get());
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();
	}
}
