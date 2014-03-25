package com.b5m.plugin.util;

import group.test.Bean;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.json.JSONArray;

public class Producer {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		final AtomicInteger counter = new AtomicInteger(0);
		MessageHelper helper= new MessageHelper("10.10.100.1:12181");
		Bean bean = new Bean();
		bean.setId(123);
		bean.setName("test");
		bean.setBd(new Date());
		List list = new ArrayList();
		list.add(new Date());
		bean.setList(list);
		String topic = "test_hotel_count";
		String msg = "{\"id\":\"15749\",\"source\":\"hotelweb1\"}";
		long mis = System.currentTimeMillis();
		for(int i = 0;i < 20;i++){
			msg = "{\"id\":\"15900a_" + i + "\",\"source\":\"hotelweb\"}";
			helper.sendMessage(topic,msg);
			counter.incrementAndGet();
		}
		
		for(int i = 0;i < 28;i++){
			msg = "{\"id\":\"16901b_" + i + "\",\"source\":\"hotelweb1\"}";
			helper.sendMessage(topic,msg);
			counter.incrementAndGet();
		}
		//helper.sendMessage(topic,msg);
		System.out.println("cost-->" + (System.currentTimeMillis() - mis) + "------>" + counter.get());
	}
}
