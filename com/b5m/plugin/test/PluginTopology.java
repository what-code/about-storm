package com.b5m.plugin.test;

import java.util.*;

import com.b5m.plugin.bolt.PluginCountBolt;
import com.b5m.plugin.spout.*;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.exception.MetaClientException;

import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class PluginTopology {
	public static TopologyBuilder build() throws MetaClientException {
		TopologyBuilder builder = new TopologyBuilder();
		final MetaClientConfig metaClientConfig = new MetaClientConfig();
        final ZKConfig zkConfig = new ZKConfig();
        //设置zookeeper地址
        zkConfig.zkConnect = "172.16.7.83:2181";
        metaClientConfig.setZkConfig(zkConfig);
        
        // publish topic
        final String topic = "test0";
        final String group = "group_gsj";
        ConsumerConfig cc = new ConsumerConfig(group);
        
        StringScheme ss = new StringScheme();
        MetaSpout ms = new MetaSpout(metaClientConfig,cc,ss);
        PluginSpout ps = new PluginSpout();
        
		//builder.setSpout("1",ps,5);
		//此bolt 使用10个线程
		//builder.setBolt("2", new PluginCountBolt(), 10).fieldsGrouping("1", new Fields("metaq_spout"));
		//builder.setBolt("2", new PluginCountBolt(), 10).shuffleGrouping("1");
		//此bolt 使用20个线程
		//builder.setBolt("3", new PluginMapBolt(), 20).allGrouping("1");
		return builder;
	}
	
	public static void main(String[] args) throws Exception {
		TopologyBuilder build = PluginTopology.build();
		Config conf = new Config();
		//topology 的工作进程数
		conf.setNumWorkers(20);
		conf.setMaxSpoutPending(5000);
		conf.put(MetaSpout.TOPIC, "test_topic");
		StormSubmitter.submitTopology("plugin_gsj", conf, build.createTopology());
	}

}
