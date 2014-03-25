package com.b5m.plugin.spout;

import backtype.storm.spout.Scheme;

import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;


public class PluginSpoutNew extends MetaSpout {
	private static final long serialVersionUID = 4382748324390L;
	public PluginSpoutNew(MetaClientConfig metaClientConfig,
			ConsumerConfig consumerConfig, Scheme scheme) {
		super(metaClientConfig, consumerConfig, scheme);
		// TODO Auto-generated constructor stub
	}
}
