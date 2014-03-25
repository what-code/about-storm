/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Authors:
 *   wuhua <wq163@163.com> , boyan <killme2008@gmail.com>
 */
package com.b5m.plugin.spout;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.b5m.plugin.util.MessageConfig;
import com.b5m.plugin.util.MessageHelper;
import com.b5m.plugin.util.MessageListenerTemplate;
import com.taobao.gecko.core.util.LinkedTransferQueue;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;



/**
 * ֧支持metamorphosis消息消费的storm spout
 * 
 * @author boyan(boyan@taobao.com)
 * @date 2011-11-8
 * 
 */
public class MetaTestSpout implements IRichSpout {
    private static final long serialVersionUID = 4382748324382L;
    private String url;
    private String topic;
    private String group;
    private transient SpoutOutputCollector collector;
    private ConcurrentLinkedQueue clq = new ConcurrentLinkedQueue();
    
    public MetaTestSpout(String url,String topic,String group) {
        this.url = url;
        this.topic = topic;
        this.group = group;
        MessageHelper helper = new MessageHelper(this.url);
		final AtomicInteger counter = new AtomicInteger(0);
		helper.Subscription(topic,group,new MessageListenerTemplate<Object>() {
			@Override
			public void recieveData(Message Message, Object data) {
				counter.incrementAndGet();
				clq.add(data);
				System.out.println("--->" + data);
			}
		});
    }


    public void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void nextTuple() {
    	Object obj = clq.poll();
    	if(obj != null){
    		this.collector.emit("meta_test_spout", new Values(obj));
    	}
    }
   

    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("meta_test_id"));
    }


    public boolean isDistributed() {
        return true;
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
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

}