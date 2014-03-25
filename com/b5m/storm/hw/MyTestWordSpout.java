package com.b5m.storm.hw;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Title:MyTestWordSpout.java
 * 
 * Description:MyTestWordSpout.java
 * 
 * Copyright: Copyright (c) 2014-3-25
 * 
 * Company: IZENE Software(Shanghai) Co., Ltd.
 * 
 * @author Shengjie Guo
 * 
 * @version 1.0
 */
public class MyTestWordSpout extends BaseRichSpout {
    public static Logger LOG = Logger.getLogger(TestWordSpout.class);
    private final AtomicInteger count = new AtomicInteger(0);
    boolean _isDistributed;
    SpoutOutputCollector _collector;

    public MyTestWordSpout() {
        this(true);
    }

    public MyTestWordSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }
        
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }
    
    public void close() {
        
    }
        
    public void nextTuple() {
    	int sum = count.incrementAndGet();
    	if(sum <= 20){
	        Utils.sleep(1000);
	        final String[] words = new String[] {"nathan", "mike", "jackson", "golda", "bertels"};
	        final Random rand = new Random();
	        final String word = words[rand.nextInt(words.length)];
	        _collector.emit(new Values(word));
	        System.out.println("----------------########send????????????--->" + word + "---count--->" + sum);
    	}
    }
    
    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {
        
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }    
}
