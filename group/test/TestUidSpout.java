package group.test;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public  class TestUidSpout extends BaseRichSpout implements Serializable{
    boolean _isDistributed;
    SpoutOutputCollector _collector;
        
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }
    
    public void close() {
        
    }
        
    public void nextTuple() {
        Utils.sleep(100);
        final Random rand = new Random();
        final int uid =rand.nextInt(10000);
        _collector.emit(new Values(uid));
        System.out.println("----nextTuple----->"+uid);
    }
    
    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {
        
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("uid"));
    }


}