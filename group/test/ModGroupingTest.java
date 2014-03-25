package group.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class ModGroupingTest {
	
	public static void main(String args[]){
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("uid", new TestUidSpout());
		//builder.setBolt("process", new ModGroupBolt(), 10).customGrouping("uid", new ModStreamGrouping());
		builder.setBolt("process", new ModGroupBolt(), 10).fieldsGrouping("uid", new Fields("uid"));
		
		Config config = new Config();
		config.setDebug(true);
		
		config.setNumWorkers(3);
		LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, builder.createTopology());
        Utils.sleep(5000);
        cluster.killTopology("test");
        cluster.shutdown();    
	}
}
