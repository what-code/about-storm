package group.test;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ModGroupBolt implements IBasicBolt {
    /**
	 * 
	 */
	private static final long serialVersionUID = 12342345L;
	OutputCollector _collector;
    String _ComponentId;
    int _TaskId;
    

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
    	_ComponentId = context.getThisComponentId();
    	_TaskId = context.getThisTaskId();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println("----execute---->"+_ComponentId+":"+_TaskId +"----recevie :" + input.getInteger(0));
    	_collector.emit(new Values(input));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("uid"));  
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}


}
