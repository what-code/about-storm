package group.test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

public class ModStreamGrouping implements CustomStreamGrouping {
	
	private Map _map;
	private TopologyContext _ctx;
	private Fields _fields;
	private List<Integer> _targetTasks;
	
	public ModStreamGrouping(){
		
	}
	
	
	public void prepare(TopologyContext context, Fields outFields,
			List<Integer> targetTasks) {
		// TODO Auto-generated method stub
		_ctx = context;
		_fields = outFields;
		_targetTasks = targetTasks;
	}

	
	public List<Integer> chooseTasks(List<Object> values) {
		// TODO Auto-generated method stub
		Long groupingKey = Long.valueOf( values.get(0).toString());
		int index = (int) (groupingKey%(_targetTasks.size()));
		return Arrays.asList(_targetTasks.get(index));
	}

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> targetTasks) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		// TODO Auto-generated method stub
		return null;
	}

}
