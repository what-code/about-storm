package com.b5m.raindrop.test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/*import com.b5m.raindrop.tao.counter.refresh.TaoAdsDBRefresh;
import com.b5m.raindrop.tao.counter.refresh.TaoGoodsDBRefresh;
import com.b5m.raindrop.tao.db.IDaoFactory;
import com.b5m.raindrop.tao.db.template.JdbcConfig;
import com.b5m.raindrop.tao.db.template.TemplateDaoFactory;*/

/**
 * 
 * @author jacky
 *
 */
public class CounterBolt extends BaseRichBolt {
	OutputCollector _collector;

	/**
	 * 
	 */
	private static final long serialVersionUID = 473666731117613272L;
	
	/**
	 * 商品统计缓冲
	 */
	private final Map<String, Long> goodsCounters = Collections.synchronizedMap(new HashMap<String, Long>());
	
	/**
	 * 广告统计缓冲
	 */
	private final Map<String, Long> adsCounters = Collections.synchronizedMap(new HashMap<String, Long>());
	
	/*private IDaoFactory daoFactory;
	
	private IRefreshHandler _goodsHandler;

	private IRefreshHandler _adsHandler;
	
	private transient Logger log;
	
	private JdbcConfig jdbcConfig;
	
	public CounterBolt(JdbcConfig jdbcConfig){
		this.jdbcConfig = jdbcConfig;
	}*/
	
	/**
	 * 
	 * @deprecated 目前还保留着{@link CounterBean#getGoodsId()}，后期将会去掉
	 * @param inputBean
	 * @return
	 */
	private String getId(CounterBean inputBean){
		return inputBean.getGoodsId()==null?inputBean.getId():inputBean.getGoodsId();
	}
	
	private void count(Map<String, Long> counters, CounterBean inputBean){
		long count = 0;
		String id = getId(inputBean);
		if(counters.containsKey(id)){
			count = counters.get(id);
		}
		count++;
		counters.put(id, count);
		/*if(log.isDebugEnabled()){
			log.debug(new StringBuilder("count(").append(id)
					.append(") >> ").append(count).toString());
		}*/
	}
	
	private void refresh(Tuple input){
		//this._goodsHandler.refresh(goodsCounters, input);
		//this._adsHandler.refresh(adsCounters, input);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;
		/*daoFactory = new TemplateDaoFactory(this.jdbcConfig);
		_goodsHandler = new TaoGoodsDBRefresh(daoFactory);
		_adsHandler = new TaoAdsDBRefresh(daoFactory);
		log = Logger.getLogger(this.getClass());*/
	}

	@Override
	public void execute(Tuple input) {
		if("signals".equals(input.getSourceStreamId())){
			System.out.println("====goodsCounters----------->" + goodsCounters);
			return ;
		}
		CounterBean counter = (CounterBean)input.getValue(0);
		// 通过category属性，来决定使用哪一个计数器进行计数
		if(counter.getCategory() == CounterCategory.Goods){
			count(goodsCounters, counter);
		}else{
			count(adsCounters, counter);
		}
		//_collector.emit(new Values(goods.getGoodsId(), count));
		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("goodsid", "count"));
	}
	
	@Override
    public void cleanup() {
		/*if(null == _goodsHandler || !(_goodsHandler instanceof Closeable))
			return ;
		try {
			((Closeable)_goodsHandler).close();
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}*/
    } 

}
