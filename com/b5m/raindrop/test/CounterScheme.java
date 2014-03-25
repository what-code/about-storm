package com.b5m.raindrop.test;

import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 实现了Storm的{@link Scheme}接口，此
 * @author jacky
 *
 */
public class CounterScheme implements Scheme {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3841835168599409648L;

	@Override
	public List<Object> deserialize(byte[] ser) {
		try {
            return new Values(BeanJsonConvert.toCounterBean((new String(ser, "UTF-8"))));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } 
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("goodsid");
	}
}
