package com.b5m.raindrop.test;

import java.io.IOException;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.b5m.plugin.spout.MetaSpout;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

/**
 * {@link IRefreshHandler}使用的实现是{@link TaoGoodsDBRefresh}
 * 
 * @author jacky
 * 
 */
public class CounterTopology {

	public static final int COUNT_META_SPOUT = 1;

	public static final int COUNT_SIGNAL_SPOUT = 1;

	public static final int COUNT_COUNT_BOLT = 8;

	public static final String PRODUCT_PROPS_PATH = "/com/b5m/raindrop/tao/counter/config.properties";

	public static final String TEST_PROPS_PATH = "/com/b5m/raindrop/tao/counter/testconfig.properties";

	private static MetaClientConfig createMetaConfig(String zkConnect) {
		MetaClientConfig metaClientConfig = new MetaClientConfig();
		ZKConfig zkConfig = new ZKConfig();
		zkConfig.zkConnect = zkConnect;
		metaClientConfig.setZkConfig(zkConfig);
		return metaClientConfig;
	}

	private static com.b5m.plugin.spout.MetaSpout createMetaSpout(String zkConnect,
			String metaGroup, Integer fetchRunnerNum) {
		ConsumerConfig consumerConfig = new ConsumerConfig(metaGroup);
		if (null != fetchRunnerNum) {
			consumerConfig.setFetchRunnerCount(fetchRunnerNum);
		}
		MetaSpout spout = new MetaSpout(createMetaConfig(zkConnect),
				consumerConfig, new CounterScheme());
		return spout;
	}

	private static void startLocalMode(Properties props) {
		TopologyBuilder builder = new TopologyBuilder();
		final String zkConnect = "localhost:2181";
		final String metaGroup = props.getProperty("meta.group", "meta-test");
		MetaSpout metaSpout = createMetaSpout(
				zkConnect,
				metaGroup,
				props.containsKey("meta.fetch.runner_num") ? Integer
						.parseInt(props.getProperty("meta.fetch.runner_num",
								"4")) : null);
		builder.setSpout("goodsid", metaSpout, COUNT_META_SPOUT);
		builder.setSpout("signal", new SignalSpout(), COUNT_SIGNAL_SPOUT);

		builder.setBolt("counter", new CounterBolt(),
				COUNT_COUNT_BOLT)
				.fieldsGrouping("goodsid", new Fields("goodsid"))
				.allGrouping("signal", "signals");

		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		conf.put(MetaSpout.TOPIC, props.getProperty("meta.topic", "spout-test"));
		// conf.put(MetaSpout.KEY_PERIMITS_NUMBER,
		// props.getProperty("meta.permits.number", "1000"));
		conf.setDebug(true);
		cluster.submitTopology(
				props.getProperty("localcluster.topology.name", "test"), conf,
				builder.createTopology());
	}

	private static Properties loadProperties(String propsPath)
			throws IOException {
		Properties props = new Properties();
		props.load(CounterTopology.class.getResourceAsStream(propsPath));
		return props;
	}

	/*private static JdbcConfig createJdbcConfig(Properties props) {
		JdbcConfig config = new JdbcConfig();
		config.setDriverClass(props.getProperty("tao.jdbc.driverClass"));
		config.setUrl(props.getProperty("tao.jdbc.url"));
		config.setUser(props.getProperty("tao.jdbc.username"));
		config.setPassword(props.getProperty("tao.jdbc.password"));
		return config;
	}*/

	private static void startSubmitMode(String[] args, Properties props)
			throws AlreadyAliveException, InvalidTopologyException {
		ArgsExtractor extractor = new ArgsExtractor(args, props);
		TopologyBuilder builder = new TopologyBuilder();
		final String zkConnect = extractor.metazkurl;
		final String metaGroup = props.getProperty("meta.group", "meta-test");
		MetaSpout metaSpout = createMetaSpout(
				zkConnect,
				metaGroup,
				props.containsKey("meta.fetch.runner_num") ? Integer
						.parseInt(props.getProperty("meta.fetch.runner_num",
								"4")) : null);
		builder.setSpout("goodsid", metaSpout, COUNT_META_SPOUT);
		builder.setSpout("signal", new SignalSpout(), COUNT_SIGNAL_SPOUT);

		builder.setBolt("counter", new CounterBolt(),
				COUNT_COUNT_BOLT)
				.fieldsGrouping("goodsid", new Fields("goodsid"))
				.allGrouping("signal", "signals");

		Config conf = new Config();
		conf.put(MetaSpout.TOPIC, props.getProperty("meta.topic", "spout-test"));
		// conf.put(MetaSpout.KEY_PERIMITS_NUMBER,
		// props.getProperty("meta.permits.number", "1000"));
		if (null != extractor.interval) {
			conf.put(SignalSpout.CONF_INTERVAL_ID,
					String.valueOf(extractor.interval));
		}
		StormSubmitter.submitTopology(extractor.topologyName, conf,
				builder.createTopology());
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws InvalidTopologyException
	 * @throws AlreadyAliveException
	 */
	public static void main(String[] args) throws IOException,
			AlreadyAliveException, InvalidTopologyException {
		Properties props = loadProperties(isEnableTest(args) ? TEST_PROPS_PATH
				: PRODUCT_PROPS_PATH);
		// 配置log4j路径
		// org.apache.log4j.PropertyConfigurator.configure(CounterTopology.class.getResource("/com/b5m/raindrop/tao/counter/log4j.properties"));
		/*
		 * if(args.length == 0){ startLocalMode(props); Utils.sleep(100000);
		 * return ; }
		 */

		startSubmitMode(args, props);
	}

	private static boolean isEnableTest(String[] args) {
		for (String arg : args) {
			if (arg.equals("-test")) {
				return true;
			}
		}
		return false;
	}

	static class ArgsExtractor {

		private String topologyName;

		private Long interval;

		private String metazkurl;

		public ArgsExtractor(String[] args, Properties props) {
			for (String arg : args) {
				if (arg.indexOf("--tname") == 0) {
					String value = getValueFromArg(arg);
					if (null == value) {
						value = props.getProperty("cluster.topology.name");
					}
					topologyName = value;
					continue;
				}

				if (arg.indexOf("--interval") == 0) {
					String value = getValueFromArg(arg);
					if (null != value) {
						interval = Long.parseLong(value);
					}
					continue;
				}

				if (arg.indexOf("--metazkurl") == 0) {
					String value = getValueFromArg(arg);
					if (null == value) {
						value = props.getProperty("meta.zk.defaulturl",
								"locahost:2181");
					}
					metazkurl = value;
					continue;
				}
			}

			if (null == topologyName)
				topologyName = props.getProperty("cluster.topology.name");
			if (null == metazkurl)
				metazkurl = props.getProperty("meta.zk.defaulturl",
						"locahost:2181");
		}

		private String getValueFromArg(String arg) {
			int equalIndex = arg.indexOf("=");
			if (equalIndex == -1)
				return null;
			return arg.substring(equalIndex + 1);
		}
	}
}
