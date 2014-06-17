package bigframe.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import bigframe.bigif.BigConfConstants;

public class MapRedConfig {
	
	private static final Log LOG = LogFactory.getLog(MapRedConfig.class);

	public MapRedConfig() {
		// TODO Auto-generated constructor stub
	}

	public static Configuration getConfiguration(Config conf) {
		Configuration mapred_config = new Configuration();
		
		String core_site = "";
		String mapred_site = "";
		
//		String hadoop_dist = conf.getProp().get(
//				BigConfConstants.BIGFRAME_HADOOP_DISTRIBUTION);
//		
//		if(hadoop_dist == null) {
//			LOG.error("Please specify property bigframe.hadoop.distribution");
//			System.exit(-1);
//		}
		
//		if(hadoop_dist.equals(Constants.APACHE_HADOOP)) {
			core_site = conf.getProp().get(
					BigConfConstants.BIGFRAME_HADOOP_HOME)
					+ "/conf/core-site.xml";
			mapred_site = conf.getProp().get(
					BigConfConstants.BIGFRAME_HADOOP_HOME)
					+ "/conf/mapred-site.xml";
//		}
		
//		else if(hadoop_dist.equals(Constants.CLOUDERA_HADOOP)) {
//			core_site = conf.getProp().get(
//					BigConfConstants.BIGFRAME_HADOOP_HOME)
//					+ "/conf/core-site.xml";
//			mapred_site = conf.getProp().get(
//					BigConfConstants.BIGFRAME_HADOOP_HOME)
//					+ "/conf/mapred-site.xml";
//		}
//		
//		else {
//			LOG.error("Unsupported Hadoop Distribution:" + hadoop_dist);
//			System.exit(-1);
//		}
		
		mapred_config.addResource(new Path(core_site));
		mapred_config.addResource(new Path(mapred_site));
		
		return mapred_config;
	}
	
}
