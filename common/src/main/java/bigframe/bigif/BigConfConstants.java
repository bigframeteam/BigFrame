package bigframe.bigif;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import bigframe.util.Constants;

/**
 * A class contain some constants used for identifying configuration properties.
 * 
 * @author andy
 *
 */
public class BigConfConstants {

	// Properties not related to data and query
	public static final String BIGFRAME_CONF_DIR = "bigframe.conf.dir";
	
	// Properties for the home dir of each engine
	/**
	 * Hadoop specific
	 */
	public static final String BIGFRAME_HADOOP_HOME = "bigframe.hadoop.home";
	public static final String BIGFRAME_HDFS_ROOTDIR = "bigframe.hdfs.root.dir";
	public static final String BIGFRAME_HADOOP_SLAVE = "bigframe.hadoop.slaves";
	
	/**
	 * Hive specific
	 */
	public static final String BIGFRAME_HIVE_HOME = "bigframe.hive.home";
	public static final String BIGFRAME_HIVE_JDBC_SERVER = "bigframe.hive.jdbc.server";
	
	/**
	 * Shark specific
	 */
	public static final String BIGFRAME_SHARK_HOME = "bigframe.shark.home";
	
	/**
	 * Spark specific
	 */
	public static final String BIGFRAME_SPARK_HOME = "bigframe.spark.home";
	public static final String BIGFRAME_SPARK_MASTER = "bigframe.spark.master";
	
	
	/**
	 * Vertica specific
	 */
	public static final String BIGFRAME_VERTICA_HOSTNAMES = "bigframe.vertica.hostnames";
	public static final String BIGFRAME_VERTICA_PORT = "bigframe.vertica.port";
	public static final String BIGFRAME_VERTICA_USERNAME = "bigframe.vertica.username";
	public static final String BIGFRAME_VERTICA_PASSWORD = "bigframe.vertica.password";
	public static final String BIGFRAME_VERTICA_DATABASE = "bigframe.vertica.database";
	public static final String BIGFRAME_VERTICA_JDBC_SERVER = "bigframe.vertica.jdbc.server";
	public static final String BIGFRAME_VERTICA_HOME = "bigframe.vertica.home";
	
	/**
	 * Hana specific
	 */
	public static final String BIGFRAME_HANA_HOSTNAMES = "bigframe.hana.hostnames";
	public static final String BIGFRAME_HANA_PORT = "bigframe.hana.port";
	public static final String BIGFRAME_HANA_USERNAME = "bigframe.hana.username";
	public static final String BIGFRAME_HANA_PASSWORD = "bigframe.hana.password";
	public static final String BIGFRAME_HANA_DATABASE = "bigframe.hana.database";
	public static final String BIGFRAME_HANA_JDBC_SERVER = "bigframe.hana.jdbc.server";
	public static final String BIGFRAME_HANA_HOME = "bigframe.hana.home";

	// Properties related to data refreshing
	public static final String BIGFRAME_REFRESH_LOCAL = "bigframe.refresh.local";
	
	
	// Properties for external scripts
	public static final String BIGFRAME_TPCDS_LOCAL = "bigframe.tpcds.local";
	public static final String BIGFRAME_TPCDS_SCRIPT = "bigframe.tpcds.script";
	public static final String BIGFRAME_GEN_SINGLETBL_SCRIPT = "bigframe.singletblgen.script";


	// BigFrame input format related properties
	public static final String BIGFRAME_APP_DOMAIN = "bigframe.application.domain";
	public static final String APPLICATION_BI = "BI";
	
	public static final String BIGFRAME_DATAVARIETY = "bigframe.datavariety";
	public static final String BIGFRAME_DATAVOLUME = "bigframe.datavolume";

	public static final String BIGFRAME_DATAVOLUME_GRAPH_PROPORTION = "bigframe.datavolume.graph.proportion";
	public static final String BIGFRAME_DATAVOLUME_RELATIONAL_PROPORTION = "bigframe.datavolume.relational.proportion";
	public static final String BIGFRAME_DATAVOLUME_NESTED_PROPORTION = "bigframe.datavolume.nested.proportion";
	public static final String BIGFRAME_DATAVOLUME_TEXT_PROPORTION = "bigframe.datavolume.text.proportion";
	static final String[] DATAVOLUME_PROPORTION = new String[] {
		BIGFRAME_DATAVOLUME_GRAPH_PROPORTION,
		BIGFRAME_DATAVOLUME_RELATIONAL_PROPORTION,
		BIGFRAME_DATAVOLUME_NESTED_PROPORTION,
		BIGFRAME_DATAVOLUME_TEXT_PROPORTION };
	public static final Set<String> BIGFRAME_DATAVOLUME_PORTION_SET = new HashSet<String>(
			Arrays.asList(DATAVOLUME_PROPORTION));

	public static final String BIGFRAME_DATAVELOCITY_RELATIONAL = "bigframe.datavelocity.relational";
	public static final String BIGFRAME_DATAVELOCITY_GRAPH = "bigframe.datavelocity.graph";
	public static final String BIGFRAME_DATAVELOCITY_NESTED = "bigframe.datavelocity.nested";
	public static final String BIGFRAME_DATAVELOCITY_TEXT = "bigframe.datavelocity.text";
	static final String[] DATAVELOCITY = new String[] {
		BIGFRAME_DATAVELOCITY_RELATIONAL, BIGFRAME_DATAVELOCITY_GRAPH,
		BIGFRAME_DATAVELOCITY_NESTED, BIGFRAME_DATAVELOCITY_TEXT };
	public static final Set<String> BIGFRAME_DATAVELOCITY = new HashSet<String>(
			Arrays.asList(DATAVELOCITY));

	public static final String BIGFRAME_DATA_HDFSPATH_RELATIONAL = "bigframe.data.hdfspath.relational";
	public static final String BIGFRAME_DATA_HDFSPATH_GRAPH = "bigframe.data.hdfspath.graph";
	public static final String BIGFRAME_DATA_HDFSPATH_NESTED = "bigframe.data.hdfspath.nested";
	public static final String BIGFRAME_DATA_HDFSPATH_TEXT = "bigframe.data.hdfspath.text";
	static final String[] DATA_HDFSPATH = new String[] {
		BIGFRAME_DATA_HDFSPATH_RELATIONAL, BIGFRAME_DATA_HDFSPATH_GRAPH,
		BIGFRAME_DATA_HDFSPATH_NESTED, BIGFRAME_DATA_HDFSPATH_TEXT };
	public static final Set<String> BIGFRAME_DATA_HDFSPATH = new HashSet<String>(
			Arrays.asList(DATA_HDFSPATH));

	public static final String BIGFRAME_QUERYVARIETY = "bigframe.queryvariety";
	public static final String BIGFRAME_QUERYVELOCITY = "bigframe.queryvelocity";
	public static final String BIGFRAME_QUERYVOLUME = "bigframe.queryvolume";

	public static final String BIGFRAME_QUERYENGINE_RELATIONAL = "bigframe.queryengine.relational";
	public static final String BIGFRAME_QUERYENGINE_GRAPH = "bigframe.queryengine.graph";
	public static final String BIGFRAME_QUERYENGINE_NESTED = "bigframe.queryengine.nested";
	public static final String BIGFRAME_QUERYENGINE_TEXT = "bigframe.queryengine.text";
	
	// Constants for Configuration parameters
	static final String[] DATATYPES = new String[] { Constants.RELATIONAL, Constants.GRAPH,
		Constants.NESTED, Constants.TEXT };
	public static final Set<String> DATAVARIETY = new HashSet<String>(
			Arrays.asList(DATATYPES));

	static final String[] QUERYTYPES = new String[] { Constants.EXPLORATORY, Constants.CONTINUOUS };
	public static final Set<String> QUERYVELOCITY = new HashSet<String>(
			Arrays.asList(QUERYTYPES));

	public static final Map<String, Integer> DATAVOLUME_MAP;
	static {
		DATAVOLUME_MAP = new HashMap<String, Integer>();
		DATAVOLUME_MAP.put("test", 1);
		DATAVOLUME_MAP.put("toy", 3);
		DATAVOLUME_MAP.put("tiny", 10);
		DATAVOLUME_MAP.put("small", 100);
		DATAVOLUME_MAP.put("medium", 1000);
		DATAVOLUME_MAP.put("large", 10000);
		DATAVOLUME_MAP.put("extra large", 100000);
	}
}
