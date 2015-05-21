package bigframe.bigif;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import bigframe.util.Config;

/**
 * A class to record all the information related to workflow drivers.
 * @author andy
 *
 */
public class WorkflowInputFormat extends Config {
	
	private String HADOOP_HOME = "";
	private String HDFS_ROOT_DIR = "";
	private String WEBHDFS_ROOT_DIR = "";
	private String HADOOP_USERNAME = "";
	private String HADOOP_CONF_DIR="";
	
	private String HIVE_HOME = "";
	private String HIVE_JDBC_SERVER = "";
	private boolean HIVE_ORC = true;
	
	private String SHARK_HOME = "";
	private String SPARK_HOME = "";
	private String SPARK_MASTER = "";
	private String SPARK_LOCAL_DIR = "";
	private Boolean SPARK_USE_BAGEL = true;
	private Integer SPARK_DOP = 8;
	private Boolean SPARK_COMPRESS_MEMORY = false;
	private Float SPARK_MEMORY_FRACTION = 0.66f;
	private Boolean SPARK_OPTIMIZE_MEMORY = true;
	
	private String VERTICA_HOSTNAMES = "";
	private String VERTICA_DATABASE = "";
	private Integer VERTICA_PORT = 0;
	private String VERTICA_USERNAME = "";
	private String VERTICA_PASSWORD = "";
	private String VERTICA_JDBC_SERVER = "";
	private String VERTICA_HOME = "";
	
	private String KAFKA_HOME = "";
	private String KAFKA_BROKER_LIST = "";
	
	private String ZOOKEEPER_CONNECT = ""; 

	// thoth related parameters
	private boolean ADD_LISTENER = false;
	private String METADATA_DB_NAME = "";
	private String METADATA_DB_CONNECTION = "";
	private String METADATA_DB_USERNAME = "";
	private String METADATA_DB_PASSWORD = "";
	
	public WorkflowInputFormat() {
	}
	
	public String getHadoopHome() {
		return HADOOP_HOME;
	}

	public String getHadoopConf() {
		return HADOOP_CONF_DIR;
	}
	
	public String getHadoopUserName() {
		return HADOOP_USERNAME;
	}
	
	
	
	public String getWEBHDFSRootDIR() {
		return WEBHDFS_ROOT_DIR;
	}
	
	public String getHDFSRootDIR() {
		return HDFS_ROOT_DIR;
	}
	
	public String getHiveHome() {
		return HIVE_HOME;
	}
	
	public String getHiveJDBCServer() {
		return HIVE_JDBC_SERVER;
	}
	
	public boolean getHiveORC() {
		return HIVE_ORC;
	}
	
	public String getSharkHome() {
		return SHARK_HOME;
	}
	
	public String getSparkHome() {
		return SPARK_HOME;
	}
	
	public String getSparkMaster() {
		return SPARK_MASTER;
	}
	
	public String getSparkLocalDir() {
		return SPARK_LOCAL_DIR;
	}

	public Boolean getSparkUseBagel() {
		return SPARK_USE_BAGEL;
	}

	public Integer getSparkDoP() {
		return SPARK_DOP;
	}

	public Boolean getSparkCompressMemory() {
		return SPARK_COMPRESS_MEMORY;
	}

	public Float getSparkMemoryFraction() {
		return SPARK_MEMORY_FRACTION;
	}

	public Boolean getSparkOptimizeMemory() {
		return SPARK_OPTIMIZE_MEMORY;
	}

	public String getVerticaHostNames() {
		return VERTICA_HOSTNAMES;
	}
	
	public String getVerticaUserName() {
		return VERTICA_USERNAME;
	}
	
	public String getVerticaPassword() {
		return VERTICA_PASSWORD;
	}
	
	public String getVerticaDatabase() {
		return VERTICA_DATABASE;
	}
	
	public Integer getVerticaPort() {
		return VERTICA_PORT;
	}
	
	public String getVerticaJDBCServer() {
		return VERTICA_JDBC_SERVER;
	}
	
	public String getVerticaHome(){
		return VERTICA_HOME;
	}
	
	
	public String getKafkaHome() {
		return KAFKA_HOME;
	}
	
	public String getKafakaBrokers() {
		return KAFKA_BROKER_LIST;
	}
	
	public String getZooKeeperConnect() {
		return ZOOKEEPER_CONNECT;
	}
	
	public Boolean getAddListener() {
		return ADD_LISTENER;
	}

	public String getMetadataDBName() {
		return METADATA_DB_NAME;
	}
	
	public String getMetadataDBConnection() {
		return METADATA_DB_CONNECTION;
	}
	
	public String getMetadataDBUsername() {
		return METADATA_DB_USERNAME;
	}
	
	public String getMetadataDBPassword() {
		return METADATA_DB_PASSWORD;
	}
	
	public void reload() {

	}

	@Override
	protected void reloadConf() {
		for (Map.Entry<String, String> entry : properties.entrySet()) {
			String key = entry.getKey().trim();
			String value = entry.getValue().trim();
			
			if (key.equals(BigConfConstants.BIGFRAME_HADOOP_HOME)) {
				HADOOP_HOME = value;
			}
			else if (key.equals(BigConfConstants.BIGFRAME_HADOOP_CONF_DIR)){
				HADOOP_CONF_DIR = value;
			}
			else if (key.equals(BigConfConstants.BIGFRAME_WEBHDFS_ROOTDIR)) {
				WEBHDFS_ROOT_DIR = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_HADOOP_USERNAME)) {
				HADOOP_USERNAME = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_HDFS_ROOTDIR)) {
				HDFS_ROOT_DIR = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_HIVE_HOME)) {
				HIVE_HOME = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_HIVE_JDBC_SERVER)) {
				HIVE_JDBC_SERVER = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_HIVE_ORC)) {
				HIVE_ORC = Boolean.valueOf(value);
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_SHARK_HOME)) {
				SHARK_HOME = value;
			}
			
			
			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_HOME)) {
				SPARK_HOME = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_MASTER)) {
				SPARK_MASTER = value;
			}

			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_LOCAL_DIR)) {
				SPARK_LOCAL_DIR = value;
			}

			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_USE_BAGEL)) {
				SPARK_USE_BAGEL = Boolean.valueOf(value);
			}

			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_DOP)) {
				SPARK_DOP = Integer.parseInt(value);
			}

			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_COMPRESS_MEMORY)) {
				SPARK_COMPRESS_MEMORY = Boolean.valueOf(value);
			}

			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_MEMORY_FRACTION)) {
				SPARK_MEMORY_FRACTION = Float.parseFloat(value);
			}

			else if (key.equals(BigConfConstants.BIGFRAME_SPARK_OPTIMIZE_MEMORY)) {
				SPARK_OPTIMIZE_MEMORY = Boolean.valueOf(value);
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_VERTICA_DATABASE)) {
				VERTICA_DATABASE = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_VERTICA_HOSTNAMES)) {
				VERTICA_HOSTNAMES = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_VERTICA_USERNAME)) {
				VERTICA_USERNAME = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_VERTICA_PASSWORD)) {
				VERTICA_PASSWORD = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_VERTICA_PORT)) {
				if(!value.equals("")) {
					VERTICA_PORT = Integer.parseInt(value);
				}
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_VERTICA_JDBC_SERVER)) {
				VERTICA_JDBC_SERVER = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_VERTICA_HOME)) {
				VERTICA_HOME = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_KAFKA_HOME)) {
				KAFKA_HOME = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_KAFKA_BROKER_LIST)) {
				KAFKA_BROKER_LIST = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_ZOOKEEPER_CONNECT)) {
				ZOOKEEPER_CONNECT = value;
			}

			else if (key.equals(BigConfConstants.BIGFRAME_ADD_LISTENER)) {
				ADD_LISTENER = Boolean.valueOf(value);
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_METADATA_DB_NAME)) {
				METADATA_DB_NAME = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_METADATA_DB_CONNECTION)) {
				METADATA_DB_CONNECTION = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_METADATA_DB_USERNAME)) {
				METADATA_DB_USERNAME = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_METADATA_DB_PASSWORD)) {
				METADATA_DB_PASSWORD = value;
			}
		}
		
	}

	@Override
	public void printConf() {
		System.out.println("Workflow Driver configuration:");
		
		System.out.println("Hadoop Home:" + HADOOP_HOME);
		System.out.println("Hadoop Root Dir: " + HDFS_ROOT_DIR);
		System.out.println("Hadoop Username:" + HADOOP_USERNAME);
		System.out.println("Hadoop Webhdfs Root Dir: " + WEBHDFS_ROOT_DIR);
		
		System.out.println("Hive Home: " + HIVE_HOME);
		System.out.println("Hive JDBC Server: " + HIVE_JDBC_SERVER);
		
		System.out.println("Shark Home: " + SHARK_HOME);
		System.out.println("Spark Home: " + SPARK_HOME);
		System.out.println("Spark MASTER: " + SPARK_MASTER);
		System.out.println("Spark local dir: " + SPARK_LOCAL_DIR);
		System.out.println("Spark use bagel: " + SPARK_USE_BAGEL);
		System.out.println("Spark DoP: " + SPARK_DOP);
		System.out.println("Spark compress memory: " + SPARK_COMPRESS_MEMORY);
		System.out.println("Spark memory fraction: " + SPARK_MEMORY_FRACTION);
		System.out.println("Spark optimize memory: " + SPARK_OPTIMIZE_MEMORY);
		
		System.out.println("Vertica Host Names: " + VERTICA_HOSTNAMES);
		System.out.println("Vertica Database: " + VERTICA_DATABASE);
		System.out.println("Vertica User Name: " + VERTICA_USERNAME);
		System.out.println("Vertica Password: " + VERTICA_PASSWORD);
		System.out.println("Vertica Port: " + VERTICA_PORT);
		System.out.println("Vertica JDBC Server: " + VERTICA_JDBC_SERVER);
		
		System.out.println("Kafka Home: " + KAFKA_HOME);
		System.out.println("Kafka Broker List: " + KAFKA_BROKER_LIST);
		System.out.println("ZooKeeper Connect: " + ZOOKEEPER_CONNECT);
		
		System.out.println("Thoth add listener: " + ADD_LISTENER);
		System.out.println("Thoth DB name: " + METADATA_DB_NAME);
		System.out.println("Thoth DB connection: " + METADATA_DB_CONNECTION);
		System.out.println("Thoth DB username: " + METADATA_DB_USERNAME);
		System.out.println("Thoth DB password: " + METADATA_DB_PASSWORD);
	}
}
