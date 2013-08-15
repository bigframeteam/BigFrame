package bigframe.bigif;

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
	
	private String HIVE_HOME = "";
	private String HIVE_JDBC_SERVER = "";
	
	private String SHARK_HOME = "";
	private String SPARK_HOME = "";
	private String SPARK_MASTER = "";
	
	public WorkflowInputFormat() {
	}
	
	public String getHadoopHome() {
		return HADOOP_HOME;
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
	
	public String getSharkHome() {
		return SHARK_HOME;
	}
	
	public String getSparkHome() {
		return SPARK_HOME;
	}
	
	public String getSparkMaster() {
		return SPARK_MASTER;
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
			
			if (key.equals(BigConfConstants.BIGFRAME_HDFS_ROOTDIR)) {
				HDFS_ROOT_DIR = value;
			}
			
			
			else if (key.equals(BigConfConstants.BIGFRAME_HIVE_HOME)) {
				HIVE_HOME = value;
			}
			
			else if (key.equals(BigConfConstants.BIGFRAME_HIVE_JDBC_SERVER)) {
				HIVE_JDBC_SERVER = value;
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
		}
		
	}

	@Override
	public void printConf() {
		System.out.println("Workflow Driver configuration:");
		System.out.println("Hadoop Home:" + HADOOP_HOME);
		System.out.println("Hadoop Root Dir: " + HDFS_ROOT_DIR);
		System.out.println("Hive Home: " + HIVE_HOME);
		System.out.println("Shark Home: " + SHARK_HOME);
		System.out.println("Spark Home: " + SPARK_HOME);
		System.out.println("Spark MASTER: " + SPARK_MASTER);
	}
}
