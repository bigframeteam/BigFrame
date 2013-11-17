package bigframe.qgen.engineDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.WorkflowInputFormat;
import bigframe.workflows.runnable.HiveGiraphRunnable;
import bigframe.workflows.runnable.HiveRunnable;

public class HiveGiraphEngineDriver extends EngineDriver {
	
	private HiveConf hive_config;
	private static final Logger LOG = Logger.getLogger(HiveGiraphEngineDriver.class);
	private List<HiveGiraphRunnable> queries = new ArrayList<HiveGiraphRunnable>();
	
	private Connection connection;
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

	public HiveGiraphEngineDriver(WorkflowInputFormat workIF) {
		super(workIF);
		hive_config = new HiveConf();
		hive_config.addResource(new Path(workIF.getHiveHome()
				+ "/conf/hive-site.xml"));
		hive_config.addResource(new Path(workIF.getHadoopHome()
				+ "/conf/core-site.xml"));
		hive_config.addResource(new Path(workIF.getHadoopHome()
				+ "/conf/mapred-site.xml"));
		// TODO Auto-generated constructor stub
	}

	@Override
	public int numOfQueries() {
		// TODO Auto-generated method stub
		return queries.size();
	}

	@Override
	public void init() {
		try {
			Class.forName(driverName);
        } catch (ClassNotFoundException e) {
        	// TODO Auto-generated catch block
        	e.printStackTrace();
        	System.exit(1);
        }

		try {
			LOG.info("Connectiong to Hive JDBC server!!!");
			connection = DriverManager.getConnection(workIF.getHiveJDBCServer(), "", "");
			if(connection == null) {
				LOG.error("Cannot connect to JDBC server! " +
						"Make sure the HiveServer is running!");
				System.exit(1);
			}
			else
				LOG.info("Successful!!!");
			
			for(HiveGiraphRunnable query : queries) {
				LOG.info("Prepare tables...");
				query.prepareHiveGiraphTables(connection);
			}
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public void run() {
		System.out.println("Running HiveGiraph queries!");
		for(HiveGiraphRunnable query : queries) {
			query.runGiraph(hive_config);
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}
	
	public void addQuery(HiveGiraphRunnable query) {
		queries.add(query);
	}
}
