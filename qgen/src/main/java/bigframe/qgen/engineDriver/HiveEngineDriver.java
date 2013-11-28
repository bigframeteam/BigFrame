package bigframe.qgen.engineDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.WorkflowInputFormat;
import bigframe.workflows.runnable.HiveRunnable;

/**
 * A class to control the workflow running on hvie system.
 * 
 * @author andy
 *
 */
public class HiveEngineDriver extends EngineDriver {
	private Connection connection;
	private List<HiveRunnable> queries = new ArrayList<HiveRunnable>();
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	private static final Log LOG = LogFactory.getLog(HiveEngineDriver.class);
	//private static int hiveServer_version = 1;
	
	public HiveEngineDriver(WorkflowInputFormat workIF) {
		super(workIF);
		// TODO Auto-generated constructor stub
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
			
			String UDF_JAR = workIF.getProp().get(BigConfConstants.BIGFRAME_UDF_JAR);
			
			connection.createStatement().execute("DELETE JAR " + UDF_JAR);
			LOG.info("Adding UDF JAR " + UDF_JAR + " to hive server");
			if(connection.createStatement().execute("ADD JAR " + UDF_JAR)) {
				LOG.info("Adding UDF JAR successful!");
			}
			else {
				LOG.error("Adding UDF JAR failed!");
			}
			
			for(HiveRunnable query : queries) {
				LOG.info("Prepare tables...");
				query.prepareHiveTables(connection);
			}
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void addQuery(HiveRunnable query) {
		queries.add(query);
	}
	
	@Override
	public void run() {
		LOG.info("Running Hive Query");
		
		for(HiveRunnable query : queries) {
			if(query.runHive(connection))
				LOG.info("Query Finished");
			else
				LOG.info("Query failed");
		}
	}

	@Override
	public void cleanup() {

		
		for(HiveRunnable query : queries) {
			query.cleanUpHive(connection);
		}
		
		if(connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	@Override
	public int numOfQueries() {

		return queries.size();
	}

}
