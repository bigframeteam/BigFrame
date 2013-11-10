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
import bigframe.workflows.runnable.SharkRunnable;


public class SharkEngineDriver extends EngineDriver {

	private Connection connection;
	private List<SharkRunnable> queries = new ArrayList<SharkRunnable>();
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	private static final Log LOG = LogFactory.getLog(SharkEngineDriver.class);
	
	public SharkEngineDriver(WorkflowInputFormat workIF) {
		super(workIF);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int numOfQueries() {
		
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
			LOG.info("Connectiong to Shark JDBC server!!!");
			connection = DriverManager.getConnection(workIF.getHiveJDBCServer(), "", "");
    	  
			if(connection == null) {
				LOG.error("Cannot connect to JDBC server! " +
						"Make sure the SharkServer is running!");
				System.exit(1);
			}
			else
				LOG.info("Successful!!!");
			
			String UDF_JAR = workIF.getProp().get(BigConfConstants.BIGFRAME_UDF_JAR);
			
			connection.createStatement().execute("DELETE JAR " + UDF_JAR);
			LOG.info("Adding UDF JAR " + UDF_JAR + " to shark server");
			if(connection.createStatement().execute("ADD JAR " + UDF_JAR)) {
				LOG.info("Adding UDF JAR successful!");
			}
			else {
				LOG.error("Adding UDF JAR failed!");
			}
			
			for(SharkRunnable query : queries) {
				LOG.info("Prepare tables...");
				query.prepareSharkTables(connection);
			}
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void addQuery(SharkRunnable query) {
		queries.add(query);
	}
	
	@Override
	public void run() {
		System.out.println("Running Shark Query");
		
		for(SharkRunnable query : queries) {
			if(query.runShark(connection))
				LOG.info("Query Finished");
			else
				LOG.info("Query failed");
		}

	}

	@Override
	public void cleanup() {
		for(SharkRunnable query : queries)
			query.cleanUpShark(connection);

	}

}
