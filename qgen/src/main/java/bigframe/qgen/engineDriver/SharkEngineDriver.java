package bigframe.qgen.engineDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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
			
			Statement stmt = connection.createStatement();
			stmt.execute("DELETE JAR " + UDF_JAR);
			LOG.info("Adding UDF JAR " + UDF_JAR + " to hive server");
			if(stmt.execute("ADD JAR " + UDF_JAR)) {
				LOG.info("Adding UDF JAR successful!");
				stmt.execute("create temporary function sentiment as \'bigframe.workflows.util.SenExtractorHive\'");
				stmt.execute("create temporary function isWithinDate as \'bigframe.workflows.util.WithinDateHive\'");
//				stmt.execute("set hive.auto.convert.join=false");
				stmt.execute("set mapred.reduce.tasks=40");
			}
			else {
				LOG.error("Adding UDF JAR failed!");
			}
			
			if(!workIF.getSkipPrepareTable())
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
		LOG.info("Running Shark Query");
		
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

		if(connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
