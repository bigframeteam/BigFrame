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
import bigframe.workflows.events.BigFrameListenerBus;
import bigframe.workflows.runnable.HiveRunnable;

/**
 * A class to control the workflow running on hvie system.
 * 
 * @author andy
 *
 */
public class HiveEngineDriver extends EngineDriver {
	private Connection connection;
	private Statement stmt;
	private List<HiveRunnable> queries = new ArrayList<HiveRunnable>();
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
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
			
			stmt = connection.createStatement();
			stmt.execute("DELETE JAR " + UDF_JAR);
			LOG.info("Adding UDF JAR " + UDF_JAR + " to hive server");
			if(stmt.execute("ADD JAR " + UDF_JAR)) {
				LOG.info("Adding UDF JAR successful!");
			}
			else {
				LOG.error("Adding UDF JAR failed!");
			}
			init(connection);
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void init(Connection connection) {
		try {
			if(connection == null) {
				System.out.println("Null connection");
				System.exit(1);
			}
			this.connection = connection;
			if(stmt == null) {
				stmt = connection.createStatement();
			}
			//TODO Set hive engine to mr
			String setHiveOnTez = "set " + "hive.execution.engine" + "=" + "mr";
			stmt.execute(setHiveOnTez);		
			LOG.info("Setting Hive engine to MR");

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
	public void run(BigFrameListenerBus eventBus) {
		LOG.info("Running Hive Query");
		
		for(HiveRunnable query : queries) {
			if(query.runHive(connection, eventBus))
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

		try {
			if(stmt != null) {
				stmt.close();
			}
			if(connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

	@Override
	public int numOfQueries() {

		return queries.size();
	}

}
