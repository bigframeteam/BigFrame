package bigframe.qgen.engineDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import bigframe.bigif.WorkflowInputFormat;
import bigframe.workflows.runnable.HiveRunnable;
import bigframe.workflows.runnable.SharkRunnable;
import bigframe.workflows.runnable.VerticaRunnable;

public class VerticaEngineDriver extends EngineDriver {

	private Connection connection;
	
	private static final Logger LOG = Logger.getLogger(HadoopEngineDriver.class);
	private List<VerticaRunnable> queries = new ArrayList<VerticaRunnable>();
	
	private static String driverName = "com.vertica.jdbc.Driver";
	
	public VerticaEngineDriver(WorkflowInputFormat workIF) {
		super(workIF);
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
			connection = DriverManager.getConnection(workIF.getVerticaJDBCServer(), 
					workIF.getVerticaUserName(), workIF.getVerticaPassword());
    	  
			if(connection == null) {
				System.out.println("Cannot connect to JDBC server! " +
						"Make sure Vertica is running!");
				System.exit(1);
			}
				
			for(VerticaRunnable query : queries) {
				System.out.println("Prepare tables!!!");
				query.prepareVerticaTables(connection);
			}

		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}

	}

	@Override
	public void run() {
		System.out.println("Running Vertica Query");
		
		for(VerticaRunnable query : queries) {
			if(query.runVertica(connection))
				System.out.println("Query Finished");
			else
				System.out.println("Query failed");
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}
	
	public void addQuery(VerticaRunnable query) {
		queries.add(query);
	}

}
