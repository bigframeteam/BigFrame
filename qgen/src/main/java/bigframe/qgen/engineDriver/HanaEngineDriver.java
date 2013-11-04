package bigframe.qgen.engineDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import bigframe.bigif.WorkflowInputFormat;
import bigframe.workflows.runnable.HanaRunnable;

public class HanaEngineDriver extends EngineDriver{
	private Connection connection;
	
	private static final Logger LOG = Logger.getLogger(HanaEngineDriver.class);
	private List<HanaRunnable> queries = new ArrayList<HanaRunnable>();
	
	private static String driverName = "com.sap.db.jdbc.Driver";

	public HanaEngineDriver(WorkflowInputFormat workIF) {
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
			connection = DriverManager.getConnection(workIF.getHanaJDBCServer(), 
					workIF.getHanaUserName(), workIF.getHanaPassword());
    	  
			if(connection == null) {
				System.out.println("Cannot connect to JDBC server! " +
						"Make sure Hana is running!");
				System.exit(1);
			}
				
			for(HanaRunnable query : queries) {
				System.out.println("Prepare tables!!!");
				query.prepareHanaTables(connection);
			}

		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}		
	}

	@Override
	public void run() {
		System.out.println("Running Hana Query");
		
		for(HanaRunnable query : queries) {
			if(query.runHana(connection))
				System.out.println("Query Finished");
			else
				System.out.println("Query failed");
		}
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	public void addQuery(HanaRunnable query) {
		queries.add(query);
	}

}
