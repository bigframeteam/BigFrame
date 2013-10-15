package bigframe.qgen.engineDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import bigframe.bigif.WorkflowInputFormat;
import bigframe.workflows.SharkRunnable;


public class SharkEngineDriver extends EngineDriver {

	private Connection connection;
	private List<SharkRunnable> queries = new ArrayList<SharkRunnable>();
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
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
			connection = DriverManager.getConnection(workIF.getHiveJDBCServer(), "", "");
    	  
			if(connection == null) {
				System.out.println("Cannot connect to JDBC server! " +
						"Make sure the SharkServer is running!");
				System.exit(1);
			}
			for(SharkRunnable query : queries) {
				query.prepareTables(connection);
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
			if(query.run(connection))
				System.out.println("Query Finished");
			else
				System.out.println("Query failed");
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
