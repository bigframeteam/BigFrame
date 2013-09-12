package bigframe.qgen.engineDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import bigframe.bigif.WorkflowInputFormat;
import bigframe.queries.HiveRunnable;

/**
 * A class to control the workflow running on hvie system.
 * 
 * @author andy
 *
 */
public class HiveWorkflow extends Workflow {
	private Connection connection;
	private List<HiveRunnable> queries = new ArrayList<HiveRunnable>();
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	//private static int hiveServer_version = 1;
	
	public HiveWorkflow(WorkflowInputFormat workIF) {
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
			System.out.println("Before connection!!!");
			connection = DriverManager.getConnection(workIF.getHiveJDBCServer(), "", "");
			System.out.println("After connection!!!");
			if(connection == null) {
				System.out.println("Cannot connect to JDBC server! " +
						"Make sure the HiveServer is running!");
				System.exit(1);
			}
			for(HiveRunnable query : queries) {
				System.out.println("Prepare tables!!!");
				query.prepareTables(connection);
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
		System.out.println("Running Hive Query");
		
		for(HiveRunnable query : queries) {
			query.run(connection);
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public int numOfQueries() {

		return queries.size();
	}

}
