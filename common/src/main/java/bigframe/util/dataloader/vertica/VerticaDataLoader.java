package bigframe.util.dataloader.vertica;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import bigframe.bigif.WorkflowInputFormat;
import bigframe.util.dataloader.DataLoader;

public abstract class VerticaDataLoader extends DataLoader {

	private static final Logger LOG = Logger.getLogger(VerticaDataLoader .class);
	
	protected static String MAPRED_VERTICA_TABLE_NAME = "mapred.vertica.table.name";
	
	protected static String MAPRED_VERTICA_DATABASE = "mapred.vertica.database";
	protected static String MAPRED_VERTICA_USERNAME = "mapred.vertica.username";
	protected static String MAPRED_VERTICA_PASSWORD = "mapred.vertica.password";
	protected static String MAPRED_VERTICA_HOSTNAMES = "mapred.vertica.hostnames";
	protected static String MAPRED_VERTICA_PORT = "mapred.vertica.port";
	
	protected static String driverName = "com.vertica.jdbc.Driver";
	protected Connection connection;
	
	public VerticaDataLoader(WorkflowInputFormat workIF) {
		super(workIF);
		

		// TODO Auto-generated constructor stub
	}

	protected void initConnection() {
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
				LOG.error("Cannot connect to JDBC server! " +
						"Make sure Vertica is running!");
				System.exit(1);
			}
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	
	}
	
	protected void closeConnection() {
		if(connection != null)
			try {
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	public abstract void prepareBaseTable() throws SQLException;
	
	public abstract void alterBaseTable() throws SQLException;
	
	public abstract boolean load(Path srcHdfsPath, String table);

}
