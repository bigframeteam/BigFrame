package bigframe.util.dataloader.hana;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import bigframe.bigif.WorkflowInputFormat;
//import bigframe.util.TableNotFoundException;
import bigframe.util.dataloader.DataLoader;
import org.apache.sqoop.Sqoop;


public abstract class HanaDataLoader extends DataLoader {

	protected static String MAPRED_HANA_TABLE_NAME = "mapred.hana.table.name";
	
	protected static String MAPRED_HANA_DATABASE = "mapred.hana.database";
	protected static String MAPRED_HANA_USERNAME = "mapred.hana.username";
	protected static String MAPRED_HANA_PASSWORD = "mapred.hana.password";
	protected static String MAPRED_HANA_HOSTNAMES = "mapred.hana.hostnames";
	protected static String MAPRED_HANA_PORT = "mapred.hana.port";
	
	protected static String driverName = "com.sap.db.jdbc.Driver";
	
	protected Connection connection;

	
	public HanaDataLoader(WorkflowInputFormat workIF) {
		super(workIF);
		init();
		// TODO Auto-generated constructor stub
	}
	
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
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}		
	}
		
	public boolean load(String srcHdfsPath, String tableName) {
		
		//create tables
		createTable();

		Configuration config = new Configuration(); 
	    config.addResource(new Path(workIF.getHadoopHome() + "/conf/core-site.xml"));
	    config.addResource(new Path(workIF.getHadoopHome() + "/conf/mapred-site.xml"));


		//call sqoop job to port the table.
        String[] sqoop_conf = { "export","-Dsqoop.export.records.per.statement=10000", 
				"--connect", workIF.getHanaJDBCServer(), 
				"--driver", driverName,
				"--table", tableName, 
				"--export-dir", srcHdfsPath,
                "--username", workIF.getHanaUserName(), 
                "--password", workIF.getHanaPassword(),
                "--fields-terminated-by", "|",
                "--batch" };
        
        Sqoop.runTool(sqoop_conf, config);
		
		return true;
	};
	
	public abstract boolean createTable();

	public abstract boolean preProcess(String hdfs);
}
