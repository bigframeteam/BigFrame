package bigframe.util.dataloader.vertica;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import bigframe.bigif.WorkflowInputFormat;
//import bigframe.util.TableNotFoundException;
import bigframe.util.dataloader.DataLoader;

public abstract class VerticaDataLoader extends DataLoader {

	protected static String MAPRED_VERTICA_TABLE_NAME = "mapred.vertica.table.name";
	
	protected static String MAPRED_VERTICA_DATABASE = "mapred.vertica.database";
	protected static String MAPRED_VERTICA_USERNAME = "mapred.vertica.username";
	protected static String MAPRED_VERTICA_PASSWORD = "mapred.vertica.password";
	protected static String MAPRED_VERTICA_HOSTNAMES = "mapred.vertica.hostnames";
	protected static String MAPRED_VERTICA_PORT = "mapred.vertica.port";
	
	
	public VerticaDataLoader(WorkflowInputFormat workIF) {
		super(workIF);
		

		// TODO Auto-generated constructor stub
	}


	public abstract boolean load(Path srcHdfsPath, String table);

}
