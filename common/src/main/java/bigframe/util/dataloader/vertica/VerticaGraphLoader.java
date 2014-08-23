package bigframe.util.dataloader.vertica;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.vertica.hadoop.VerticaOutputFormat;
import com.vertica.hadoop.VerticaRecord;

import bigframe.bigif.WorkflowInputFormat;
import bigframe.util.dataloader.vertica.VerticaTweetLoader.Map;

public class VerticaGraphLoader extends VerticaDataLoader {

	public VerticaGraphLoader(WorkflowInputFormat workIF) {
		super(workIF);
		// TODO Auto-generated constructor stub
	}

	
	public static class Map extends
		Mapper<LongWritable, Text, Text, VerticaRecord> {
		
		VerticaRecord record = null;
		
		String tableName;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			try {
				record = new VerticaRecord(context.getConfiguration());
			} catch (Exception e) {
				throw new IOException(e);
			}
			
			tableName = context.getConfiguration().get(MAPRED_VERTICA_TABLE_NAME);
		}

		@Override
	    public void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {

			if (record == null) {
				throw new IOException("No output record found");
			}
			
			String [] fields = value.toString().split("\\|");
			
			for(int i = 0; i < fields.length; i++ ) {
				try {
//					System.out.println("The value: " + fields[i]);
					record.setFromString(i, fields[i]);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			context.write(new Text(tableName), record);
			
	    }
	}
	
	
	
	@Override
	public boolean load(Path srcHdfsPath, String table) {
		// TODO Auto-generated method stub

		 
	    	initConnection();
	    	try {
				Statement stmt = connection.createStatement();
			
			    if(table.equals("twitter_graph")){
	//			    VerticaOutputFormat.setOutput(job, "twitter_graph", true, "friend_id int", "follower_id int");
			    	String copyToTwitterGraph = "COPY twitter_graph SOURCE Hdfs(url='" + workIF.getWEBHDFSRootDIR() + 
			    			srcHdfsPath + "/part-*'," +
			    			"username='" + workIF.getHadoopUserName() + "')";
			    	
			    	if(stmt.execute(copyToTwitterGraph))
			    		return true;
			    	else 
			    		return false;
			    	
			    }
			    
			    else {
			    	//throw new TableNotFoundException("Table " + table + " doesn't exist!");
			    	System.out.println("Table " + table + " doesn't exist!");
			    	return false;
			    }
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
		    closeConnection();

		
		return false;
	}

	@Override
	public boolean load() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void prepareBaseTable() throws SQLException {
		initConnection(); 
		Statement stmt = connection.createStatement();
		connection.setAutoCommit(false);
		String [] drop_table = {"DROP TABLE IF EXISTS twitter_graph cascade"};
		
		for(String str : drop_table) {
			stmt.addBatch(str);
		}
		
		String createWebSales = "create table twitter_graph(friend_id int, follower_id int)"; 
		stmt.addBatch(createWebSales);
		
		
		System.out.println("Preparing base tables!");
		stmt.executeBatch();
		connection.commit();		
		
		closeConnection();
		
	}

	@Override
	public void alterBaseTable() throws SQLException {
		// TODO Auto-generated method stub
		
	}

}
