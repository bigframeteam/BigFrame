package bigframe.util.dataloader.hana;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.sqoop.Sqoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.commons.io.FileUtils;

import bigframe.bigif.WorkflowInputFormat;

public class HanaTweetLoader extends HanaDataLoader {
	
	public HanaTweetLoader(WorkflowInputFormat workIF) {
		super(workIF);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean createTable() {
		
		String create_tweet = "create column table Tweet( " +
							"	id int, " +
							"	user_id int, " +
							"	created_at timestamp, " +
							"	text varchar(200) " +
							") ";
					
		String create_entities = "create column table Entities( " +
							"	tweet_id int, " +
							"	hashtag varchar(50) " +
							") ";
		
		String create_score_map = " create column table score_map( " +
							"	name varchar(50), " +
							"	score INT " +
							" ) ";	
		
		try {
			Statement stmt = connection.createStatement();
			stmt.execute(create_tweet);
			stmt.execute(create_entities);
			stmt.execute(create_score_map);

		} catch (SQLException e) {
			System.out.println("Please make sure to drop tables before load the data. (Tweet, Entities, Score_Map)");
			return false;
		}
		return true;
	}
	
	@Override
	public boolean preProcess(String srcHdfsPath) {
		//load score map table
		if(!loadScoreMap()) {
			System.out.println("Fail to load Score_Map table!");
			return false;
		};
		
		//parsing tweet table data to sqoop compatible format
		TweetParser tweet_parser = new TweetParser();
		if (!tweet_parser.run(srcHdfsPath, workIF)) {
			return false;
		}
		
		//parsing entities table data to sqoop compatible format
		EntitiesParser entities_parser = new EntitiesParser();
		if (!entities_parser.run(srcHdfsPath, workIF)) {
			return false;
		}
		return true;
	}
	
	@Override
	public boolean load(String srcHdfsPath, String tableName) {
		String path = workIF.getHDFSRootDIR() +"/parsed_" + tableName;
		return super.load(path, tableName);
	}

	@Override
	public boolean load() {
		
		// TODO Auto-generated method stub
		return false;
	}
	
	@SuppressWarnings("resource")
	public boolean loadScoreMap() {

		InputStream fis = null;

		BufferedReader reader = null;

		try {
			Statement stmt = connection.createStatement();
			
			fis = HanaTweetLoader.class.getClassLoader().getResourceAsStream("AFINN-111.txt");

			reader = new BufferedReader(new InputStreamReader(fis));

			PreparedStatement pstmt = connection.prepareStatement("INSERT INTO SCORE_MAP VALUES (?,?)");
			String sentiment_info = reader.readLine();

			while (sentiment_info != null) {

				String[] info = sentiment_info.split("\t");
				String name = info[0];
				double score = Double.parseDouble(info[1]);

				pstmt.setString(1, name);
				pstmt.setDouble(2, score);

				pstmt.addBatch();
				sentiment_info = reader.readLine();
			}
			connection.setAutoCommit(false);
			pstmt.executeBatch();
			connection.commit();
			connection.setAutoCommit(true);

			pstmt.close();
			stmt.close();
		} catch (SQLException se) {
			se.printStackTrace();
			return false;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}	
		return true;
	}
}
