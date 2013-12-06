package bigframe.util.dataloader.vertica;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.vertica.hadoop.VerticaOutputFormat;
import com.vertica.hadoop.VerticaRecord;

import bigframe.bigif.BigConfConstants;
import bigframe.bigif.WorkflowInputFormat;
//import bigframe.util.TableNotFoundException;
import bigframe.util.parser.JsonParser;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * A data loader that can load tweet data from different sources into Vertica.
 *  
 * @author andy
 *
 */
public class VerticaTweetLoader extends VerticaDataLoader {

	private static String TWEET_TEMP_DIR = "tweet_temp_dir";
	private static String ENTITIES_TEMP_DIR = "entities_temp_dir";
	private static String USERS_TEMP_DIR = "users_temp_dir";
	private static String TWEETJSON_TEMP_DIR = "tweetjson_temp_dir";
	
	public VerticaTweetLoader(WorkflowInputFormat workflowIF) {
		super(workflowIF);
		// TODO Auto-generated constructor stub
	}

	
	public static class ParseTweetMapper extends
	Mapper<LongWritable, Text, NullWritable, Text> {
		
		String tableName;
		
		SimpleDateFormat twitterDateFormat;
	
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			twitterDateFormat = new SimpleDateFormat(
					"EEE MMM dd HH:mm:ss ZZZZZ yyyy", java.util.Locale.ENGLISH);
			
			tableName = context.getConfiguration().get(MAPRED_VERTICA_TABLE_NAME);
		}
		
		@Override
	    public void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {
			
			JSONObject tweet_json = JsonParser.parseJsonFromString(value.toString());
			
			List<String> atts = new LinkedList<String>();
			StringBuffer record = new StringBuffer();
			
			
			if(tableName.equals("tweet")) {
			
				atts.add( (String) tweet_json.get("coordinates") );
				
				try {
					String created_at = (String) tweet_json.get("created_at");
					Timestamp created_date = new Timestamp(twitterDateFormat.parse(created_at).getTime());
					atts.add( created_date.toString() );
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				JSONObject user_json = (JSONObject) tweet_json.get("user");
				Long user_id = (Long) user_json.get("id");
	
				atts.add( (String) tweet_json.get("favorited") );
				atts.add( (String) tweet_json.get("truncated") );
				atts.add( (String) tweet_json.get("id_str") );
				atts.add( (String) tweet_json.get("in_reply_to_user_id_str") );
				atts.add( (String) tweet_json.get("text") );
				atts.add( (String) tweet_json.get("contributors") );
				atts.add( ((Long) tweet_json.get("id")).toString() );
				atts.add( (String) tweet_json.get("retweet_count") );
				atts.add( (String) tweet_json.get("in_reply_to_status_id_str") );
				atts.add( (String) tweet_json.get("geo") );
				atts.add( (String) tweet_json.get("retweeted") );
				atts.add( (String) tweet_json.get("in_reply_to_user_id") );
				atts.add( user_id.toString());
				atts.add( (String) tweet_json.get("in_reply_to_screen_name") );
				atts.add( (String) tweet_json.get("source") );
				atts.add( (String) tweet_json.get("place") );
				atts.add( (String) tweet_json.get("in_reply_to_status_id") );
				
				
				
				for(int i = 0; i < atts.size(); i++) {
					record.append(atts.get(i));
					if(i<atts.size()-1)
						record.append("|");
				}
				
				context.write(null, new Text(record.toString()));
			}
			
			else if(tableName.equals("entities")) {
				
				Long tweet_id = (Long) tweet_json.get("id");

				atts.add( tweet_id.toString() );
				
				
				JSONObject entities_json = (JSONObject) tweet_json.get("entities");
				atts.add( ((JSONArray)entities_json.get("urls")).toJSONString() );
				JSONArray hashtags = (JSONArray) entities_json.get("hashtags");
				
				if (hashtags.isEmpty()){
					atts.add("");
					atts.add( ((JSONArray)entities_json.get("user_mentions")).toJSONString() );
					for(int i = 0; i < atts.size(); i++) {
						record.append(atts.get(i));
						if(i<atts.size()-1)
							record.append("|");
					}
					
					context.write(null, new Text(record.toString()));
				}
				//String hashtags_str = hashtags.toString().substring(1,hashtags.toString().length()-1);
				else {
					for(Object tag : hashtags) {
						String tag_str = (String) tag;
						
						atts.add(tag_str);
						atts.add( ((JSONArray)entities_json.get("user_mentions")).toJSONString() );
						
						for(int i = 0; i < atts.size(); i++) {
							record.append(atts.get(i));
							if(i<atts.size()-1)
								record.append("|");
						}
						
						context.write(null, new Text(record.toString()));
						
					}				
				}

			}
			
//			else if(tableName.equals("users")) {
//				JSONObject users_json = (JSONObject) tweet_json.get("users");
//			}
			
			else if(tableName.equals("tweetjson")) {
				context.write(null, value);
			}
			
		}
	
	}
	

	
	public void prepareBaseTable() throws SQLException {
		initConnection(); 
		Statement stmt = connection.createStatement();
		connection.setAutoCommit(false);
		String [] drop_table = {"DROP TABLE IF EXISTS users",
								"DROP TABLE IF EXISTS entities",
								"DROP TABLE IF EXISTS tweetjson",
								"DROP TABLE IF EXISTS tweet"};
		
		for(String str : drop_table) {
			stmt.addBatch(str);
		}
		
		
			
    	String createUsers = "CREATE TABLE users (profile_sidebar_border_color char(20)," +
			    		"name varchar(70), profile_sidebar_fill_color char(20), profile_background_tile char(20), "+
			    		"profile_image_url varchar(70), location varchar(20), created_at  TIMESTAMP WITH TIMEZONE," +
			    		"id_str char(20), follow_request_sent varchar(20), profile_link_color char(20)," + 
			    		"favourites_count char(10),  url varchar(70), contributors_enabled BOOLEAN, utc_offset varchar(20)," +
			    		"id char(10), profile_use_background_image varchar(70), listed_count char(10), protected BOOLEAN," +
			    		"lang char(20), profile_text_color char(20), followers_count char(10), time_zone char(20)," +
			    		"verified char(10), geo_enabled char(10), profile_background_color varchar(70)," +
			    		"notifications char(10), description varchar(200), friends_count char(10), profile_background_image_url varchar(120),"+
			    		"statuses_count char(10), screen_name char(20), following char(10), show_all_inline_media char(10))";
    
		String createTweetJson = "CREATE TABLE tweetjson (json varchar(10000))";
		
    	String createEntities = "CREATE TABLE entities (tweet_id int, urls varchar(100)," +
			    		"hashtag varchar(50), user_mentions varchar(200))";
    
    	String createTweet = "Create TABLE tweet (coordinates char(20), created_at TIMESTAMP WITHOUT TIME ZONE," +
		    			"favorited char(10), truncated char(10), id_str char(20), in_reply_to_user_id_str char(20)," +
		    			"text varchar(200), contributors char(20), id int, retweet_count char(10), in_reply_to_status_id_str char(20)," +
		    			"geo char(20), retweeted char(10), in_reply_to_user_id char(10), user_id int, in_reply_to_screen_name char(20)," +
		    			"source char(20), place char(20), in_reply_to_status_id char(10))";


    	stmt.addBatch(createUsers);
    	stmt.addBatch(createEntities);
    	stmt.addBatch(createTweet);
    	stmt.addBatch(createTweetJson);
    			
    	System.out.println("Preparing base tables!");
		stmt.executeBatch();
		connection.commit();	
		closeConnection();
	}

	

	@Override
	public void alterBaseTable() throws SQLException {
		initConnection(); 
		Statement stmt = connection.createStatement();
		connection.setAutoCommit(false);
		String [] alterTables = {"ALTER TABLE entities ADD PRIMARY KEY (tweet_id)",
								 "ALTER TABLE tweet ADD PRIMARY KEY (id)"};
		
		for(String str : alterTables) {
			stmt.addBatch(str);
		}
		
		stmt.executeBatch();
		connection.commit();
		
	}
	
	@Override
	public boolean load(Path srcHdfsPath, String table) {
		
		Configuration mapred_config = new Configuration();
		
		mapred_config.addResource(new Path(workIF.getHadoopHome()
				+ "/conf/core-site.xml"));
		mapred_config.addResource(new Path(workIF.getHadoopHome()
				+ "/conf/mapred-site.xml"));
		
		mapred_config.set(MAPRED_VERTICA_DATABASE, workIF.getVerticaDatabase());
		mapred_config.set(MAPRED_VERTICA_USERNAME, workIF.getVerticaUserName());
		mapred_config.set(MAPRED_VERTICA_PASSWORD, workIF.getVerticaPassword());
		mapred_config.set(MAPRED_VERTICA_HOSTNAMES, workIF.getVerticaHostNames());
		mapred_config.set(MAPRED_VERTICA_PORT, workIF.getVerticaPort().toString());
		
		
		try {
			
			mapred_config.set(MAPRED_VERTICA_TABLE_NAME, table);
			
			Job job = new Job(mapred_config);
		    
			FileInputFormat.setInputPaths(job, srcHdfsPath);
	
		    job.setJobName("Transforming tweet for table: " + table);
		    
//		    job.setOutputKeyClass(Text.class);
//		    job.setOutputValueClass(VerticaRecord.class);
//		    
//		    job.setOutputFormatClass(VerticaOutputFormat.class);
//		    
//		    job.setJarByClass(VerticaTpcdsLoader.class);
//		    job.setMapperClass(Map.class);
//		    //job.setReducerClass(Reduce.class);
//		    
//		    job.setNumReduceTasks(0);
//		 
//		    if(table.equals("entities"))
//			    VerticaOutputFormat.setOutput(job, "entities", true, "tweet_id int", "urls varchar(100)",
//			    		"hashtag varchar(50)", "user_mentions varchar(200)");
//		   
//		    else if(table.equals("user"))
//			    VerticaOutputFormat.setOutput(job, "users", true, "profile_sidebar_border_color char(20)", 
//			    		"name varchar(70)", "profile_sidebar_fill_color char(20)", "profile_background_tile char(20)",
//			    		"profile_image_url varchar(70)", "location varchar(20)", "created_at  TIMESTAMP WITH TIMEZONE",
//			    		"id_str char(20)", "follow_request_sent varchar(20)", "profile_link_color char(20)", 
//			    		"favourites_count int",  "url varchar(70)", "contributors_enabled BOOLEAN", "utc_offset varchar(20)",
//			    		"id int", "profile_use_background_image varchar(70)", "listed_count int", "protected BOOLEAN",
//			    		"lang char(20)", "profile_text_color char(20)", "followers_count int", "time_zone char(20)",
//			    		"verified boolean", "geo_enabled boolean", "profile_background_color varchar(70)",
//			    		"notifications boolean", "description varchar(200)", "friends_count int", "profile_background_image_url varchar(120)",
//			    		"statuses_count int", "screen_name char(20)", "following boolean", "show_all_inline_media boolean");
//		    
//		    else if(table.equals("tweet")) 
//		    	VerticaOutputFormat.setOutput(job, "tweet", true, "coordinates char(20)", "created_at TIMESTAMP WITHOUT TIME ZONE",
//		    			"favorited boolean", "truncated boolean", "id_str char(20)", "in_reply_to_user_id_str char(20)",
//		    			"text varchar(200)", "contributors char(20)", "id int", "retweet_count int", "in_reply_to_status_id_str char(20)",
//		    			"geo char(20)", "retweeted boolean", "in_reply_to_user_id int", "user_id int", "in_reply_to_screen_name char(20)",
//		    			"source char(20)", "place char(20)", "in_reply_to_status_id int");
//		    
//		    else if(table.equals("tweetjson"))
//		    	VerticaOutputFormat.setOutput(job, "tweetjson", true, "json varchar(10000)");
//		    
//		    else {
//		    	//throw new TableNotFoundException("Table " + table + " doesn't exist!");
//		    	System.out.println("Table " + table + " doesn't exist!");
//		    	return false;
//		    }
		
		    
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    
		    job.setJarByClass(VerticaTpcdsLoader.class);
		    job.setMapperClass(ParseTweetMapper.class);
		    
		    job.setNumReduceTasks(0);
		   
	    	initConnection();
	    	try {
				Statement stmt = connection.createStatement();
				FileSystem fs = FileSystem.get(mapred_config);
			    if(table.equals("tweet")) {
					
					String hdfs_dir = TWEET_TEMP_DIR;
					Path outputDir = new Path(hdfs_dir);	

					if(fs.exists(outputDir))
						fs.delete(outputDir, true);
			    	
					FileOutputFormat.setOutputPath(job, outputDir);
					
			    	if(job.waitForCompletion(true)) {
			    	
				    	String copyToTweet = "COPY tweet SOURCE Hdfs(url='" + workIF.getWEBHDFSRootDIR() +"/"+TWEET_TEMP_DIR+"/part-*'," +
				    			"username='" + workIF.getHadoopUserName() + "')";
				    	
				    	if(stmt.execute(copyToTweet))
				    		return true;
				    	else 
				    		return false;
			    	}
			    	else
			    		return false;
			    }
			    
			    else  if(table.equals("entities")) {
					String hdfs_dir = ENTITIES_TEMP_DIR;
					Path outputDir = new Path(hdfs_dir);	

					if(fs.exists(outputDir))
						fs.delete(outputDir, true);
			    	
					FileOutputFormat.setOutputPath(job, outputDir);
					
			    	if(job.waitForCompletion(true)) {
			    	
				    	String copyToEntites = "COPY entities SOURCE Hdfs(url='" + workIF.getWEBHDFSRootDIR() +"/"+ENTITIES_TEMP_DIR+"/part-*'," +
				    			"username='" + workIF.getHadoopUserName() + "')";
				    	
				    	if(stmt.execute(copyToEntites))
				    		return true;
				    	else 
				    		return false;
			    	}
			    	else
			    		return false;
			    }
			    
			    else  if(table.equals("tweetjson")) {
			    	String copyToTweetJson = "COPY tweetjson SOURCE Hdfs(url='" + workIF.getWEBHDFSRootDIR() +"/nested_data/part-*'," +
			    			"username='" + workIF.getHadoopUserName() + "')";
			    	
			    	if(stmt.execute(copyToTweetJson))
			    		return true;
			    	else 
			    		return false;
			    	
			    }
			    
			    else {
			    	System.out.println("Table " + table + " doesn't exist!");
			    	return false;
			    }
	    
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
		    closeConnection();
		    
		    
		
			return true;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}

	@Override
	public boolean load() {
		// TODO Auto-generated method stub
		return false;
	}

	public static class Map extends
	Mapper<LongWritable, Text, Text, VerticaRecord> {
	
		VerticaRecord record = null;
		
		String tableName;
		
		SimpleDateFormat twitterDateFormat;
	
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			try {
				record = new VerticaRecord(context.getConfiguration());
			} catch (Exception e) {
				throw new IOException(e);
			}
			
			twitterDateFormat = new SimpleDateFormat(
					"EEE MMM dd HH:mm:ss ZZZZZ yyyy", java.util.Locale.ENGLISH);
			
			tableName = context.getConfiguration().get(MAPRED_VERTICA_TABLE_NAME);
		}
	
		@Override
	    public void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException {
	
			if (record == null) {
				throw new IOException("No output record found");
			}
			
			JSONObject tweet_json = JsonParser.parseJsonFromString(value.toString());
			
			if(tableName.equals("tweet")) {
				
				Long tweet_id = (Long) tweet_json.get("id");
				String text = (String) tweet_json.get("text");
				String create_at = (String) tweet_json.get("created_at");
				
				try {
					
					
					Timestamp create_date = new Timestamp(twitterDateFormat.parse(create_at).getTime());

				
					JSONObject user_json = (JSONObject) tweet_json.get("user");
					Long user_id = (Long) user_json.get("id");
				
					record.set("created_at", create_date);
	
					record.setFromString("id", tweet_id.toString());
					record.setFromString("user_id", user_id.toString());
					record.setFromString("text", text);
				
					context.write(new Text(tableName), record);
					
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
					
				
			}
			else if(tableName.equals("entities")) {
				Long tweet_id = (Long) tweet_json.get("id");
				
				try {


				
					JSONObject entities_json = (JSONObject) tweet_json.get("entities");
					JSONArray hashtags = (JSONArray) entities_json.get("hashtags");
					
					if (hashtags.isEmpty()){
						record.setFromString("tweet_id", tweet_id.toString());
						record.setFromString("hashtag", "");
						context.write(new Text(tableName), record);
					}
					//String hashtags_str = hashtags.toString().substring(1,hashtags.toString().length()-1);
					else {
						for(Object tag : hashtags) {
							String tag_str = (String) tag;
							
							record.setFromString("tweet_id", tweet_id.toString());
							record.setFromString("hashtag", tag_str);
							context.write(new Text(tableName), record);
						}				
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			else if(tableName.equals("tweetjson")){
				try {
					record.setFromString("json", value.toString());
					context.write(new Text(tableName), record);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
		}
	}

}
