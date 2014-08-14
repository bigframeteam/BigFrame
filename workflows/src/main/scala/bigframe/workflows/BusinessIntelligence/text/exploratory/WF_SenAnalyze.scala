/**
 *
 */
package bigframe.workflows.BusinessIntelligence.text.exploratory

import java.sql.Connection
import java.util.concurrent.{Executors, ExecutorService}
import java.util.concurrent.Future
import bigframe.workflows.Query
import bigframe.workflows.runnable.HadoopRunnable
import bigframe.workflows.runnable.HiveRunnable
import bigframe.workflows.BaseTablePath
import org.apache.hadoop.conf.Configuration
import bigframe.workflows.events.BigFrameListenerBus

/**
 * A Class can run sentiment analysis on different engines.
 * 
 * @author andy
 *
 */
class WF_SenAnalyze(basePath : BaseTablePath) extends Query with HadoopRunnable 
			with HiveRunnable {

	val tweet_dir = basePath.nested_path
	
	def printDescription(): Unit = {}

	/**
	 * Use a executor to submit the query. It is useful when
	 * the query is a complex DAG.
	 */
	override def runHadoop(mapred_config: Configuration): java.lang.Boolean = {
		val SenAnalyzeHadoop = new SenAnalyzeHadoop(tweet_dir, mapred_config)
		
		val pool: ExecutorService = Executors.newFixedThreadPool(1)
				
		val future = pool.submit(SenAnalyzeHadoop)
		pool.shutdown()
		true
		//if(future.get()) true else false
	}
	
		/*
	 * Prepeare the basic tables before run the Hive query
	 */
	override def prepareHiveTables(connection: Connection): Unit = {
		
		val tweets_HDFSPath = basePath.nested_path
		
		val stmt = connection.createStatement();
		
		val dropTweets = "DROP TABLE tweets"
		val dropSenAnalyse = "DROP TABLE senAnalyse"
			
		stmt.execute(dropTweets)	
		stmt.execute(dropSenAnalyse)	
		/**
		 * create json format table
		 */
		
		val createSenAnalyse = "CREATE EXTERNAL TABLE senAnalyse (product_name string, user_id int, sentiment float)"
				
		val createTweets = "create external table tweets" +
				"	(" +
				"		contributors string," +
				"		coordinates string," +
				"		created_at string," +
				"		entities struct<hashtags:array<string>, " +
				"						urls:array<struct<expanded_url:string, " +
				"											indices:array<int>, " +
				"											url:string>>, " +
				"						user_mentions:array<struct<id:string," +
				"													id_str:string, " +
				"													indices:array<int>, " +
				"													name:string, " +
				"													screen_name:string>>>," +
				"		favorited string," +
				"		geo string," +
				"		id int," +
				"		id_str string," +
				"		in_reply_to_screen_name string," +
				"		in_reply_to_status_id string," +
				"		in_reply_to_status_id_str string," +
				"		in_reply_to_user_id string," +
				"		in_reply_to_user_id_str string," +
				"		place string," +
				"		retweet_count string," +
				"		retweeted string," +
				"		source string," +
				"		text string," +
				"		truncated string," +
				"		user struct<contributors_enabled:string, " +
				"					created_at:string, " +
				"					description:string, " +
				"					favourites_count:string, " +
				"					follow_request_sent:string, " +
				"					followers_count:string, " +
				"					following:string, " +
				"					friends_count:string, " +
				"					geo_enabled:string, " +
				"					id:int, " +
				"					id_str:string, " +
				"					lang:string, " +
				"					listed_count:string, " +
				"					`location`:string, " +
				"					name:string, " +
				"					notifications:string, " +
				"					profile_background_color:string, " +
				"					profile_background_image_url:string, " +
				"					profile_background_tile:string," +
				"					profile_image_url:string," +
				" 					profile_link_color:string, " +
				"					profile_sidebar_border_color:string, " +
				"					profile_sidebar_fill_color:string, " +
				"					profile_text_color:string, " +
				"					profile_use_background_image:string, " +
				"					protected:string, screen_name:string, " +
				"					show_all_inline_media:string, " +
				"					statuses_count:string, " +
				"					time_zone:string, " +
				"					url:string, " +
				"					utc_offset:string, " +
				"					verified:string>" +
				"	)" +
				"	ROW FORMAT SERDE \'org.openx.data.jsonserde.JsonSerDe\'" +
				"	location \'" + tweets_HDFSPath +"\'"
				
			stmt.execute(createTweets)
			stmt.execute(createSenAnalyse)
			
			stmt.execute("create temporary function sentiment as \'bigframe.workflows.util.SenExtractorHive\'")
			
			
	}
	
	/**
	 * Run the benchmark query
	 */
	override def runHive(connection: Connection, 
	    eventBus: BigFrameListenerBus): Boolean = {
		
		val stmt = connection.createStatement();
		
	
		
		val querySenAnalyse = "INSERT INTO TABLE senAnalyse" +
				"	SELECT entities.hashtags[0], user.id, sum(sentiment(text))" +
				"	FROM tweets" +
				"	WHERE entities.hashtags[0] IS NOT NULL" +
				"	GROUP BY" +
				"	entities.hashtags[0], user.id" 
			
		return stmt.execute(querySenAnalyse)
		
	}
	
	override def cleanUpHive(connection: Connection): Unit = {}
}