package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import java.sql.Connection
import java.sql.SQLException
import bigframe.workflows.Query
import bigframe.workflows.runnable.HiveRunnable
import bigframe.workflows.BaseTablePath
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConversions._
import bigframe.workflows.events.BigFrameListenerBus
import bigframe.workflows.events.WorkflowStartedEvent
import bigframe.workflows.events.WorkflowCompletedEvent
import bigframe.workflows.events.ComponentStartedEvent
import bigframe.workflows.events.ComponentCompletedEvent
import bigframe.workflows.events.QueryStartedEvent
import bigframe.workflows.events.QueryCompletedEvent

class WF_ReportSaleSentimentHive(basePath: BaseTablePath, num_iter: Int, val useOrc: Boolean) extends Query  with HiveRunnable{

	def printDescription(): Unit = {}

	
	/*
	 * Prepeare the basic tables before run the Hive query
	 */
	override def prepareHiveTables(connection: Connection): Unit = {
		val tablePreparator = new PrepareTable_Hive(basePath)
		
	    if(useOrc == true) {
   		    tablePreparator.prepareTableImpl2(connection)
	    }
	    else {
	        tablePreparator.prepareTableImpl1(connection)
	    }
	}
	
	def cleanUpHiveImpl1(connection: Connection): Unit = {
		
		val stmt = connection.createStatement()	
		
		val list_drop = Seq("DROP TABLE IF EXISTS promotionSelected",
							"DROP VIEW IF EXISTS promotedProduct",
							"DROP VIEW IF EXISTS RptSalesByProdCmpn",
							"DROP TABLE IF EXISTS relevantTweet",
							"DROP VIEW IF EXISTS senAnalyse",
							"DROP TABLE IF EXISTS tweetByUser",
							"DROP TABLE IF EXISTS tweetByProd",
							"DROP VIEW IF EXISTS sumFriendTweets",
							"DROP TABLE IF EXISTS mentionProb",
							"DROP VIEW IF EXISTS simUserByProd",
							"DROP TABLE IF EXISTS transitMatrix",
							"DROP TABLE IF EXISTS randSuffVec",
							"DROP TABLE IF EXISTS initialRank")
		
		list_drop.foreach(stmt.execute(_))
									
		for(iteration <- 1 to num_iter) {
							
			val drop_twitterRank = "DROP TABLE IF EXISTS twitterRank"+iteration

			stmt.execute(drop_twitterRank)
				
		}
	}
	
	override def cleanUpHive(connection: Connection): Unit = {
		//cleanUpHiveImpl1(connection)
	}
	
	
	def runHiveImpl1(connection: Connection, 
	    eventBus: BigFrameListenerBus) : Boolean = {

	  eventBus.post(new WorkflowStartedEvent("Hive"));
			
		try {
			val stmt = connection.createStatement();			
								
			eventBus.post(new ComponentStartedEvent("filter promotions", "hive"))

			val drop_promotionSelected = "DROP TABLE IF EXISTS promotionSelected"
			val create_promotionSelected = "CREATE TABLE promotionSelected (promo_id string, item_sk int," +
					"start_date_sk int, end_date_sk int)"
			
			/**
			 * Choose all promotion except those contain NULL value.
			 */
			val query_promotionSelected = "INSERT INTO TABLE promotionSelected" +
					"	SELECT p_promo_id, p_item_sk, p_start_date_sk, p_end_date_sk " +
					"	FROM promotion " +
					"	WHERE p_item_sk IS NOT NULL AND p_start_date_sk IS NOT NULL AND p_end_date_sk IS NOT NULL"

			eventBus.post(new QueryStartedEvent(query_promotionSelected, "hive"))

			stmt.execute(drop_promotionSelected)
			stmt.execute(create_promotionSelected)
			stmt.execute(query_promotionSelected)

			eventBus.post(new QueryCompletedEvent(query_promotionSelected, "hive"))
			eventBus.post(new ComponentCompletedEvent("filter promotions", "hive"))
			eventBus.post(new ComponentStartedEvent("filter products", "hive"))
			
			val drop_promotedProduct = "DROP VIEW IF EXISTS promotedProduct"
			val drop_promotedProduct_t = "DROP TABLE IF EXISTS promotedProduct"
			val create_promotedProduct = "CREATE VIEW promotedProduct (item_sk, product_name, start_date_sk, end_date_sk) AS" +
					"	SELECT i_item_sk, i_product_name, start_date_sk, end_date_sk " +
					"	FROM item JOIN promotionSelected " +
					"	ON item.i_item_sk = promotionSelected.item_sk" +
					"	WHERE i_product_name IS NOT NULL"

			eventBus.post(new QueryStartedEvent(create_promotedProduct, "hive"))

			stmt.execute(drop_promotedProduct)
			stmt.execute(drop_promotedProduct_t)
			stmt.execute(create_promotedProduct)	
							
			eventBus.post(new QueryCompletedEvent(create_promotedProduct, "hive"))
			eventBus.post(new ComponentCompletedEvent("filter products", "hive"))
			eventBus.post(new ComponentStartedEvent("sales report", "hive"))
        			
			val drop_RptSalesByProdCmpn = "DROP VIEW IF EXISTS RptSalesByProdCmpn"
			val drop_RptSalesByProdCmpn_t = "DROP TABLE IF EXISTS RptSalesByProdCmpn"
			val create_RptSalesByProdCmpn = "CREATE VIEW RptSalesByProdCmpn (promo_id, item_sk, totalsales) AS" +
					"	SELECT promotionSelected.promo_id, promotionSelected.item_sk, sum(price*quantity) as totalsales " +
					"	FROM" + 
					"		(SELECT ws_sold_date_sk as sold_date_sk, ws_item_sk as item_sk, ws_sales_price as price, ws_quantity as quantity " +
					"		FROM web_sales " + 
					"		UNION ALL" + 
					"		SELECT ss_sold_date_sk as sold_date_sk, ss_item_sk as item_sk, ss_sales_price as price, ss_quantity as quantity "  +
					"		FROM store_sales" + 
					"		UNION ALL" + 
					"		SELECT cs_sold_date_sk as sold_date_sk, cs_item_sk as item_sk, cs_sales_price as price, cs_quantity as quantity" +
					"		FROM catalog_sales) sales"  +
					"		JOIN promotionSelected "  +
					"		ON sales.item_sk = promotionSelected.item_sk" +
					"	WHERE " + 
					"		promotionSelected.start_date_sk <= sold_date_sk " +
					"		AND" + 
					"		sold_date_sk <= promotionSelected.end_date_sk" +
					"	GROUP BY" + 
					"		promotionSelected.promo_id, promotionSelected.item_sk "

			eventBus.post(new QueryStartedEvent(create_RptSalesByProdCmpn, "hive"))

			stmt.execute(drop_RptSalesByProdCmpn)		
			stmt.execute(drop_RptSalesByProdCmpn_t)	
			stmt.execute(create_RptSalesByProdCmpn)
			
			eventBus.post(new QueryCompletedEvent(create_RptSalesByProdCmpn, "hive"))
			eventBus.post(new ComponentCompletedEvent("sales report", "hive"))
			eventBus.post(new ComponentStartedEvent("filter tweets", "hive"))
		
			val drop_relevantTweet = "DROP TABLE IF EXISTS relevantTweet"
			val create_relevantTweet = "CREATE TABLE relevantTweet" +
					"	(item_sk int, user_id int, text string)"
			
			val query_relevantTweet	= "	INSERT INTO TABLE relevantTweet" +
					"		SELECT item_sk, user_id, text" +
					"		FROM " +
					"			(SELECT `user`.id as user_id, text, created_at, entities.hashtags[0] as hashtag" +
					"			FROM tweets" +
					"			WHERE size(entities.hashtags) > 0 ) t1 " +
					"		JOIN " +	
					"			(SELECT item_sk, product_name, start_date, d_date as end_date" +
					"			FROM " +
					"				(SELECT item_sk, product_name, d_date as start_date, end_date_sk" +
					"				FROM promotedProduct JOIN date_dim" +
					"				ON promotedProduct.start_date_sk = date_dim.d_date_sk) t2 " +
					"			JOIN date_dim " +
					"			ON t2.end_date_sk = date_dim.d_date_sk) t3" +
					"		ON t1.hashtag = t3.product_name" +
					"		WHERE isWithinDate(created_at, start_date, end_date)"

			eventBus.post(new QueryStartedEvent(query_relevantTweet, "hive"))

			stmt.execute(drop_relevantTweet)
			stmt.execute(create_relevantTweet)
			stmt.execute(query_relevantTweet)
			
			eventBus.post(new QueryCompletedEvent(query_relevantTweet, "hive"))
			eventBus.post(new ComponentCompletedEvent("filter tweets", "hive"))
			eventBus.post(new ComponentStartedEvent("sentiment analysis", "hive"))

			val drop_senAnalyse = "DROP VIEW IF EXISTS senAnalyse"
			val drop_senAnalyse_t = "DROP TABLE IF EXISTS senAnalyse"
			val create_senAnalyse = "CREATE VIEW senAnalyse" +
					"	(item_sk, user_id, sentiment_score) AS" +
					"	SELECT item_sk, user_id, sum(sentiment(text)) as sum_score" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	item_sk, user_id"
			
			eventBus.post(new QueryStartedEvent(create_senAnalyse, "hive"))
		
			stmt.execute(drop_senAnalyse)
			stmt.execute(drop_senAnalyse_t)
			stmt.execute(create_senAnalyse)
			
			eventBus.post(new QueryCompletedEvent(create_senAnalyse, "hive"))
			eventBus.post(new ComponentCompletedEvent("sentiment analysis", "hive"))
			eventBus.post(new ComponentStartedEvent("transition matrices", "hive"))
			
			val drop_tweetByUser = "DROP TABLE IF EXISTS tweetByUser"
			val create_tweetByUser = "CREATE TABLE tweetByUser (user_id int, num_tweets int)"
				
			val query_tweetByUser =	"INSERT INTO TABLE tweetByUser" +
					"	SELECT user_id, count(*)" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	user_id"
			
			eventBus.post(new QueryStartedEvent(query_tweetByUser, "hive"))

			stmt.execute(drop_tweetByUser)
			stmt.execute(create_tweetByUser)
			stmt.execute(query_tweetByUser)
					
			eventBus.post(new QueryCompletedEvent(query_tweetByUser, "hive"))

			val drop_tweetByProd = "DROP TABLE IF EXISTS tweetByProd"
			val create_tweetByProd = "CREATE TABLE tweetByProd (item_sk int, num_tweets int)"
				
			val	query_tweetByProd =	"INSERT INTO TABLE tweetByProd" +
					"	SELECT item_sk, count(*)" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	item_sk"
																																																																																																																		
			eventBus.post(new QueryStartedEvent(query_tweetByProd, "hive"))

			stmt.execute(drop_tweetByProd)
			stmt.execute(create_tweetByProd)
			stmt.execute(query_tweetByProd)

			eventBus.post(new QueryCompletedEvent(query_tweetByProd, "hive"))
							
			val drop_sumFriendTweets = "DROP VIEW IF EXISTS sumFriendTweets"
			val drop_sumFriendTweets_t = "DROP TABLE IF EXISTS sumFriendTweets"
			val create_sumFriendTweets = "CREATE VIEW sumFriendTweets (follower_id, num_friend_tweets) AS" +
					"	SELECT user_id, " +
					"		CASE WHEN num_friend_tweets > 0 THEN num_friend_tweets" +
					"			 ELSE 0L" +
					"		END" +
					"	FROM" +
					"		(SELECT user_id, sum(friend_tweets) as num_friend_tweets" +
					"		FROM tweetByUser LEFT OUTER JOIN" +
					"			(SELECT follower_id, friend_id, num_tweets as friend_tweets" +
					"			FROM tweetByUser JOIN twitter_graph" +
					"	 		ON tweetByUser.user_id = twitter_graph.friend_id) f" +
					"		ON tweetByUser.user_id = f.follower_id" +
					"		GROUP BY " +
					"		user_id) result"
				
			eventBus.post(new QueryStartedEvent(create_sumFriendTweets, "hive"))

			stmt.execute(drop_sumFriendTweets)
			stmt.execute(drop_sumFriendTweets_t)
			stmt.execute(create_sumFriendTweets)
			
			eventBus.post(new QueryCompletedEvent(create_sumFriendTweets, "hive"))
			
			val drop_mentionProb = "DROP TABLE IF EXISTS mentionProb"
			val create_mentionProb = "CREATE TABLE mentionProb (item_sk int, user_id int, prob float)"
				
			val query_mentionProb =	"INSERT INTO TABLE mentionProb" +
					"	SELECT item_sk, tweetByUser.user_id, r.num_tweets/tweetByUser.num_tweets" +
					"	FROM tweetByUser JOIN " +
					"		(SELECT item_sk, user_id, count(*) as num_tweets" +
					"		FROM relevantTweet" +
					"		GROUP BY" +
					"		item_sk, user_id) r" +
					"	ON tweetByUser.user_id = r.user_id"
				
			eventBus.post(new QueryStartedEvent(query_mentionProb, "hive"))

			stmt.execute(drop_mentionProb)
			stmt.execute(create_mentionProb)
			stmt.execute(query_mentionProb)

			eventBus.post(new QueryCompletedEvent(query_mentionProb, "hive"))
			
			val drop_simUserByProd = "DROP VIEW IF EXISTS simUserByProd"
			val drop_simUserByProd_t = "DROP TABLE IF EXISTS simUserByProd"
			val create_simUserByProd = "CREATE VIEW simUserByProd " +
					"	(item_sk, follower_id, friend_id, similarity) AS" +
					"	SELECT f.item_sk, follower_id, friend_id, (1 - ABS(follower_prob - prob)) as similarity" +
					"	FROM " +
					"		(SELECT item_sk, follower_id, friend_id, prob as follower_prob" +
					"		FROM mentionProb JOIN twitter_graph " +
					"		ON mentionProb.user_id = twitter_graph.follower_id) f" +
					"	JOIN mentionProb " +
					"	ON	f.friend_id = mentionProb.user_id AND f.item_sk=mentionProb.item_sk"
		
			eventBus.post(new QueryStartedEvent(create_simUserByProd, "hive"))
			
			stmt.execute(drop_simUserByProd)
			stmt.execute(drop_simUserByProd_t)
			stmt.execute(create_simUserByProd)
					
			eventBus.post(new QueryCompletedEvent(create_simUserByProd, "hive"))		

			val drop_transitMatrix = "DROP TABLE IF EXISTS transitMatrix"
			val create_transitMatrix = "CREATE TABLE transitMatrix (item_sk int, follower_id int, friend_id int, transit_prob float)" 
				
			val query_transitMatrix = "INSERT INTO TABLE transitMatrix" +
					"	SELECT item_sk, follower_id, friend_id, " +
					"		CASE WHEN follower_id != friend_id THEN num_tweets/num_friend_tweets*similarity" +
					"			 ELSE 0.0" +
					"		END" +
					"	FROM" +
					"		(SELECT item_sk, t1.follower_id, friend_id, similarity, num_friend_tweets" +
					"		FROM simUserByProd t1 JOIN sumFriendTweets t2" +
					"		ON t1.follower_id = t2.follower_id) t3" +
					"	JOIN tweetByUser" +
					"	ON t3.friend_id = tweetByUser.user_id"
				
			eventBus.post(new QueryStartedEvent(query_transitMatrix, "hive"))

			stmt.execute(drop_transitMatrix)
			stmt.execute(create_transitMatrix)
			stmt.execute(query_transitMatrix)

			eventBus.post(new QueryCompletedEvent(query_transitMatrix, "hive"))
			
			val drop_randSufferVec = "DROP TABLE IF EXISTS randSuffVec"
			val create_randSuffVec = "CREATE TABLE randSuffVec (item_sk int, user_id int, prob float)" 
				
			val query_randSuffVec =	"INSERT INTO TABLE randSuffVec	" +
					"	SELECT t1.item_sk, user_id, t1.num_tweets/t2.num_tweets" +
					"	FROM" +
					"		(SELECT item_sk, user_id, count(*) as num_tweets" +
					"		FROM relevantTweet" +
					"		GROUP BY" +
					"		item_sk, user_id) t1" +
					"	JOIN tweetByProd t2" +
					"	ON t1.item_sk = t2.item_sk"
				
			eventBus.post(new QueryStartedEvent(query_randSuffVec, "hive"))

			stmt.execute(drop_randSufferVec)
			stmt.execute(create_randSuffVec)
			stmt.execute(query_randSuffVec)
			
			eventBus.post(new QueryCompletedEvent(query_randSuffVec, "hive"))
			eventBus.post(new ComponentCompletedEvent("transition matrices", "hive"))
			eventBus.post(new ComponentStartedEvent("twitter rank", "hive"))

			val drop_initalRank = "DROP TABLE IF EXISTS initialRank"
			val create_initialRank = "CREATE TABLE initialRank (item_sk int, user_id int, rank_score float)" 
				
			val query_initialRank =	"INSERT INTO TABLE initialRank" +
					"	SELECT t2.item_sk, user_id, 1.0/num_users as rank_score" +
					"	FROM " +
					"		(SELECT item_sk, count(*) as num_users" +
					"		FROM" +
					"			(SELECT item_sk, user_id" +
					"			FROM relevantTweet" +
					"			GROUP BY" +
					"			item_sk, user_id) t1" +
					"		GROUP BY" +
					"		item_sk) t2 JOIN mentionProb" +
					"	ON t2.item_sk = mentionProb.item_sk"
				
			eventBus.post(new QueryStartedEvent(query_initialRank, "hive"))
			
			stmt.execute(drop_initalRank)
			stmt.execute(create_initialRank)
			stmt.execute(query_initialRank)
					
			eventBus.post(new QueryCompletedEvent(query_initialRank, "hive"))

			val alpha = 0.85
			for(iteration <- 1 to num_iter) {
						
				val twitterRank_previous = if(iteration == 1) "initialRank" else "twitterRank"+(iteration-1)	
				val drop_twitterRank = "DROP TABLE IF EXISTS twitterRank"+iteration
				val create_twitterRank = "CREATE TABLE twitterRank"+iteration+" (item_sk int, user_id int, rank_score float)"
				
				val query_twitterRank =	"INSERT INTO TABLE twitterRank"+iteration +
						"	SELECT t4.item_sk, t4.user_id, " +
						"		CASE WHEN sum_follower_score > 0 THEN " + alpha + " * sum_follower_score + " + (1-alpha) +" * prob" +
						"			 ELSE " + (1-alpha) +" * prob" +
						"		END" +
						"	FROM" +
						"		(SELECT t1.item_sk, follower_id, sum(transit_prob * rank_score) as sum_follower_score" +
						"		FROM transitMatrix t1 JOIN " + twitterRank_previous +" t2" +
						"		ON t1.friend_id = t2.user_id AND t1.item_sk = t2.item_sk " +
						"		GROUP BY " +
						"		t1.item_sk, follower_id) t3" +
						"	RIGHT OUTER JOIN randSUffVec t4" +
						"	ON t3.item_sk = t4.item_sk AND t3.follower_id = t4.user_id"
			
				eventBus.post(new QueryStartedEvent(query_twitterRank, "hive"))

				stmt.execute(drop_twitterRank)
				stmt.execute(create_twitterRank)
				stmt.execute(query_twitterRank)

				eventBus.post(new QueryCompletedEvent(query_twitterRank, "hive"))
			}
			
			eventBus.post(new ComponentCompletedEvent("twitter rank", "hive"))
			eventBus.post(new ComponentStartedEvent("report generation", "hive"))
			
			val drop_RptSAProdCmpn = "DROP TABLE IF EXISTS RptSAProdCmpn"
			val create_RptSAProdCmpn = "CREATE TABLE RptSAProdCmpn (promo_id string, item_sk int, totalsales float , total_sentiment float)"
					
			stmt.execute(drop_RptSAProdCmpn)
			stmt.execute(create_RptSAProdCmpn)
			
			val twitterRank = "twitterRank" + num_iter
			
			val query_RptSAProdCmpn = "INSERT INTO TABLE RptSAProdCmpn" +
					"	SELECT promo_id, t.item_sk, totalsales, total_sentiment" +
					"	FROM " +
					"		(SELECT senAnalyse.item_sk, sum(rank_score * sentiment_score) as total_sentiment" +
					"		FROM senAnalyse JOIN " + twitterRank +
					"		ON senAnalyse.item_sk = " + twitterRank + ".item_sk " +
					"		AND senAnalyse.user_id = " + twitterRank +".user_id" +
					"		GROUP BY" +
					"		senAnalyse.item_sk) t" +
					"	JOIN RptSalesByProdCmpn " +
					"	ON t.item_sk = RptSalesByProdCmpn.item_sk"				

			eventBus.post(new QueryStartedEvent(query_RptSAProdCmpn, "hive"))
      
//      try {
//        stmt.executeQuery(query_RptSAProdCmpn)
//      }
//      catch {
//        case sqle :
//        SQLException => sqle.printStackTrace()
//        case e :
//        Exception => e.printStackTrace()
//      }
			
			stmt.execute(query_RptSAProdCmpn)
				
      eventBus.post(new QueryCompletedEvent(query_RptSAProdCmpn, "hive"))
			eventBus.post(new ComponentCompletedEvent("report generation", "hive"))
			eventBus.post(new WorkflowCompletedEvent("Hive"));
			
      stmt.close();
			
      return true
		} catch {
			case sqle :
				SQLException => sqle.printStackTrace()
			case e :
				Exception => e.printStackTrace()
		} 
		
		return false
	}
	
	
	/**
	 * Run the benchmark query
	 */
	override def runHive(connection: Connection, 
	    eventBus: BigFrameListenerBus): Boolean = {
		
			return runHiveImpl1(connection, eventBus)

	}
	
		
}
