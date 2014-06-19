package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import java.sql.Connection
import java.sql.SQLException

import bigframe.workflows.Query
import bigframe.workflows.runnable.SharkRunnable
import bigframe.workflows.BaseTablePath

import scala.collection.JavaConversions._

class WF_ReportSaleSentimentShark(basePath: BaseTablePath, num_iter: Int, val useRC: Boolean) extends Query  with SharkRunnable{

	def printDescription(): Unit = {}
	
	/*
	 * Prepeare the basic tables before run the Shark query
	 */
	override def prepareSharkTables(connection: Connection): Unit = {
		val tablePreparator = new PrepareTable_Hive(basePath)
		
		if(useRC)
			tablePreparator.prepareTableRCFile(connection)
		else
			tablePreparator.prepareTableTextFile(connection)
	}
	
	def cleanUpSharkImpl1(connection: Connection): Unit = {
		
		val stmt = connection.createStatement()	
		
		val list_drop = Seq("DROP TABLE IF EXISTS promotionSelected_cached",
							"DROP VIEW IF EXISTS promotedProduct",
							"DROP VIEW IF EXISTS RptSalesByProdCmpn",
							"DROP TABLE IF EXISTS relevantTweet_cached",
							"DROP VIEW IF EXISTS senAnalyse",
							"DROP TABLE IF EXISTS tweetByUser_cached",
							"DROP TABLE IF EXISTS tweetByProd_cached",
							"DROP VIEW IF EXISTS sumFriendTweets",
							"DROP TABLE IF EXISTS mentionProb_cached",
							"DROP VIEW IF EXISTS simUserByProd",
							"DROP TABLE IF EXISTS transitMatrix_cached",
							"DROP TABLE IF EXISTS randSuffVec_cached",
							"DROP TABLE IF EXISTS initialRank_cached")

		
		list_drop.foreach(stmt.execute(_))
									
		for(iteration <- 1 to num_iter) {
							
			val drop_twitterRank = "DROP TABLE IF EXISTS twitterRank"+iteration+"_cached"

			stmt.execute(drop_twitterRank)
				
		}
	}


	override def cleanUpShark(connection: Connection): Unit = {
		cleanUpSharkImpl1(connection)
	}
	
	
	def runSharkImpl1(connection: Connection) : Boolean = {
			
		try {
			val stmt = connection.createStatement();			
						
//			val lower = 1
//			val upper = 300
			
			
			val drop_promotionSelected = "DROP TABLE IF EXISTS promotionSelected_cached"
			val create_promotionSelected = "CREATE TABLE promotionSelected_cached AS" +
					"	SELECT p_promo_id as promo_id, p_item_sk as item_sk, p_start_date_sk as start_date_sk, p_end_date_sk as end_date_sk " +
					"	FROM promotion " +
					"	WHERE p_item_sk IS NOT NULL AND p_start_date_sk IS NOT NULL AND p_end_date_sk IS NOT NULL"
			
			/**
			 * Choose all promotion except those contain NULL value.
			 */
			stmt.execute(drop_promotionSelected)
			stmt.execute(create_promotionSelected)

			
			val drop_promotedProduct = "DROP VIEW IF EXISTS promotedProduct"
			val create_promotedProduct = "CREATE VIEW promotedProduct (item_sk, product_name, start_date_sk, end_date_sk) AS" +
					"	SELECT i_item_sk, i_product_name, start_date_sk, end_date_sk " +
					"	FROM item JOIN promotionSelected_cached " +
					"	ON item.i_item_sk = promotionSelected_cached.item_sk" +
					"	WHERE i_product_name IS NOT NULL"
			stmt.execute(drop_promotedProduct)
			stmt.execute(create_promotedProduct)	
							
			
			val drop_RptSalesByProdCmpn = "DROP VIEW IF EXISTS RptSalesByProdCmpn"
			val create_RptSalesByProdCmpn = "CREATE VIEW RptSalesByProdCmpn (promo_id, item_sk, totalsales) AS" +
					"	SELECT promotionSelected_cached.promo_id, promotionSelected_cached.item_sk, sum(price*quantity) as totalsales " +
					"	FROM" + 
					"		(SELECT ws_sold_date_sk as sold_date_sk, ws_item_sk as item_sk, ws_sales_price as price, ws_quantity as quantity " +
					"		FROM web_sales " + 
					"		UNION ALL" + 
					"		SELECT ss_sold_date_sk as sold_date_sk, ss_item_sk as item_sk, ss_sales_price as price, ss_quantity as quantity "  +
					"		FROM store_sales" + 
					"		UNION ALL" + 
					"		SELECT cs_sold_date_sk as sold_date_sk, cs_item_sk as item_sk, cs_sales_price as price, cs_quantity as quantity" +
					"		FROM catalog_sales) sales"  +
					"		JOIN promotionSelected_cached "  +
					"		ON sales.item_sk = promotionSelected_cached.item_sk" +
					"	WHERE " + 
					"		promotionSelected_cached.start_date_sk <= sold_date_sk " +
					"		AND" + 
					"		sold_date_sk <= promotionSelected_cached.end_date_sk" +
					"	GROUP BY" + 
					"		promotionSelected_cached.promo_id, promotionSelected_cached.item_sk "
			stmt.execute(drop_RptSalesByProdCmpn)			
			stmt.execute(create_RptSalesByProdCmpn)
			
			val drop_relevantTweet = "DROP TABLE IF EXISTS relevantTweet_cached"
			val create_relevantTweet = "CREATE TABLE relevantTweet_cached AS" +
					"		SELECT item_sk, user_id, text" +
					"		FROM " +
					"			(SELECT user.id as user_id, text, created_at, entities.hashtags[0] as hashtag" +
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
			
			
			stmt.execute(drop_relevantTweet)
			stmt.execute(create_relevantTweet)
			

			val drop_senAnalyse = "DROP VIEW IF EXISTS senAnalyse"
			val create_senAnalyse = "CREATE VIEW senAnalyse" +
					"	(item_sk, user_id, sentiment_score) AS" +
					"	SELECT item_sk, user_id, sum(sentiment(text)) as sum_score" +
					"	FROM relevantTweet_cached" +
					"	GROUP BY" +
					"	item_sk, user_id"
			
			stmt.execute(drop_senAnalyse)
			stmt.execute(create_senAnalyse)
			
			
			val drop_tweetByUser = "DROP TABLE IF EXISTS tweetByUser_cached"
			val create_tweetByUser = "CREATE TABLE tweetByUser_cached AS" +
					"	SELECT user_id, count(*) as num_tweets" +
					"	FROM relevantTweet_cached" +
					"	GROUP BY" +
					"	user_id"
				
			
			stmt.execute(drop_tweetByUser)
			stmt.execute(create_tweetByUser)
					
			val drop_tweetByProd = "DROP TABLE IF EXISTS tweetByProd_cached"
			val create_tweetByProd = "CREATE TABLE tweetByProd_cached AS" + 
					"	SELECT item_sk, count(*) as num_tweets" +
					"	FROM relevantTweet_cached" +
					"	GROUP BY" +
					"	item_sk"
				
																																																																																																																		
			stmt.execute(drop_tweetByProd)
			stmt.execute(create_tweetByProd)

			val drop_cachedTwitterGraph = "DROP TABLE IF EXISTS twitter_graph_cached"
			val create_cachedTwitterGraph = "CREATE TABLE twitter_graph_cached AS SELECT * from twitter_graph"
				
			stmt.execute(drop_cachedTwitterGraph)				
			stmt.execute(create_cachedTwitterGraph)
			
							
			val drop_sumFriendTweets = "DROP VIEW IF EXISTS sumFriendTweets"
			val create_sumFriendTweets = "CREATE VIEW sumFriendTweets (follower_id, num_friend_tweets) AS" +
					"	SELECT user_id, " +
					"		CASE WHEN num_friend_tweets > 0 THEN num_friend_tweets" +
					"			 ELSE 0L" +
					"		END" +
					"	FROM" +
					"		(SELECT user_id, sum(friend_tweets) as num_friend_tweets" +
					"		FROM tweetByUser_cached LEFT OUTER JOIN" +
					"			(SELECT follower_id, friend_id, num_tweets as friend_tweets" +
					"			FROM tweetByUser_cached JOIN twitter_graph_cached" +
					"	 		ON tweetByUser_cached.user_id = twitter_graph_cached.friend_id) f" +
					"		ON tweetByUser_cached.user_id = f.follower_id" +
					"		GROUP BY " +
					"		user_id) result"
				
			stmt.execute(drop_sumFriendTweets)
			stmt.execute(create_sumFriendTweets)
			
			
			val drop_mentionProb = "DROP TABLE IF EXISTS mentionProb_cached"
			val create_mentionProb = "CREATE TABLE mentionProb_cached AS" + 
					"	SELECT item_sk, tweetByUser_cached.user_id as user_id, r.num_tweets/tweetByUser_cached.num_tweets as prob" +
					"	FROM tweetByUser_cached JOIN " +
					"		(SELECT item_sk, user_id, count(*) as num_tweets" +
					"		FROM relevantTweet_cached" +
					"		GROUP BY" +
					"		item_sk, user_id) r" +
					"	ON tweetByUser_cached.user_id = r.user_id"
				
			stmt.execute(drop_mentionProb)
			stmt.execute(create_mentionProb)
			
			val drop_simUserByProd = "DROP VIEW IF EXISTS simUserByProd"
			val create_simUserByProd = "CREATE VIEW simUserByProd " +
					"	(item_sk, follower_id, friend_id, similarity) AS" +
					"	SELECT f.item_sk, follower_id, friend_id, (1 - ABS(follower_prob - prob)) as similarity" +
					"	FROM " +
					"		(SELECT item_sk, follower_id, friend_id, prob as follower_prob" +
					"		FROM mentionProb_cached JOIN twitter_graph_cached " +
					"		ON mentionProb_cached.user_id = twitter_graph_cached.follower_id) f" +
					"	JOIN mentionProb_cached " +
					"	ON	f.friend_id = mentionProb_cached.user_id AND f.item_sk=mentionProb_cached.item_sk"
		

			
			stmt.execute(drop_simUserByProd)
			stmt.execute(create_simUserByProd)
					
					

			val drop_transitMatrix = "DROP TABLE IF EXISTS transitMatrix_cached"
			val create_transitMatrix = "CREATE TABLE transitMatrix_cached AS" +
					"	SELECT item_sk, follower_id, friend_id, " +
					"		CASE WHEN follower_id != friend_id THEN num_tweets/num_friend_tweets*similarity" +
					"			 ELSE 0.0 " +
					"		END AS transit_prob" +
					"	FROM" +
					"		(SELECT item_sk, t1.follower_id, friend_id, similarity, num_friend_tweets" +
					"		FROM simUserByProd t1 JOIN sumFriendTweets t2" +
					"		ON t1.follower_id = t2.follower_id) t3" +
					"	JOIN tweetByUser_cached" +
					"	ON t3.friend_id = tweetByUser_cached.user_id"
				
			stmt.execute(drop_transitMatrix)
			stmt.execute(create_transitMatrix)
			
			val drop_randSufferVec = "DROP TABLE IF EXISTS randSuffVec_cached"
			val create_randSuffVec = "CREATE TABLE randSuffVec_cached AS" +
					"	SELECT t1.item_sk as item_sk, user_id, t1.num_tweets/t2.num_tweets as prob" +
					"	FROM" +
					"		(SELECT item_sk, user_id, count(*) as num_tweets" +
					"		FROM relevantTweet_cached" +
					"		GROUP BY" +
					"		item_sk, user_id) t1" +
					"	JOIN tweetByProd_cached t2" +
					"	ON t1.item_sk = t2.item_sk"
				
			stmt.execute(drop_randSufferVec)
			stmt.execute(create_randSuffVec)
			
			val drop_initalRank = "DROP TABLE IF EXISTS initialRank_cached"
			val create_initialRank = "CREATE TABLE initialRank_cached AS" +
					"	SELECT t2.item_sk as item_sk, user_id, 1.0/num_users as rank_score" +
					"	FROM " +
					"		(SELECT item_sk, count(*) as num_users" +
					"		FROM" +
					"			(SELECT item_sk, user_id" +
					"			FROM relevantTweet_cached" +
					"			GROUP BY" +
					"			item_sk, user_id) t1" +
					"		GROUP BY" +
					"		item_sk) t2 JOIN mentionProb_cached" +
					"	ON t2.item_sk = mentionProb_cached.item_sk"
				
				
			stmt.execute(drop_initalRank)
			stmt.execute(create_initialRank)
					
			val alpha = 0.85
			for(iteration <- 1 to num_iter) {
						
				val twitterRank_previous = if(iteration == 1) "initialRank_cached" else "twitterRank"+(iteration-1)+"_cached"	
				val drop_twitterRank = "DROP TABLE IF EXISTS twitterRank"+iteration+"_cached"
				val create_twitterRank = "CREATE TABLE twitterRank"+iteration+"_cached AS" +
						"	SELECT t4.item_sk as item_sk, t4.user_id as user_id, " +
						"		CASE WHEN sum_follower_score > 0 THEN " + alpha + " * sum_follower_score + " + (1-alpha) +" * prob" +
						"			 ELSE " + (1-alpha) +" * prob" +
						"		END as rank_score" +
						"	FROM" +
						"		(SELECT t1.item_sk, follower_id, sum(transit_prob * rank_score) as sum_follower_score" +
						"		FROM transitMatrix_cached t1 JOIN " + twitterRank_previous +" t2" +
						"		ON t1.friend_id = t2.user_id AND t1.item_sk = t2.item_sk " +
						"		GROUP BY " +
						"		t1.item_sk, follower_id) t3" +
						"	RIGHT OUTER JOIN randSUffVec_cached t4" +
						"	ON t3.item_sk = t4.item_sk AND t3.follower_id = t4.user_id"
				
			
				stmt.execute(drop_twitterRank)
				stmt.execute(create_twitterRank)
				
			}
			
			val drop_RptSAProdCmpn = "DROP TABLE IF EXISTS RptSAProdCmpn"
			val create_RptSAProdCmpn = "CREATE TABLE RptSAProdCmpn (promo_id string, item_sk int, totalsales float , total_sentiment float)"
					
			stmt.execute(drop_RptSAProdCmpn)
			stmt.execute(create_RptSAProdCmpn)
			
			val twitterRank = "twitterRank" + num_iter +"_cached"
			
			val query_RptSAProdCmpn = "INSERT INTO TABLE RptSAProdCmpn" +
					"	SELECT promo_id, t.item_sk, totalsales, total_sentiment" +
					"	FROM " +
					"		(SELECT senAnalyse.item_sk, sum(rank_score * sentiment_score) as total_sentiment" +
					"		FROM senAnalyse JOIN " + twitterRank +
					"		ON senAnalyse.item_sk = " + twitterRank + ".item_sk " +
					"		WHERE senAnalyse.user_id = " + twitterRank +".user_id" +
					"		GROUP BY" +
					"		senAnalyse.item_sk) t" +
					"	JOIN RptSalesByProdCmpn " +
					"	ON t.item_sk = RptSalesByProdCmpn.item_sk"				
			
			if (stmt.execute(query_RptSAProdCmpn)) {
				stmt.close();
				return true
			}
			else{ 
				stmt.close();
				return false
			}

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
	override def runShark(connection: Connection): Boolean = {
		
			return runSharkImpl1(connection)

	}
	
		
}