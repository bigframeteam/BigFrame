package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import bigframe.workflows.runnable.ImpalaHiveRunnable
import bigframe.workflows.Query
import bigframe.workflows.BaseTablePath
import bigframe.util.Constants

import java.sql.Connection
import java.sql.SQLException


import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

class WF_ReportSaleSentimentImpalaHive(basePath: BaseTablePath, num_iter: Int = 10, isUseORC: Boolean, 
		isBaseTableCompressed: Boolean, fileFormat: String) extends Query with ImpalaHiveRunnable {

		val LOG  = LogFactory.getLog(classOf[WF_ReportSaleSentimentImpalaHive]);
	
	def prepareImpalaHiveTables(impala_connect: Connection, hive_connect: Connection): Unit = {
		val preparor = new PrepareTable_ImpalaHive(basePath)
		
		if(fileFormat == Constants.ORC_FORMAT)
			preparor.prepareTableORCFileFormat(impala_connect, hive_connect, isBaseTableCompressed)
		else if(fileFormat == Constants.PARQUET_FORMAT)
			preparor.prepareTableParquetFormat(impala_connect, hive_connect, isBaseTableCompressed)
		else if(fileFormat == Constants.MIXED_FORMAT)
			preparor.prepareTableMixedParquetORCFormat(impala_connect, hive_connect, isBaseTableCompressed)
		else if(fileFormat == Constants.TEXT_FORMAT)
			preparor.prepareTableTextFileFormat(impala_connect, hive_connect)
		
	}

	def runImpalaHiveImpl1(impala_connect: Connection, hive_connect: Connection): Boolean = { 
		
		try {
			val hive_stmt = hive_connect.createStatement()
			val impala_stmt = impala_connect.createStatement()
						
//			impala_stmt.execute("SET hive.exec.compress.output=false")
			
//			val lower = 1
//			val upper = 300
			
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
			impala_stmt.execute(drop_promotionSelected)
			impala_stmt.execute(create_promotionSelected)
			impala_stmt.execute(query_promotionSelected)

			
			val drop_promotedProduct = "DROP VIEW IF EXISTS promotedProduct"
			val create_promotedProduct = "CREATE VIEW promotedProduct (item_sk, product_name, start_date_sk, end_date_sk) AS" +
					"	SELECT i_item_sk, i_product_name, start_date_sk, end_date_sk " +
					"	FROM item JOIN promotionSelected " +
					"	ON item.i_item_sk = promotionSelected.item_sk" +
					"	WHERE i_product_name IS NOT NULL"
			impala_stmt.execute(drop_promotedProduct)
			impala_stmt.execute(create_promotedProduct)	
							
			
			val drop_RptSalesByProdCmpn = "DROP VIEW IF EXISTS RptSalesByProdCmpn"
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
			impala_stmt.execute(drop_RptSalesByProdCmpn)			
			impala_stmt.execute(create_RptSalesByProdCmpn)
			
			LOG.info("Hive Execution: Get Relevant Tweets Begin")
			
			val drop_relevantTweet = "DROP TABLE IF EXISTS relevantTweet"
			val create_relevantTweet = "CREATE TABLE relevantTweet" +
					"	(item_sk int, user_id int, text string)"
			
			val query_relevantTweet	= "	INSERT INTO TABLE relevantTweet" +
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
			
			hive_stmt.execute(drop_relevantTweet)
			hive_stmt.execute(create_relevantTweet)
			hive_stmt.execute(query_relevantTweet)
			
			LOG.info("Hive Execution: Get Relevant Tweets End")
			
			
			LOG.info("Hive Execution: Get Sentiment Begin")
			val drop_senAnalyse = "DROP TABLE IF EXISTS senAnalyse"
			val create_senAnalyse = "CREATE TABLE senAnalyse AS" +
					"	SELECT item_sk, user_id, sum(sentiment(text)) as sentiment_score" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	item_sk, user_id"
			
			hive_stmt.execute(drop_senAnalyse)
			hive_stmt.execute(create_senAnalyse)
			LOG.info("Hive Execution: Get Sentiment End")
			
			
			/**
			 * Need to refresh the metastore to pick up tables created by Hive
			 */
			impala_stmt.execute("invalidate metadata")
			
			val drop_tweetByUser = "DROP TABLE IF EXISTS tweetByUser"
			val create_tweetByUser = "CREATE TABLE tweetByUser (user_id int, num_tweets int)"
				
			val query_tweetByUser =	"INSERT INTO TABLE tweetByUser" +
					"	SELECT user_id, count(*)" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	user_id"
			
			impala_stmt.execute(drop_tweetByUser)
			impala_stmt.execute(create_tweetByUser)
			impala_stmt.execute(query_tweetByUser)
					
			val drop_tweetByProd = "DROP TABLE IF EXISTS tweetByProd"
			val create_tweetByProd = "CREATE TABLE tweetByProd (item_sk int, num_tweets int)"
				
			val	query_tweetByProd =	"INSERT INTO TABLE tweetByProd" +
					"	SELECT item_sk, count(*)" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	item_sk"
																																																																																																																		
			impala_stmt.execute(drop_tweetByProd)
			impala_stmt.execute(create_tweetByProd)
			impala_stmt.execute(query_tweetByProd)

							
			val drop_sumFriendTweets = "DROP VIEW IF EXISTS sumFriendTweets"
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
				
			impala_stmt.execute(drop_sumFriendTweets)
			impala_stmt.execute(create_sumFriendTweets)
			
			
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
				
			impala_stmt.execute(drop_mentionProb)
			impala_stmt.execute(create_mentionProb)
			impala_stmt.execute(query_mentionProb)

			
			val drop_simUserByProd = "DROP VIEW IF EXISTS simUserByProd"
			val create_simUserByProd = "CREATE VIEW simUserByProd " +
					"	(item_sk, follower_id, friend_id, similarity) AS" +
					"	SELECT f.item_sk, follower_id, friend_id, (1 - ABS(follower_prob - prob)) as similarity" +
					"	FROM " +
					"		(SELECT item_sk, follower_id, friend_id, prob as follower_prob" +
					"		FROM mentionProb JOIN twitter_graph " +
					"		ON mentionProb.user_id = twitter_graph.follower_id) f" +
					"	JOIN mentionProb " +
					"	ON	f.friend_id = mentionProb.user_id AND f.item_sk=mentionProb.item_sk"
		

			
			impala_stmt.execute(drop_simUserByProd)
			impala_stmt.execute(create_simUserByProd)
					
					

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
				
			impala_stmt.execute(drop_transitMatrix)
			impala_stmt.execute(create_transitMatrix)
			impala_stmt.execute(query_transitMatrix)
			
			
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
				
			impala_stmt.execute(drop_randSufferVec)
			impala_stmt.execute(create_randSuffVec)
			impala_stmt.execute(query_randSuffVec)
			
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
				
			impala_stmt.execute(drop_initalRank)
			impala_stmt.execute(create_initialRank)
			impala_stmt.execute(query_initialRank)
					
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
			
				impala_stmt.execute(drop_twitterRank)
				impala_stmt.execute(create_twitterRank)
				impala_stmt.execute(query_twitterRank)
				
			}
			
			val drop_RptSAProdCmpn = "DROP TABLE IF EXISTS RptSAProdCmpn"
			val create_RptSAProdCmpn = "CREATE TABLE RptSAProdCmpn (promo_id string, item_sk int, totalsales float , total_sentiment float)"
					
			impala_stmt.execute(drop_RptSAProdCmpn)
			impala_stmt.execute(create_RptSAProdCmpn)
			
			val twitterRank = "twitterRank" + num_iter
			
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
			
			if (impala_stmt.execute(query_RptSAProdCmpn)) {
				impala_stmt.close()
				hive_stmt.close()
				return true
			}
			else{ 
				impala_stmt.close()
				hive_stmt.close()
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
	
	def runImpalaHiveImpl2(impala_connect: Connection, hive_connect: Connection): Boolean = { 
		
		try {
			
			val hive_stmt = hive_connect.createStatement()
			val impala_stmt = impala_connect.createStatement()
						
			hive_stmt.execute("SET hive.exec.compress.output=false")
			
//			val lower = 1
//			val upper = 300
			
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
			impala_stmt.execute(drop_promotionSelected)
			impala_stmt.execute(create_promotionSelected)
			impala_stmt.execute(query_promotionSelected)

			
			val drop_promotedProduct = "DROP VIEW IF EXISTS promotedProduct"
			val create_promotedProduct = "CREATE VIEW promotedProduct (item_sk, product_name, start_date_sk, end_date_sk) AS" +
					"	SELECT i_item_sk, i_product_name, start_date_sk, end_date_sk " +
					"	FROM item JOIN promotionSelected " +
					"	ON item.i_item_sk = promotionSelected.item_sk" +
					"	WHERE i_product_name IS NOT NULL"
			impala_stmt.execute(drop_promotedProduct)
			impala_stmt.execute(create_promotedProduct)	
							
			
			val drop_RptSalesByProdCmpn = "DROP VIEW IF EXISTS RptSalesByProdCmpn"
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
			impala_stmt.execute(drop_RptSalesByProdCmpn)			
			impala_stmt.execute(create_RptSalesByProdCmpn)
			
			LOG.info("Hive Execution: Get Relevant Tweets Begin")
			
			val drop_relevantTweet = "DROP TABLE IF EXISTS relevantTweet"
			val create_relevantTweet = "CREATE TABLE relevantTweet" +
					"	(item_sk int, user_id int, text string)"
			
			val query_relevantTweet	= "	INSERT INTO TABLE relevantTweet" +
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
			
			hive_stmt.execute(drop_relevantTweet)
			hive_stmt.execute(create_relevantTweet)
			hive_stmt.execute(query_relevantTweet)
			
			LOG.info("Hive Execution: Get Relevant Tweets End")
			
			
			LOG.info("Hive Execution: Get Sentiment Begin")
			val drop_senAnalyse = "DROP TABLE IF EXISTS senAnalyse"
			val create_senAnalyse = "CREATE TABLE senAnalyse AS" +
					"	SELECT item_sk, user_id, sum(sentiment(text)) as sentiment_score" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	item_sk, user_id"
			
			hive_stmt.execute(drop_senAnalyse)
			hive_stmt.execute(create_senAnalyse)
			LOG.info("Hive Execution: Get Sentiment End")
			
			/**
			 * Need to refresh the metastore to pick up tables created by Hive
			 */
			impala_stmt.execute("invalidate metadata")
			
			
			LOG.info("Impala Execution: Get tweetByUser Begin")
			val drop_tweetByUser = "DROP TABLE IF EXISTS tweetByUser"
			val create_tweetByUser = "CREATE TABLE tweetByUser (user_id int, num_tweets bigint)"
				
			val query_tweetByUser =	"INSERT INTO TABLE tweetByUser" +
					"	SELECT user_id, count(*)" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	user_id"
			
			impala_stmt.execute(drop_tweetByUser)
			impala_stmt.execute(create_tweetByUser)
			impala_stmt.execute(query_tweetByUser)
			LOG.info("Hive Execution: Get tweetByUser End")
					
			LOG.info("Impala Execution: Get tweetByProd Begin")
			val drop_tweetByProd = "DROP TABLE IF EXISTS tweetByProd"
			val create_tweetByProd = "CREATE TABLE tweetByProd (item_sk int, num_tweets bigint)"
				
			val	query_tweetByProd =	"INSERT INTO TABLE tweetByProd" +
					"	SELECT item_sk, count(*)" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	item_sk"
																																																																																																																		
			impala_stmt.execute(drop_tweetByProd)
			impala_stmt.execute(create_tweetByProd)
			impala_stmt.execute(query_tweetByProd)
			LOG.info("Impala Execution: Get tweetByProd End")	
							
			LOG.info("Impala Execution: Get sumFriendTweets Begin")
			val drop_sumFriendTweets = "DROP VIEW IF EXISTS sumFriendTweets"
			val create_sumFriendTweets = "CREATE VIEW sumFriendTweets (follower_id, num_friend_tweets) AS" +
//			val create_sumFriendTweets = "CREATE TABLE sumFriendTweets (follower_id int, num_friend_tweets bigint)"
//				
//			val query_sunFriendTweets = "INSERT INTO TABLE sumFriendTweets" +
					"	SELECT user_id, " +
					"		CASE WHEN num_friend_tweets > 0 THEN num_friend_tweets" +
					"			 ELSE 0" +
					"		END" +
					"	FROM" +
					"		(SELECT user_id, sum(friend_tweets) as num_friend_tweets" +
//					"		FROM tweetByUser LEFT OUTER JOIN" +
//					"			(SELECT follower_id, friend_id, num_tweets as friend_tweets" +
//					"			FROM twitter_graph JOIN tweetByUser" +
//					"	 		ON tweetByUser.user_id = twitter_graph.friend_id) f" +
//					"		ON tweetByUser.user_id = f.follower_id" +
					"		FROM " +
					"			(SELECT follower_id, friend_id, num_tweets as friend_tweets" +
					"			FROM twitter_graph JOIN tweetByUser" +
					"	 		ON tweetByUser.user_id = twitter_graph.friend_id) f RIGHT OUTER JOIN tweetByUser" +
					"		ON tweetByUser.user_id = f.follower_id" +
					"		GROUP BY " +
					"		user_id) result"
				
			impala_stmt.execute(drop_sumFriendTweets)
			impala_stmt.execute(create_sumFriendTweets)
//			impala_stmt.execute(query_sunFriendTweets)
			LOG.info("Impala Execution: Get sumFriendTweets End")
			
			LOG.info("Impala Execution: Get mentionProb Begin")
			val drop_mentionProb = "DROP TABLE IF EXISTS mentionProb"
			val create_mentionProb = "CREATE TABLE mentionProb (item_sk int, user_id int, prob double)"
				
			val query_mentionProb =	"INSERT INTO TABLE mentionProb" +
					"	SELECT item_sk, tweetByUser.user_id, r.num_tweets/tweetByUser.num_tweets" +
					"	FROM tweetByUser JOIN " +
					"		(SELECT item_sk, user_id, count(*) as num_tweets" +
					"		FROM relevantTweet" +
					"		GROUP BY" +
					"		item_sk, user_id) r" +
					"	ON tweetByUser.user_id = r.user_id"
				
			impala_stmt.execute(drop_mentionProb)
			impala_stmt.execute(create_mentionProb)
			impala_stmt.execute(query_mentionProb)
			LOG.info("Impala Execution: Get mentionProb Begin")

			LOG.info("Impala Execution: Get simUserByProd Begin")
			val drop_simUserByProd = "DROP TABLE IF EXISTS simUserByProd"
//			val create_simUserByProd = "CREATE VIEW simUserByProd (item_sk, follower_id, friend_id, similarity) AS" +
			val create_simUserByProd = "CREATE TABLE simUserByProd (item_sk int, follower_id int, friend_id int, similarity double)"
//				
			val query_simUserByProd = "INSERT INTO TABLE simUserByProd"	+
					"	SELECT f.item_sk, follower_id, friend_id, (1 - ABS(follower_prob - prob)) as similarity" +
					"	FROM " +
					"		(SELECT item_sk, follower_id, friend_id, prob as follower_prob" +
					"		FROM twitter_graph JOIN  mentionProb" +
					"		ON mentionProb.user_id = twitter_graph.follower_id) f" +
					"	JOIN mentionProb " +
					"	ON	f.friend_id = mentionProb.user_id AND f.item_sk=mentionProb.item_sk"
		

			
			impala_stmt.execute(drop_simUserByProd)
			impala_stmt.execute(create_simUserByProd)
			impala_stmt.execute(query_simUserByProd)		
			LOG.info("Impala Execution: Get simUserByProd End")

			
			LOG.info("Impala Execution: Get transitMatrix Begin")
			val drop_transitMatrix = "DROP TABLE IF EXISTS transitMatrix"
			val create_transitMatrix = "CREATE TABLE transitMatrix (item_sk int, follower_id int, friend_id int, transit_prob double)" 
				
			val query_transitMatrix = "INSERT INTO TABLE transitMatrix" +
					"	SELECT item_sk, follower_id, friend_id, " +
					"		CASE WHEN follower_id != friend_id THEN num_tweets/num_friend_tweets*similarity" +
					"			 ELSE 0" +
					"		END" +
					"	FROM" +
					"		(SELECT item_sk, t1.follower_id, friend_id, similarity, num_friend_tweets" +
					"		FROM simUserByProd t1 JOIN sumFriendTweets t2" +
					"		ON t1.follower_id = t2.follower_id) t3" +
					"	JOIN tweetByUser" +
					"	ON t3.friend_id = tweetByUser.user_id"
				
			impala_stmt.execute(drop_transitMatrix)
			impala_stmt.execute(create_transitMatrix)
			impala_stmt.execute(query_transitMatrix)
			LOG.info("Impala Execution: Get transitMatrix End")
			
			LOG.info("Impala Execution: Get randSuffVec Begin")
			val drop_randSufferVec = "DROP TABLE IF EXISTS randSuffVec"
			val create_randSuffVec = "CREATE TABLE randSuffVec (item_sk int, user_id int, prob double)" 
				
			val query_randSuffVec =	"INSERT INTO TABLE randSuffVec	" +
					"	SELECT t1.item_sk, user_id, t1.num_tweets/t2.num_tweets" +
					"	FROM" +
					"		(SELECT item_sk, user_id, count(*) as num_tweets" +
					"		FROM relevantTweet" +
					"		GROUP BY" +
					"		item_sk, user_id) t1" +
					"	JOIN tweetByProd t2" +
					"	ON t1.item_sk = t2.item_sk"
				
			impala_stmt.execute(drop_randSufferVec)
			impala_stmt.execute(create_randSuffVec)
			impala_stmt.execute(query_randSuffVec)
			LOG.info("Impala Execution: Get randSuffVec End")
			
			
			LOG.info("Impala Execution: Get initialRank Begin")
			val drop_initalRank = "DROP TABLE IF EXISTS initialRank"
			val create_initialRank = "CREATE TABLE initialRank (item_sk int, user_id int, rank_score double)" 
				
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
				
			impala_stmt.execute(drop_initalRank)
			impala_stmt.execute(create_initialRank)
			impala_stmt.execute(query_initialRank)
			LOG.info("Impala Execution: Get initialRank End")
					
			val alpha = 0.85
			for(iteration <- 1 to num_iter) {
						
				LOG.info("Impala Execution: Get TwitterRank" + iteration +" Begin")
				val twitterRank_previous = if(iteration == 1) "initialRank" else "twitterRank"+(iteration-1)	
				val drop_twitterRank = "DROP TABLE IF EXISTS twitterRank"+iteration
				val create_twitterRank = "CREATE TABLE twitterRank"+iteration+" (item_sk int, user_id int, rank_score double)"
				
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
			
				impala_stmt.execute(drop_twitterRank)
				impala_stmt.execute(create_twitterRank)
				impala_stmt.execute(query_twitterRank)
				LOG.info("Impala Execution: Get TwitterRank" + iteration +" End")
				
			}
			
			LOG.info("Impala Execution: Get TwitterRank RptSAProdCmpn Begin")
			val drop_RptSAProdCmpn = "DROP TABLE IF EXISTS RptSAProdCmpn"
			val create_RptSAProdCmpn = "CREATE TABLE RptSAProdCmpn (promo_id string, item_sk int, totalsales double , total_sentiment double)"
					
			impala_stmt.execute(drop_RptSAProdCmpn)
			impala_stmt.execute(create_RptSAProdCmpn)
			
			val twitterRank = "twitterRank" + num_iter
			
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
			
			if (impala_stmt.execute(query_RptSAProdCmpn)) {
				impala_stmt.close
				hive_stmt.close
				LOG.info("Impala Execution: Get RptSAProdCmpn End")
				return true
			}
			else{ 
				impala_stmt.close
				hive_stmt.close
				return false
			}

		} catch {
			case sqle :
				SQLException => {
					sqle.printStackTrace()
				}
			case e :
				Exception => {
					e.printStackTrace()
				}
		} 
		
		
		return false	
	
		
	}
	
	def runImpalaHive(impala_connect: Connection, hive_connect: Connection): Boolean = { 
		
		if(fileFormat == Constants.ORC_FORMAT)
			return runImpalaHiveImpl1(impala_connect, hive_connect)
		else if(fileFormat == Constants.PARQUET_FORMAT)
			return runImpalaHiveImpl2(impala_connect, hive_connect)
		else if(fileFormat == Constants.MIXED_FORMAT)
			return runImpalaHiveImpl2(impala_connect, hive_connect)
		else
			return false
		
//		return runImpalaHiveImpl1(impala_connect, hive_connect)
	}

	def cleanUpImpalaHiveImpl1(impala_connect: Connection, hive_connect: Connection): Unit = {
		val stmt = hive_connect.createStatement()	
		
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
	
	def cleanUpImpalaHiveImpl2(impala_connect: Connection, hive_connect: Connection): Unit = {
		LOG.info("Droping Intermediate Tables...")
		
		val hive_stmt = hive_connect.createStatement()	
		val impala_stmt = impala_connect.createStatement()	
		
		val list_drop = Seq("DROP TABLE IF EXISTS promotionSelected",
							"DROP VIEW IF EXISTS promotedProduct",
							"DROP VIEW IF EXISTS RptSalesByProdCmpn",
							"DROP TABLE IF EXISTS tweetByUser",
							"DROP TABLE IF EXISTS tweetByProd",
							"DROP VIEW IF EXISTS sumFriendTweets",
							"DROP TABLE IF EXISTS mentionProb",
							"DROP VIEW IF EXISTS simUserByProd",
							"DROP TABLE IF EXISTS transitMatrix",
							"DROP TABLE IF EXISTS randSuffVec",
							"DROP TABLE IF EXISTS initialRank")
		
		list_drop.foreach(impala_stmt.execute(_))
									
		for(iteration <- 1 to num_iter) {
							
			val drop_twitterRank = "DROP TABLE IF EXISTS twitterRank"+iteration

			impala_stmt.execute(drop_twitterRank)
				
		}
		
		hive_stmt.execute("DROP TABLE IF EXISTS relevantTweet")
		hive_stmt.execute("DROP TABLE IF EXISTS senAnalyse")
		
		hive_stmt.close()
		impala_stmt.close()
		
	}
	
	def cleanUpImpalaHive(impala_connect: Connection, hive_connect: Connection): Unit = {
		
		if(fileFormat == Constants.ORC_FORMAT)
			cleanUpImpalaHiveImpl1(impala_connect, hive_connect)
		else if(fileFormat == Constants.PARQUET_FORMAT)
			cleanUpImpalaHiveImpl2(impala_connect, hive_connect)
			
		else if(fileFormat == Constants.MIXED_FORMAT)
			cleanUpImpalaHiveImpl2(impala_connect, hive_connect)
	}

	def printDescription(): Unit = {}

}