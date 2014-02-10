package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import org.apache.spark.SparkContext
import org.apache.spark.bagel._
import org.apache.spark.bagel.Bagel._

import bigframe.workflows.runnable.SharkBagelRunnable
import bigframe.workflows.Query
import bigframe.workflows.BaseTablePath
import bigframe.workflows.util.TRVertex
import bigframe.workflows.util.TRMessage
import bigframe.workflows.util.HDFSUtil

import shark.{SharkContext, SharkEnv}

import bigframe.workflows.BusinessIntelligence.graph.exploratory.WF_TwitterRankBagel

class WF_ReportSaleSentimentSharkBagel(basePath: BaseTablePath, hadoop_home: String, hiveWarehouse: String, 
		numItr: Int, val useRC: Boolean, val numPartition: Int = 10)
		extends Query with SharkBagelRunnable {

	val twitterRank_path = basePath.relational_path  + "/../twitterrank"
	val hdfsUtil = new HDFSUtil(hadoop_home)
	
	def printDescription(): Unit = {}
	
	/*
	 * Prepeare the basic tables before run the Shark query
	 */
	override def prepareSharkBagelTables(sc: SharkContext): Unit = {
		
		val table_preparer = new PrepareTable_SharkC(basePath)
		
		if(useRC)
			table_preparer.prepareTableImplRC(sc)
		else
			table_preparer.prepareTableImplText(sc)
	}
	
	def cleanUpSharkBagelImpl1(sc: SharkContext): Unit = {
		
	}
	
	override def cleanUpSharkBagel(sc: SharkContext): Unit = {
		cleanUpSharkBagelImpl1(sc)
	}
	
	
	def runSharkBagelImpl1(sc: SharkContext) : Boolean = {
			
			val drop_promotionSelected = "DROP TABLE IF EXISTS promotionSelected_cached"
			val create_promotionSelected = "CREATE TABLE promotionSelected_cached AS" +
					"	SELECT p_promo_id as promo_id, p_item_sk as item_sk, p_start_date_sk as start_date_sk, p_end_date_sk as end_date_sk " +
					"	FROM promotion " +
					"	WHERE p_item_sk IS NOT NULL AND p_start_date_sk IS NOT NULL AND p_end_date_sk IS NOT NULL"
			
			/**
			 * Choose all promotion except those contain NULL value.
			 */
			sc.sql(drop_promotionSelected)
			sc.sql(create_promotionSelected)

			
			val drop_promotedProduct = "DROP VIEW IF EXISTS promotedProduct"
			val create_promotedProduct = "CREATE VIEW promotedProduct (item_sk, product_name, start_date_sk, end_date_sk) AS" +
					"	SELECT i_item_sk, i_product_name, start_date_sk, end_date_sk " +
					"	FROM item JOIN promotionSelected_cached " +
					"	ON item.i_item_sk = promotionSelected_cached.item_sk" +
					"	WHERE i_product_name IS NOT NULL"
			sc.runSql(drop_promotedProduct)
			sc.runSql(create_promotedProduct)	
							
			
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
			sc.runSql(drop_RptSalesByProdCmpn)			
			sc.runSql(create_RptSalesByProdCmpn)
			
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
			
			
			sc.runSql(drop_relevantTweet)
			sc.runSql(create_relevantTweet)
			

			val drop_senAnalyse = "DROP VIEW IF EXISTS senAnalyse"
			val create_senAnalyse = "CREATE VIEW senAnalyse" +
					"	(item_sk, user_id, sentiment_score) AS" +
					"	SELECT item_sk, user_id, sum(sentiment(text)) as sum_score" +
					"	FROM relevantTweet_cached" +
					"	GROUP BY" +
					"	item_sk, user_id"
			
			sc.runSql(drop_senAnalyse)
			sc.runSql(create_senAnalyse)
			
			
			val drop_tweetByUser = "DROP TABLE IF EXISTS tweetByUser_cached"
			val create_tweetByUser = "CREATE TABLE tweetByUser_cached AS" +
					"	SELECT user_id, count(*) as num_tweets" +
					"	FROM relevantTweet_cached" +
					"	GROUP BY" +
					"	user_id"
				
			
			sc.runSql(drop_tweetByUser)
			sc.runSql(create_tweetByUser)
					
			val drop_tweetByProd = "DROP TABLE IF EXISTS tweetByProd_cached"
			val create_tweetByProd = "CREATE TABLE tweetByProd_cached AS" + 
					"	SELECT item_sk, count(*) as num_tweets" +
					"	FROM relevantTweet_cached" +
					"	GROUP BY" +
					"	item_sk"
				
																																																																																																																		
			sc.runSql(drop_tweetByProd)
			sc.runSql(create_tweetByProd)

			val drop_cachedTwitterGraph = "DROP TABLE IF EXISTS twitter_graph_cached"
			val create_cachedTwitterGraph = "CREATE TABLE twitter_graph_cached AS SELECT * from twitter_graph"
				
			sc.runSql(drop_cachedTwitterGraph)				
			sc.runSql(create_cachedTwitterGraph)
			
							
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
				
			sc.runSql(drop_sumFriendTweets)
			sc.runSql(create_sumFriendTweets)
			
			
			val drop_mentionProb = "DROP TABLE IF EXISTS mentionProb_cached"
			val create_mentionProb = "CREATE TABLE mentionProb_cached AS" + 
					"	SELECT item_sk, tweetByUser_cached.user_id as user_id, r.num_tweets/tweetByUser_cached.num_tweets as prob" +
					"	FROM tweetByUser_cached JOIN " +
					"		(SELECT item_sk, user_id, count(*) as num_tweets" +
					"		FROM relevantTweet_cached" +
					"		GROUP BY" +
					"		item_sk, user_id) r" +
					"	ON tweetByUser_cached.user_id = r.user_id"
				
			sc.runSql(drop_mentionProb)
			sc.runSql(create_mentionProb)
			
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
		

			
			sc.runSql(drop_simUserByProd)
			sc.runSql(create_simUserByProd)
					
					

			val drop_transitMatrix = "DROP TABLE IF EXISTS transitMatrix"
			val create_transitMatrix = "CREATE TABLE transitMatrix AS" +
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
				
			sc.runSql(drop_transitMatrix)
			sc.runSql(create_transitMatrix)
			
			val drop_randSufferVec = "DROP VIEW IF EXISTS randSuffVec"
			val create_randSuffVec = "CREATE VIEW randSuffVec AS" +
					"	SELECT t1.item_sk as item_sk, user_id, t1.num_tweets/t2.num_tweets as prob" +
					"	FROM" +
					"		(SELECT item_sk, user_id, count(*) as num_tweets" +
					"		FROM relevantTweet_cached" +
					"		GROUP BY" +
					"		item_sk, user_id) t1" +
					"	JOIN tweetByProd_cached t2" +
					"	ON t1.item_sk = t2.item_sk"
				
			sc.runSql(drop_randSufferVec)
			sc.runSql(create_randSuffVec)
			
			val drop_initalRank = "DROP VIEW IF EXISTS initialRank"
			val create_initialRank = "CREATE VIEW initialRank AS" +
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
				
				
			sc.runSql(drop_initalRank)
			sc.runSql(create_initialRank)
					
			
			val drop_rankandsuffvec = "DROP TABLE IF EXISTS randandsuffvec"
			val create_rankandsuffvec = "CREATE TABLE randandsuffvec AS" +
					"	SELECT t1.item_sk, t1.user_id, rank_score, prob" +
					"	FROM" +
					"		randSuffVec t1" +
					"	JOIN" +
					"		initialRank t2" +
					"	ON t1.item_sk = t2.item_sk AND t1.user_id = t2.user_id"
			
			
					
					
			sc.runSql(drop_rankandsuffvec)
			sc.runSql(create_rankandsuffvec)
					
			val alpha = 0.85

			val twitterRank_bagel = new WF_TwitterRankBagel(hiveWarehouse + "/randandsuffvec", hiveWarehouse + "/transitmatrix", 
					numItr, alpha, numPartition, twitterRank_path)
			
			if(twitterRank_bagel.runBagel(sc)) {
			
				val drop_TwitterRank = "DROP TABLE IF EXISTS twitterRank"
				val create_TwitterRank = "CREATE EXTERNAL TABLE twitterRank (item_sk int, user_id int, rank_score float)" +
					"	row format delimited fields terminated by \'|\' " +
					"	location " + "\'" + twitterRank_path + "\'"
				
				sc.runSql(drop_TwitterRank)
				sc.runSql(create_TwitterRank)
					
				val drop_RptSAProdCmpn = "DROP TABLE IF EXISTS RptSAProdCmpn"
				val create_RptSAProdCmpn = "CREATE TABLE RptSAProdCmpn (promo_id string, item_sk int, totalsales float , total_sentiment float)"
						
				sc.runSql(drop_RptSAProdCmpn)
				sc.runSql(create_RptSAProdCmpn)
				
				val twitterRank = "twitterRank"
				
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
						
				sc.runSql(query_RptSAProdCmpn)
			}
						
		return true
	}
	
	
	/**
	 * Run the benchmark query
	 */
	override def runSharkBagel(sc: SharkContext): Boolean = {
		
			return runSharkBagelImpl1(sc)

	}

}