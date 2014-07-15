package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import java.util.concurrent.{Executors, ExecutorService}
import java.util.concurrent.Future
import java.sql.Connection
import java.sql.SQLException

import bigframe.workflows.Query
import bigframe.workflows.runnable.VerticaRunnable
import bigframe.workflows.BaseTablePath
import org.apache.log4j.Logger

import scala.collection.JavaConversions._

class WF_ReportSaleSentimentVertica(basePath: BaseTablePath, num_iter: Int, jsonAsString: Boolean) extends Query with VerticaRunnable {

	val LOG = Logger.getLogger(classOf[WF_ReportSaleSentimentVertica]);
	
	def printDescription(): Unit = {}


	override def prepareVerticaTables(connection: Connection): Unit = {}
	

		
	def cleanUpVertica1(connection: Connection): Unit = {
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
							
			val drop_twitterRank = "DROP VIEW IF EXISTS twitterRank"+iteration

			stmt.execute(drop_twitterRank)
				
		}
	}
	
	override def cleanUpVertica(connection: Connection): Unit = {
		cleanUpVertica1(connection)		
	}
	
	
	
	/**
	 * This implementation requires more memory since it doesn't materialize all intermediate tables.  
	 * 
	 */
	def verticaImpl1(connection: Connection): Boolean = {
		try {
			val stmt = connection.createStatement();
						
			val lower = 1
			val upper = 300
			
			LOG.info("Running the Report Sales Sentiment workflow with json normalized")
			val drop_promotionSelected = "DROP TABLE IF EXISTS promotionSelected"
			val create_promotionSelected = "CREATE TABLE promotionSelected (promo_id char(16), item_sk int," +
					"start_date_sk int, end_date_sk int)"
			stmt.execute(drop_promotionSelected)
			stmt.execute(create_promotionSelected)

			/**
			 * Choose all promotion except those contain NULL value.
			 */
			val query_promotionSelected = "INSERT INTO promotionSelected " + 
					"	SELECT p_promo_id, p_item_sk, p_start_date_sk, p_end_date_sk " +
					"	FROM promotion " +
					"	WHERE p_item_sk IS NOT NULL AND p_start_date_sk IS NOT NULL AND p_end_date_sk IS NOT NULL"
			

			stmt.executeUpdate(query_promotionSelected)

			
			val drop_promotedProduct = "DROP VIEW IF EXISTS promotedProduct"
			val create_promotedProduct = "CREATE VIEW promotedProduct (item_sk, product_name, start_date_sk, end_date_sk) AS" +
					"	SELECT i_item_sk, i_product_name, start_date_sk, end_date_sk" +
					"	FROM item JOIN promotionSelected " +
					"	ON i_item_sk = item_sk AND i_product_name IS NOT NULL"
			stmt.execute(drop_promotedProduct)
			stmt.execute(create_promotedProduct)	
							
			
			val drop_RptSalesByProdCmpn = "DROP VIEW IF EXISTS RptSalesByProdCmpn"
			val create_RptSalesByProdCmpn = "CREATE VIEW RptSalesByProdCmpn " +
					"	(promo_id, item_sk, totalsales) AS" +
					"		SELECT p.promo_id, p.item_sk, sum(price*quantity) as totalsales " +
					"		FROM" + 
					"			(SELECT ws_sold_date_sk as sold_date_sk, ws_item_sk as item_sk, ws_sales_price as price, ws_quantity as quantity " +
					"			FROM web_sales " +
					"			WHERE ws_sales_price IS NOT NULL AND ws_quantity IS NOT NULL" + 
					"			UNION ALL " + 
					"			SELECT ss_sold_date_sk as sold_date_sk, ss_item_sk as item_sk, ss_sales_price as price, ss_quantity as quantity "  +
					"			FROM store_sales" + 
					"			WHERE ss_sales_price IS NOT NULL AND ss_quantity IS NOT NULL" + 
					"			UNION ALL" + 
					"			SELECT cs_sold_date_sk as sold_date_sk, cs_item_sk as item_sk, cs_sales_price as price, cs_quantity as quantity" +
					"			FROM catalog_sales"  +
					"			WHERE cs_sales_price IS NOT NULL AND cs_quantity IS NOT NULL ) sales" + 	
					"			JOIN promotionSelected p "  +
					"			ON sales.item_sk = p.item_sk" +
					"		WHERE " + 
					"			p.start_date_sk <= sold_date_sk " +
					"			AND" + 
					"			sold_date_sk <= p.end_date_sk" +
					"		GROUP BY" + 
					"			p.promo_id, p.item_sk "
			stmt.execute(drop_RptSalesByProdCmpn)
			stmt.execute(create_RptSalesByProdCmpn)
						
			
			val drop_relevantTweet = "DROP TABLE IF EXISTS relevantTweet"
			val create_relevantTweet = "CREATE TABLE relevantTweet" +
					"	(item_sk int, user_id int, text varchar(200) )"
			
			stmt.execute(drop_relevantTweet)
			stmt.execute(create_relevantTweet)
			
			
			LOG.info("Begin to get the set of relevant tweets")
			val query_relevantTweet = "INSERT INTO relevantTweet" +
					"		SELECT item_sk, user_id, text" +
					"		FROM " +
					"			(SELECT user_id, hashtag, created_at, text" +
					"			FROM tweet JOIN entities" +
					"			ON tweet_id = id) t1 ," +
					"			(SELECT item_sk, product_name, start_date, d_date as end_date" +
					"			FROM " +
					"				(SELECT item_sk, product_name, d_date as start_date, end_date_sk" +
					"				FROM promotedProduct, date_dim" +
					"				WHERE start_date_sk = d_date_sk) t2 " +
					"			JOIN date_dim " +
					"			ON end_date_sk = d_date_sk) t3" +
					"		WHERE product_name = hashtag AND created_at > start_date AND created_at < end_date"

					
			stmt.executeUpdate(query_relevantTweet)
			LOG.info("Finish getting the set of relevant tweets")

			val drop_senAnalyse = "DROP VIEW IF EXISTS senAnalyse"
			val create_senAnalyse = "CREATE VIEW senAnalyse" +
					"	(item_sk, user_id, sentiment_score) AS" +
					"	SELECT item_sk, user_id, sum(sentiment(text)) as sum_score" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	item_sk, user_id"
			
			stmt.execute(drop_senAnalyse)
			stmt.execute(create_senAnalyse)
			
			
			val drop_tweetByUser = "DROP TABLE IF EXISTS tweetByUser"
			val create_tweetByUser = "CREATE TABLE tweetByUser (user_id int, num_tweets int)"
				
			val query_tweetByUser =	"INSERT INTO tweetByUser" +
					"	SELECT user_id, count(*)" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	user_id"
			
			stmt.execute(drop_tweetByUser)
			stmt.execute(create_tweetByUser)
			stmt.executeUpdate(query_tweetByUser)
					
			val drop_tweetByProd = "DROP TABLE IF EXISTS tweetByProd"
			val create_tweetByProd = "CREATE TABLE tweetByProd (item_sk int, num_tweets int)"
				
			val query_tweetByProd =	"INSERT INTO tweetByProd" +
					"	SELECT item_sk, count(*)" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	item_sk"
																																																																																																																		
			stmt.execute(drop_tweetByProd)
			stmt.execute(create_tweetByProd)
			stmt.executeUpdate(query_tweetByProd)

							
			val drop_sumFriendTweets = "DROP VIEW IF EXISTS sumFriendTweets"
			val create_sumFriendTweets = "CREATE VIEW sumFriendTweets (follower_id, num_friend_tweets) AS" +
					"	SELECT user_id, " +
					"		CASE WHEN num_friend_tweets > 0 THEN num_friend_tweets" +
					"			 ELSE 0" +
					"		END" +
					"	FROM" +
					"		(SELECT user_id, sum(friend_tweets) as num_friend_tweets" +
					"		FROM tweetByUser LEFT OUTER JOIN" +
					"			(SELECT follower_id, friend_id, num_tweets as friend_tweets" +
					"			FROM tweetByUser JOIN twitter_graph" +
					"	 		ON user_id = friend_id) f" +
					"		ON user_id = follower_id" +
					"		GROUP BY " +
					"		user_id) result"
				
			stmt.execute(drop_sumFriendTweets)
			stmt.execute(create_sumFriendTweets)
			
			
			val drop_mentionProb = "DROP TABLE IF EXISTS mentionProb"
			val create_mentionProb = "CREATE TABLE mentionProb (item_sk int, user_id int, prob float)"
				
			stmt.execute(drop_mentionProb)
			stmt.execute(create_mentionProb)
			
			val query_mentionProb = "INSERT INTO mentionProb" +
					"	SELECT item_sk, t.user_id, r.num_tweets/t.num_tweets" +
					"	FROM tweetByUser as t JOIN " +
					"		(SELECT item_sk, user_id, count(*) as num_tweets" +
					"		FROM relevantTweet" +
					"		GROUP BY" +
					"		item_sk, user_id) r" +
					"	ON t.user_id = r.user_id"
			
			stmt.executeUpdate(query_mentionProb)

			
			val drop_simUserByProd = "DROP VIEW IF EXISTS simUserByProd"
			val create_simUserByProd = "CREATE VIEW simUserByProd " +
					"	(item_sk, follower_id, friend_id, similarity) AS" +
					"	SELECT f.item_sk, follower_id, friend_id, (1 - ABS(follower_prob - prob)) as similarity" +
					"	FROM " +
					"		(SELECT item_sk, follower_id, friend_id, prob as follower_prob" +
					"		FROM mentionProb JOIN twitter_graph " +
					"		ON user_id = follower_id) f" +
					"	JOIN mentionProb " +
					"	ON	friend_id = user_id AND f.item_sk=mentionProb.item_sk"
			
			stmt.execute(drop_simUserByProd)
			stmt.execute(create_simUserByProd)
					
					
		val drop_transitMatrix = "DROP TABLE IF EXISTS transitMatrix"
			val create_transitMatrix = "CREATE TABLE transitMatrix (item_sk int, follower_id int, friend_id int, transit_prob float)"
				
			stmt.execute(drop_transitMatrix)
			stmt.execute(create_transitMatrix)
			
			val query_transitMatrix = "INSERT INTO transitMatrix" +
					"	SELECT item_sk, follower_id, friend_id, " +
					"		CASE WHEN follower_id != friend_id THEN num_tweets/num_friend_tweets*similarity" +
					"			 ELSE 0" +
					"		END" +
					"	FROM" +
					"		(SELECT item_sk, t1.follower_id, friend_id, similarity, num_friend_tweets" +
					"		FROM simUserByProd t1 JOIN sumFriendTweets t2" +
					"		ON t1.follower_id = t2.follower_id) t3" +
					"	JOIN tweetByUser" +
					"	ON friend_id = user_id"
					
					
			stmt.executeUpdate(query_transitMatrix)
			
			
			val drop_randSufferVec = "DROP TABLE IF EXISTS randSuffVec"
			val create_randSuffVec = "CREATE TABLE randSUffVec (item_sk int, user_id int, prob float)"
				
			stmt.execute(drop_randSufferVec)
			stmt.execute(create_randSuffVec)
			
			val query_randSuffVec = "INSERT INTO randSuffVec" +
					"	SELECT t1.item_sk, user_id, t1.num_tweets/t2.num_tweets" +
					"	FROM" +
					"		(SELECT item_sk, user_id, count(*) as num_tweets" +
					"		FROM relevantTweet" +
					"		GROUP BY" +
					"		item_sk, user_id) t1" +
					"	JOIN tweetByProd t2" +
					"	ON t1.item_sk = t2.item_sk"
			
			stmt.executeUpdate(query_randSuffVec)
			
			val drop_initalRank = "DROP TABLE IF EXISTS initialRank"
			val create_initialRank = "CREATE TABLE initialRank (item_sk int, user_id int, rank_score float)"
				
			stmt.execute(drop_initalRank)
			stmt.execute(create_initialRank)
			
			val query_initialRank = "INSERT INTO initialRank" +
					"	SELECT t1.item_sk, user_id, 1/num_users" +
					"	FROM " +
					"		(SELECT item_sk, COUNT(DISTINCT user_id) as num_users" +
					"		FROM relevantTweet" +
					"		GROUP BY" +
					"		item_sk) t1 " +
					"	JOIN mentionProb" +
					"	ON t1.item_sk = mentionProb.item_sk"
			stmt.executeUpdate(query_initialRank)


			LOG.info("Begin to compute TwitterRank")
			val alpha = 0.85
			for(iteration <- 1 to num_iter) {
						
				val twitterRank_previous = if(iteration == 1) "initialRank" else "twitterRank"+(iteration-1)	
				val drop_twitterRank = "DROP VIEW IF EXISTS twitterRank"+iteration
				val create_twitterRank = "CREATE VIEW twitterRank"+iteration+" (item_sk, user_id, rank_score) AS" +
						"	SELECT t4.item_sk, t4.user_id, " +
						"		CASE WHEN sum_follower_score > 0 THEN " + alpha + " * sum_follower_score + " + (1-alpha) +" * prob" +
						"			 ELSE " + (1-alpha) +" * prob" +
						"		END" +
						"	FROM" +
						"		(SELECT t1.item_sk, follower_id, sum(transit_prob * rank_score) as sum_follower_score" +
						"		FROM transitMatrix t1, " + twitterRank_previous +" t2" +
						"		WHERE t1.friend_id = t2.user_id AND t1.item_sk = t2.item_sk " +
						"		GROUP BY " +
						"		t1.item_sk, follower_id) t3" +
						"	RIGHT JOIN randSUffVec t4" +
						"	ON t3.item_sk = t4.item_sk AND t3.follower_id = t4.user_id"
			
				stmt.execute(drop_twitterRank)
				stmt.execute(create_twitterRank)
			}
			LOG.info("Finish computing TwitterRank")
			
			val drop_RptSAProdCmpn = "DROP TABLE IF EXISTS RptSAProdCmpn"
			val create_RptSAProdCmpn = "CREATE TABLE RptSAProdCmpn (promo_id char(16), item_sk int, totalsales float , total_sentiment float)"
					
			stmt.execute(drop_RptSAProdCmpn)
			stmt.execute(create_RptSAProdCmpn)
			
			val query_RptSAProdCmpn = "INSERT INTO RptSAProdCmpn" +
					"	SELECT promo_id, t4.item_sk, totalsales, total_sentiment" +
					"	FROM " +
					"		(SELECT t1.item_sk, sum(rank_score * sentiment_score) as total_sentiment" +
					"		FROM senAnalyse t1, twitterRank" + num_iter + " t2" +
					"		WHERE t1.item_sk = t2.item_sk AND t1.user_id = t2.user_id" +
					"		GROUP BY" +
					"		t1.item_sk) t3" +
					"	JOIN RptSalesByProdCmpn t4 " +
					"	ON t3.item_sk = t4.item_sk"				
			
			if (stmt.executeUpdate(query_RptSAProdCmpn) > 0) {
				LOG.info("Finish the Report Sales Sentiment workflow")
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
	 * This implementation store tweets json as string. 
	 * 
	 */
	def verticaImpl2(connection: Connection): Boolean = {
		try {
			val stmt = connection.createStatement();
						
			val lower = 1
			val upper = 300
			
			LOG.info("Running the Report Sales Sentiment workflow with json as string")
			val drop_promotionSelected = "DROP TABLE IF EXISTS promotionSelected"
			val create_promotionSelected = "CREATE TABLE promotionSelected (promo_id char(16), item_sk int," +
					"start_date_sk int, end_date_sk int)"
			stmt.execute(drop_promotionSelected)
			stmt.execute(create_promotionSelected)

			/**
			 * Choose all promotion except those contain NULL value.
			 */
			val query_promotionSelected = "INSERT INTO promotionSelected " + 
					"	SELECT p_promo_id, p_item_sk, p_start_date_sk, p_end_date_sk " +
					"	FROM promotion " +
					"	WHERE p_item_sk IS NOT NULL AND p_start_date_sk IS NOT NULL AND p_end_date_sk IS NOT NULL"
			

			stmt.executeUpdate(query_promotionSelected)

			
			val drop_promotedProduct = "DROP VIEW IF EXISTS promotedProduct"
			val create_promotedProduct = "CREATE VIEW promotedProduct (item_sk, product_name, start_date_sk, end_date_sk) AS" +
					"	SELECT i_item_sk, i_product_name, start_date_sk, end_date_sk" +
					"	FROM item JOIN promotionSelected " +
					"	ON i_item_sk = item_sk AND i_product_name IS NOT NULL"
			stmt.execute(drop_promotedProduct)
			stmt.execute(create_promotedProduct)	
							
			
			val drop_RptSalesByProdCmpn = "DROP VIEW IF EXISTS RptSalesByProdCmpn"
			val create_RptSalesByProdCmpn = "CREATE VIEW RptSalesByProdCmpn " +
					"	(promo_id, item_sk, totalsales) AS" +
					"		SELECT p.promo_id, p.item_sk, sum(price*quantity) as totalsales " +
					"		FROM" + 
					"			(SELECT ws_sold_date_sk as sold_date_sk, ws_item_sk as item_sk, ws_sales_price as price, ws_quantity as quantity " +
					"			FROM web_sales " +
					"			WHERE ws_sales_price IS NOT NULL AND ws_quantity IS NOT NULL" + 
					"			UNION ALL " + 
					"			SELECT ss_sold_date_sk as sold_date_sk, ss_item_sk as item_sk, ss_sales_price as price, ss_quantity as quantity "  +
					"			FROM store_sales" + 
					"			WHERE ss_sales_price IS NOT NULL AND ss_quantity IS NOT NULL" + 
					"			UNION ALL" + 
					"			SELECT cs_sold_date_sk as sold_date_sk, cs_item_sk as item_sk, cs_sales_price as price, cs_quantity as quantity" +
					"			FROM catalog_sales"  +
					"			WHERE cs_sales_price IS NOT NULL AND cs_quantity IS NOT NULL ) sales" + 	
					"			JOIN promotionSelected p "  +
					"			ON sales.item_sk = p.item_sk" +
					"		WHERE " + 
					"			p.start_date_sk <= sold_date_sk " +
					"			AND" + 
					"			sold_date_sk <= p.end_date_sk" +
					"		GROUP BY" + 
					"			p.promo_id, p.item_sk "
			stmt.execute(drop_RptSalesByProdCmpn)
			stmt.execute(create_RptSalesByProdCmpn)
						
			
			val drop_relevantTweet = "DROP TABLE IF EXISTS relevantTweet"
			val create_relevantTweet = "CREATE TABLE relevantTweet" +
					"	(item_sk int, user_id int, text varchar(200) )"
			
			stmt.execute(drop_relevantTweet)
			stmt.execute(create_relevantTweet)
			
			LOG.info("Begin to get the set of relevant tweets")
			val query_relevantTweet = "INSERT INTO relevantTweet" +
					"		SELECT item_sk, user_id, text" +
					"		FROM " +
					"			(SELECT user_id, hashtag, to_timestamp(created_at, 'WW Mon DD HH:MI:SS TZ YYYY') as created_at, text" +
					"			FROM (SELECT twitter(json) over(partition by hash(json)) from tweetjson) foo) t1 ," +
					//"			FROM (SELECT twitter(json) over() from tweetjson) foo) t1 ," +
					"			(SELECT item_sk, product_name, start_date, d_date as end_date" +
					"			FROM " +
					"				(SELECT item_sk, product_name, d_date as start_date, end_date_sk" +
					"				FROM promotedProduct, date_dim" +
					"				WHERE start_date_sk = d_date_sk) t2 " +
					"			JOIN date_dim " +
					"			ON end_date_sk = d_date_sk) t3" +
					"		WHERE product_name = hashtag AND created_at > start_date AND created_at < end_date"

					
			stmt.executeUpdate(query_relevantTweet)
			LOG.info("Finish getting the set of relevant tweets")

			val drop_senAnalyse = "DROP VIEW IF EXISTS senAnalyse"
			val create_senAnalyse = "CREATE VIEW senAnalyse" +
					"	(item_sk, user_id, sentiment_score) AS" +
					"	SELECT item_sk, user_id, sum(sentiment(text)) as sum_score" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	item_sk, user_id"
			
			stmt.execute(drop_senAnalyse)
			stmt.execute(create_senAnalyse)
			
			
			val drop_tweetByUser = "DROP TABLE IF EXISTS tweetByUser"
			val create_tweetByUser = "CREATE TABLE tweetByUser (user_id int, num_tweets int)"
				
			val query_tweetByUser =	"INSERT INTO tweetByUser" +
					"	SELECT user_id, count(*)" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	user_id"
			
			stmt.execute(drop_tweetByUser)
			stmt.execute(create_tweetByUser)
			stmt.executeUpdate(query_tweetByUser)
					
			val drop_tweetByProd = "DROP TABLE IF EXISTS tweetByProd"
			val create_tweetByProd = "CREATE TABLE tweetByProd (item_sk int, num_tweets int)"
				
			val query_tweetByProd =	"INSERT INTO tweetByProd" +
					"	SELECT item_sk, count(*)" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	item_sk"
																																																																																																																		
			stmt.execute(drop_tweetByProd)
			stmt.execute(create_tweetByProd)
			stmt.executeUpdate(query_tweetByProd)

							
			val drop_sumFriendTweets = "DROP VIEW IF EXISTS sumFriendTweets"
			val create_sumFriendTweets = "CREATE VIEW sumFriendTweets (follower_id, num_friend_tweets) AS" +
					"	SELECT user_id, " +
					"		CASE WHEN num_friend_tweets > 0 THEN num_friend_tweets" +
					"			 ELSE 0" +
					"		END" +
					"	FROM" +
					"		(SELECT user_id, sum(friend_tweets) as num_friend_tweets" +
					"		FROM tweetByUser LEFT OUTER JOIN" +
					"			(SELECT follower_id, friend_id, num_tweets as friend_tweets" +
					"			FROM tweetByUser JOIN twitter_graph" +
					"	 		ON user_id = friend_id) f" +
					"		ON user_id = follower_id" +
					"		GROUP BY " +
					"		user_id) result"
				
			stmt.execute(drop_sumFriendTweets)
			stmt.execute(create_sumFriendTweets)
			
			
			val drop_mentionProb = "DROP TABLE IF EXISTS mentionProb"
			val create_mentionProb = "CREATE TABLE mentionProb (item_sk int, user_id int, prob float)"
				
			stmt.execute(drop_mentionProb)
			stmt.execute(create_mentionProb)
			
			val query_mentionProb = "INSERT INTO mentionProb" +
					"	SELECT item_sk, t.user_id, r.num_tweets/t.num_tweets" +
					"	FROM tweetByUser as t JOIN " +
					"		(SELECT item_sk, user_id, count(*) as num_tweets" +
					"		FROM relevantTweet" +
					"		GROUP BY" +
					"		item_sk, user_id) r" +
					"	ON t.user_id = r.user_id"
			
			stmt.executeUpdate(query_mentionProb)

			
			val drop_simUserByProd = "DROP VIEW IF EXISTS simUserByProd"
			val create_simUserByProd = "CREATE VIEW simUserByProd " +
					"	(item_sk, follower_id, friend_id, similarity) AS" +
					"	SELECT f.item_sk, follower_id, friend_id, (1 - ABS(follower_prob - prob)) as similarity" +
					"	FROM " +
					"		(SELECT item_sk, follower_id, friend_id, prob as follower_prob" +
					"		FROM mentionProb JOIN twitter_graph " +
					"		ON user_id = follower_id) f" +
					"	JOIN mentionProb " +
					"	ON	friend_id = user_id AND f.item_sk=mentionProb.item_sk"
			
			stmt.execute(drop_simUserByProd)
			stmt.execute(create_simUserByProd)
					
					
		val drop_transitMatrix = "DROP TABLE IF EXISTS transitMatrix"
			val create_transitMatrix = "CREATE TABLE transitMatrix (item_sk int, follower_id int, friend_id int, transit_prob float)"
				
			stmt.execute(drop_transitMatrix)
			stmt.execute(create_transitMatrix)
			
			val query_transitMatrix = "INSERT INTO transitMatrix" +
					"	SELECT item_sk, follower_id, friend_id, " +
					"		CASE WHEN follower_id != friend_id THEN num_tweets/num_friend_tweets*similarity" +
					"			 ELSE 0" +
					"		END" +
					"	FROM" +
					"		(SELECT item_sk, t1.follower_id, friend_id, similarity, num_friend_tweets" +
					"		FROM simUserByProd t1 JOIN sumFriendTweets t2" +
					"		ON t1.follower_id = t2.follower_id) t3" +
					"	JOIN tweetByUser" +
					"	ON friend_id = user_id"
					
					
			stmt.executeUpdate(query_transitMatrix)
			
			
			val drop_randSufferVec = "DROP TABLE IF EXISTS randSuffVec"
			val create_randSuffVec = "CREATE TABLE randSUffVec (item_sk int, user_id int, prob float)"
				
			stmt.execute(drop_randSufferVec)
			stmt.execute(create_randSuffVec)
			
			val query_randSuffVec = "INSERT INTO randSuffVec" +
					"	SELECT t1.item_sk, user_id, t1.num_tweets/t2.num_tweets" +
					"	FROM" +
					"		(SELECT item_sk, user_id, count(*) as num_tweets" +
					"		FROM relevantTweet" +
					"		GROUP BY" +
					"		item_sk, user_id) t1" +
					"	JOIN tweetByProd t2" +
					"	ON t1.item_sk = t2.item_sk"
			
			stmt.executeUpdate(query_randSuffVec)
			
			val drop_initalRank = "DROP TABLE IF EXISTS initialRank"
			val create_initialRank = "CREATE TABLE initialRank (item_sk int, user_id int, rank_score float)"
				
			stmt.execute(drop_initalRank)
			stmt.execute(create_initialRank)
			
			val query_initialRank = "INSERT INTO initialRank" +
					"	SELECT t1.item_sk, user_id, 1/num_users" +
					"	FROM " +
					"		(SELECT item_sk, COUNT(DISTINCT user_id) as num_users" +
					"		FROM relevantTweet" +
					"		GROUP BY" +
					"		item_sk) t1 " +
					"	JOIN mentionProb" +
					"	ON t1.item_sk = mentionProb.item_sk"
			stmt.executeUpdate(query_initialRank)

			LOG.info("Begin to compute TwitterRank")
			val alpha = 0.85
			for(iteration <- 1 to num_iter) {
						
				val twitterRank_previous = if(iteration == 1) "initialRank" else "twitterRank"+(iteration-1)	
				val drop_twitterRank = "DROP VIEW IF EXISTS twitterRank"+iteration
				val create_twitterRank = "CREATE VIEW twitterRank"+iteration+" (item_sk, user_id, rank_score) AS" +
						"	SELECT t4.item_sk, t4.user_id, " +
						"		CASE WHEN sum_follower_score > 0 THEN " + alpha + " * sum_follower_score + " + (1-alpha) +" * prob" +
						"			 ELSE " + (1-alpha) +" * prob" +
						"		END" +
						"	FROM" +
						"		(SELECT t1.item_sk, follower_id, sum(transit_prob * rank_score) as sum_follower_score" +
						"		FROM transitMatrix t1, " + twitterRank_previous +" t2" +
						"		WHERE t1.friend_id = t2.user_id AND t1.item_sk = t2.item_sk " +
						"		GROUP BY " +
						"		t1.item_sk, follower_id) t3" +
						"	RIGHT JOIN randSUffVec t4" +
						"	ON t3.item_sk = t4.item_sk AND t3.follower_id = t4.user_id"
			
				stmt.execute(drop_twitterRank)
				stmt.execute(create_twitterRank)
				
			}
			LOG.info("Finish computing TwitterRank")
			
			val drop_RptSAProdCmpn = "DROP TABLE IF EXISTS RptSAProdCmpn"
			val create_RptSAProdCmpn = "CREATE TABLE RptSAProdCmpn (promo_id char(16), item_sk int, totalsales float , total_sentiment float)"
					
			stmt.execute(drop_RptSAProdCmpn)
			stmt.execute(create_RptSAProdCmpn)
			
			val query_RptSAProdCmpn = "INSERT INTO RptSAProdCmpn" +
					"	SELECT promo_id, t4.item_sk, totalsales, total_sentiment" +
					"	FROM " +
					"		(SELECT t1.item_sk, sum(rank_score * sentiment_score) as total_sentiment" +
					"		FROM senAnalyse t1, twitterRank" + num_iter + " t2" +
					"		WHERE t1.item_sk = t2.item_sk AND t1.user_id = t2.user_id" +
					"		GROUP BY" +
					"		t1.item_sk) t3" +
					"	JOIN RptSalesByProdCmpn t4 " +
					"	ON t3.item_sk = t4.item_sk"				
			
			if (stmt.executeUpdate(query_RptSAProdCmpn) > 0) {
				LOG.info("Finish the Report Sales Sentiment workflow")
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
	
	override def runVertica(connection: Connection): Boolean = {
		
		if(jsonAsString) return verticaImpl2(connection) else return verticaImpl1(connection)
		
	}
}