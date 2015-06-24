package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import org.apache.spark.sql.hive.HiveContext
import java.sql.Connection
import java.sql.SQLException
import bigframe.workflows.Query
import bigframe.workflows.runnable.MixedRunnable
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
import java.util.Currency

class WF_ReportSaleSentimentMixed(basePath: BaseTablePath, num_iter: Int, 
    val useOrc: Boolean, val engineString: String) extends Query  with MixedRunnable{
  
    val HIVE:String = "hive"
    val SPARK:String = "spark"
    val WORKFLOW_NAME:String = "Combined " + engineString

	def printDescription(): Unit = {}

	
	/*
	 * Prepeare the basic tables before run the query
	 * Using Hive preparator as it is not critical to use spark/hive from performance perspective
	 */
	override def prepareHiveTables(hc: HiveContext, connection: Connection): Unit = {
		val tablePreparatorHive = new PrepareTable_Hive(basePath)
		val tablePreparatorSpark = new PrepareTable_SparkSQL(basePath)
		
	    if(useOrc == true) {
   		    tablePreparatorHive.prepareTableImpl2(connection)
   		    tablePreparatorSpark.prepareTableImpl2(hc)
	    }
	    else {
	        tablePreparatorHive.prepareTableImpl1(connection)
   		    tablePreparatorSpark.prepareTableImpl1(hc)
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
	
	override def cleanUp(hc: HiveContext, connection: Connection): Unit = {
		//cleanUpHiveImpl1(connection)
	}
	
	def getNestedEngine(): String = {
	  if(engineString == null || engineString.size != 3) {
	    return "";
	  }
	  val c = engineString.charAt(0)
	  if(c == 'H') {
	    return HIVE;
	  }
	  return SPARK;
	}

	def getRelationalEngine(): String = {
	  if(engineString == null || engineString.size != 3) {
	    return "";
	  }
	  val c = engineString.charAt(1)
	  if(c == 'H') {
	    return HIVE;
	  }
	  return SPARK;
	}
	
	def getGraphEngine(): String = {
	  if(engineString == null || engineString.size != 3) {
	    return "";
	  }
	  val c:Char = engineString.charAt(2)
	  if(c == 'H') {
	    return HIVE;
	  }
	  return SPARK;
	}
	
	def runHiveImpl1(hc: HiveContext, connection: Connection, 
	    eventBus: BigFrameListenerBus) : Boolean = {
	  
	  var currentEngine = HIVE

	  eventBus.post(new WorkflowStartedEvent(WORKFLOW_NAME))
			
		try {
			val stmt = connection.createStatement();			

			currentEngine = getRelationalEngine
			eventBus.post(new ComponentStartedEvent("filter promotions", currentEngine))

			val drop_promotionSelected = "DROP TABLE IF EXISTS promotionSelected"
			val create_promotionSelected = "CREATE TABLE promotionSelected (promo_id string, item_sk int," +
					"start_date_sk int, end_date_sk int)"
			
			/**
			 * Choose all promotion except those contain NULL value.
			 */
			val query_promotionSelected = "INSERT INTO TABLE promotionSelected" +
					"	SELECT p_promo_id as promo_id, p_item_sk as item_sk, p_start_date_sk as start_date_sk, p_end_date_sk as end_date_sk " +
					"	FROM promotion " +
					"	WHERE p_item_sk IS NOT NULL AND p_start_date_sk IS NOT NULL AND p_end_date_sk IS NOT NULL"

			eventBus.post(new QueryStartedEvent(query_promotionSelected, currentEngine))

			if(currentEngine.equals(HIVE)) {
				stmt.execute(drop_promotionSelected)
				stmt.execute(create_promotionSelected)
				stmt.execute(query_promotionSelected)
			} else {
				hc.hql(drop_promotionSelected)
				hc.hql(create_promotionSelected)
				hc.hql(query_promotionSelected)
			}

			eventBus.post(new QueryCompletedEvent(query_promotionSelected, currentEngine))
			eventBus.post(new ComponentCompletedEvent("filter promotions", currentEngine))

			currentEngine = getRelationalEngine	//redundant
			eventBus.post(new ComponentStartedEvent("filter products", currentEngine))
			
			val drop_promotedProduct = "DROP VIEW IF EXISTS promotedProduct"
			val drop_promotedProduct_t = "DROP TABLE IF EXISTS promotedProduct"
			val create_promotedProduct = "CREATE TABLE promotedProduct AS" +
					"	SELECT i_item_sk as item_sk, i_product_name as product_name, start_date_sk, end_date_sk " +
					"	FROM item JOIN promotionSelected " +
					"	ON item.i_item_sk = promotionSelected.item_sk" +
					"	WHERE i_product_name IS NOT NULL"

			eventBus.post(new QueryStartedEvent(create_promotedProduct, currentEngine))

			if(currentEngine.equals(HIVE)) {
				stmt.execute(drop_promotedProduct)
				stmt.execute(drop_promotedProduct_t)
				stmt.execute(create_promotedProduct)
			} else {
			  	hc.hql(drop_promotedProduct_t)
			  	hc.hql(drop_promotedProduct)
			  	hc.hql(create_promotedProduct)
			}
							
			eventBus.post(new QueryCompletedEvent(create_promotedProduct, currentEngine))
			eventBus.post(new ComponentCompletedEvent("filter products", currentEngine))

			currentEngine = getRelationalEngine	//redundant
			eventBus.post(new ComponentStartedEvent("sales report", currentEngine))
        			
			val drop_RptSalesByProdCmpn = "DROP VIEW IF EXISTS RptSalesByProdCmpn"
			val drop_RptSalesByProdCmpn_t = "DROP TABLE IF EXISTS RptSalesByProdCmpn"
			val create_RptSalesByProdCmpn = "CREATE TABLE RptSalesByProdCmpn AS" +
					"	SELECT promotionSelected.promo_id as promo_id, promotionSelected.item_sk as item_sk, sum(price*quantity) as totalsales " +
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

			eventBus.post(new QueryStartedEvent(create_RptSalesByProdCmpn, currentEngine))

			if(currentEngine.equals(HIVE)) {
				stmt.execute(drop_RptSalesByProdCmpn)		
				stmt.execute(drop_RptSalesByProdCmpn_t)	
				stmt.execute(create_RptSalesByProdCmpn)
			} else {
			  	hc.hql(drop_RptSalesByProdCmpn)
			  	hc.hql(drop_RptSalesByProdCmpn_t)
			  	hc.hql(create_RptSalesByProdCmpn)
			}
			
			eventBus.post(new QueryCompletedEvent(create_RptSalesByProdCmpn, currentEngine))
			eventBus.post(new ComponentCompletedEvent("sales report", currentEngine))

			currentEngine = getNestedEngine
			eventBus.post(new ComponentStartedEvent("filter tweets", currentEngine))
		
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

			eventBus.post(new QueryStartedEvent(query_relevantTweet, currentEngine))

			if(currentEngine.equals(HIVE)) {
				stmt.execute(drop_relevantTweet)
				stmt.execute(create_relevantTweet)
				stmt.execute(query_relevantTweet)
			} else {
			  	hc.hql(drop_relevantTweet)
			  	hc.hql(create_relevantTweet)
			  	hc.hql(query_relevantTweet)
			}
			
			eventBus.post(new QueryCompletedEvent(query_relevantTweet, currentEngine))
			eventBus.post(new ComponentCompletedEvent("filter tweets", currentEngine))

			currentEngine = getNestedEngine	//redundant
			eventBus.post(new ComponentStartedEvent("sentiment analysis", currentEngine))

			val drop_senAnalyse = "DROP VIEW IF EXISTS senAnalyse"
			val drop_senAnalyse_t = "DROP TABLE IF EXISTS senAnalyse"
			val create_senAnalyse = "CREATE TABLE senAnalyse" +
					"	 AS" +
					"	SELECT item_sk, user_id, sum(sentiment(text)) as sentiment_score" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	item_sk, user_id"
			
			eventBus.post(new QueryStartedEvent(create_senAnalyse, currentEngine))
		
			if(currentEngine.equals(HIVE)) {
				stmt.execute(drop_senAnalyse)
				stmt.execute(drop_senAnalyse_t)
				stmt.execute(create_senAnalyse)
			} else {
			  	hc.hql(drop_senAnalyse)
			  	hc.hql(drop_senAnalyse_t)
			  	hc.hql(create_senAnalyse)
			}
			eventBus.post(new QueryCompletedEvent(create_senAnalyse, currentEngine))
			eventBus.post(new ComponentCompletedEvent("sentiment analysis", currentEngine))

			currentEngine = getGraphEngine
			eventBus.post(new ComponentStartedEvent("transition matrices", currentEngine))
			
			val drop_tweetByUser = "DROP TABLE IF EXISTS tweetByUser"
			val create_tweetByUser = "CREATE TABLE tweetByUser (user_id int, num_tweets int)"
				
			val query_tweetByUser =	"INSERT INTO TABLE tweetByUser" +
					"	SELECT user_id, count(*)" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	user_id"
			
			eventBus.post(new QueryStartedEvent(query_tweetByUser, currentEngine))

			if(currentEngine.equals(HIVE)) {
				stmt.execute(drop_tweetByUser)
				stmt.execute(create_tweetByUser)
				stmt.execute(query_tweetByUser)
			} else {
			  	hc.hql(drop_tweetByUser)
			  	hc.hql(create_tweetByUser)
			  	hc.hql(query_tweetByUser)
			}
					
			eventBus.post(new QueryCompletedEvent(query_tweetByUser, currentEngine))

			val drop_tweetByProd = "DROP TABLE IF EXISTS tweetByProd"
			val create_tweetByProd = "CREATE TABLE tweetByProd (item_sk int, num_tweets int)"
				
			val	query_tweetByProd =	"INSERT INTO TABLE tweetByProd" +
					"	SELECT item_sk, count(*)" +
					"	FROM relevantTweet" +
					"	GROUP BY" +
					"	item_sk"
																																																																																																																		
			eventBus.post(new QueryStartedEvent(query_tweetByProd, currentEngine))

			if(currentEngine.equals(HIVE)) {
				stmt.execute(drop_tweetByProd)
				stmt.execute(create_tweetByProd)
				stmt.execute(query_tweetByProd)
			} else {
			  	hc.hql(drop_tweetByProd)
			  	hc.hql(create_tweetByProd)
			  	hc.hql(query_tweetByProd)
			}

			eventBus.post(new QueryCompletedEvent(query_tweetByProd, currentEngine))
							
			val drop_sumFriendTweets = "DROP VIEW IF EXISTS sumFriendTweets"
			val drop_sumFriendTweets_t = "DROP TABLE IF EXISTS sumFriendTweets"
			val create_sumFriendTweets = "CREATE TABLE sumFriendTweets AS" +
					"	SELECT user_id as follower_id, " +
					"		(CASE WHEN num_friend_tweets > 0 THEN num_friend_tweets" +
					"			 ELSE 0L" +
					"		END) as num_friend_tweets" +
					"	FROM" +
					"		(SELECT user_id, sum(friend_tweets) as num_friend_tweets" +
					"		FROM tweetByUser LEFT OUTER JOIN" +
					"			(SELECT follower_id, friend_id, num_tweets as friend_tweets" +
					"			FROM tweetByUser JOIN twitter_graph" +
					"	 		ON tweetByUser.user_id = twitter_graph.friend_id) f" +
					"		ON tweetByUser.user_id = f.follower_id" +
					"		GROUP BY " +
					"		user_id) result"
				
			eventBus.post(new QueryStartedEvent(create_sumFriendTweets, currentEngine))

			if(currentEngine.equals(HIVE)) {
				stmt.execute(drop_sumFriendTweets)
				stmt.execute(drop_sumFriendTweets_t)
				stmt.execute(create_sumFriendTweets)
			} else {
			  	hc.hql(drop_sumFriendTweets)
			  	hc.hql(drop_sumFriendTweets_t)
			  	hc.hql(create_sumFriendTweets)
			}
			
			eventBus.post(new QueryCompletedEvent(create_sumFriendTweets, currentEngine))
			
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
				
			eventBus.post(new QueryStartedEvent(query_mentionProb, currentEngine))

			if(currentEngine.equals(HIVE)) {
				stmt.execute(drop_mentionProb)
				stmt.execute(create_mentionProb)
				stmt.execute(query_mentionProb)
			} else {
			  	hc.hql(drop_mentionProb)
			  	hc.hql(create_mentionProb)
			  	hc.hql(query_mentionProb)
			}

			eventBus.post(new QueryCompletedEvent(query_mentionProb, currentEngine))
			
			val drop_simUserByProd = "DROP VIEW IF EXISTS simUserByProd"
			val drop_simUserByProd_t = "DROP TABLE IF EXISTS simUserByProd"
			val create_simUserByProd = "CREATE TABLE simUserByProd " +
					"	 AS" +
					"	SELECT f.item_sk as item_sk, follower_id, friend_id, (1 - ABS(follower_prob - prob)) as similarity" +
					"	FROM " +
					"		(SELECT item_sk, follower_id, friend_id, prob as follower_prob" +
					"		FROM mentionProb JOIN twitter_graph " +
					"		ON mentionProb.user_id = twitter_graph.follower_id) f" +
					"	JOIN mentionProb " +
					"	ON	f.friend_id = mentionProb.user_id AND f.item_sk=mentionProb.item_sk"
		
			eventBus.post(new QueryStartedEvent(create_simUserByProd, currentEngine))
			
			if(currentEngine.equals(HIVE)) {
				stmt.execute(drop_simUserByProd)
				stmt.execute(drop_simUserByProd_t)
				stmt.execute(create_simUserByProd)
			} else {
				hc.hql(drop_simUserByProd)
				hc.hql(drop_simUserByProd_t)
				hc.hql(create_simUserByProd)
			}
					
			eventBus.post(new QueryCompletedEvent(create_simUserByProd, currentEngine))		

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
				
			eventBus.post(new QueryStartedEvent(query_transitMatrix, currentEngine))

			if(currentEngine.equals(HIVE)) {
				stmt.execute(drop_transitMatrix)
				stmt.execute(create_transitMatrix)
				stmt.execute(query_transitMatrix)
			} else {
			  	hc.hql(drop_transitMatrix)
			  	hc.hql(create_transitMatrix)
			  	hc.hql(query_transitMatrix)
			}

			eventBus.post(new QueryCompletedEvent(query_transitMatrix, currentEngine))
			
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
				
			eventBus.post(new QueryStartedEvent(query_randSuffVec, currentEngine))

			if(currentEngine.equals(HIVE)) {
				stmt.execute(drop_randSufferVec)
				stmt.execute(create_randSuffVec)
				stmt.execute(query_randSuffVec)
			} else {
			  	hc.hql(drop_randSufferVec)
			  	hc.hql(create_randSuffVec)
			  	hc.hql(query_randSuffVec)
			}
			
			eventBus.post(new QueryCompletedEvent(query_randSuffVec, currentEngine))
			eventBus.post(new ComponentCompletedEvent("transition matrices", currentEngine))

			currentEngine = getGraphEngine	//redundant
			eventBus.post(new ComponentStartedEvent("twitter rank", currentEngine))

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
				
			eventBus.post(new QueryStartedEvent(query_initialRank, currentEngine))

			if(currentEngine.equals(HIVE)) {
				stmt.execute(drop_initalRank)
				stmt.execute(create_initialRank)
				stmt.execute(query_initialRank)
			} else {
			  	hc.hql(drop_initalRank)
			  	hc.hql(create_initialRank)
			  	hc.hql(query_initialRank)
			}
					
			eventBus.post(new QueryCompletedEvent(query_initialRank, currentEngine))

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
			
				eventBus.post(new QueryStartedEvent(query_twitterRank, currentEngine))

				if(currentEngine.equals(HIVE)) {
					stmt.execute(drop_twitterRank)
					stmt.execute(create_twitterRank)
					stmt.execute(query_twitterRank)
				} else {
				  	hc.hql(drop_twitterRank)
				  	hc.hql(create_twitterRank)
				  	hc.hql(query_twitterRank)
				}

				eventBus.post(new QueryCompletedEvent(query_twitterRank, currentEngine))
			}
			
			eventBus.post(new ComponentCompletedEvent("twitter rank", currentEngine))

			currentEngine = getRelationalEngine	// could be anything here
			eventBus.post(new ComponentStartedEvent("report generation", currentEngine))
			
			val drop_RptSAProdCmpn = "DROP TABLE IF EXISTS RptSAProdCmpn"
			val create_RptSAProdCmpn = "CREATE TABLE RptSAProdCmpn (promo_id string, item_sk int, totalsales float , total_sentiment float)"
					
			if(currentEngine.equals(HIVE)) {
				stmt.execute(drop_RptSAProdCmpn)
				stmt.execute(create_RptSAProdCmpn)
			} else {
			  	hc.hql(drop_RptSAProdCmpn)
			  	hc.hql(create_RptSAProdCmpn)
			}
			
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

			eventBus.post(new QueryStartedEvent(query_RptSAProdCmpn, currentEngine))

			if(currentEngine.equals(HIVE)) {
				if (stmt.execute(query_RptSAProdCmpn)) {
					eventBus.post(new QueryCompletedEvent(query_RptSAProdCmpn, currentEngine))
					eventBus.post(new ComponentCompletedEvent("report generation", currentEngine))
					eventBus.post(new WorkflowCompletedEvent(WORKFLOW_NAME));
					stmt.close();
					return true
				}
				else{ 
					eventBus.post(new ComponentCompletedEvent("report generation", currentEngine))
					eventBus.post(new WorkflowCompletedEvent(WORKFLOW_NAME));
					stmt.close();
					return false
				}
			} else {
			  	hc.hql(query_RptSAProdCmpn)
				eventBus.post(new QueryCompletedEvent(query_RptSAProdCmpn, currentEngine))
				eventBus.post(new ComponentCompletedEvent("report generation", currentEngine))
				eventBus.post(new WorkflowCompletedEvent(WORKFLOW_NAME));
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
	override def runMixedFlow(hc: HiveContext, connection: Connection, 
	    eventBus: BigFrameListenerBus): Boolean = {
		
			return runHiveImpl1(hc, connection, eventBus)

	}
	
		
}