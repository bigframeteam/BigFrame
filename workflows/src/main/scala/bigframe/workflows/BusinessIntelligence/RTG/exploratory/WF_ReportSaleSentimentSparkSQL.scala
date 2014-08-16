package bigframe.workflows.BusinessIntelligence.RTG.exploratory

//import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.hive.HiveContext
import java.util.concurrent.{Executors, ExecutorService}
import java.util.concurrent.Future
import java.sql.Connection
import java.sql.SQLException
import bigframe.workflows.Query
import bigframe.workflows.runnable.HadoopRunnable
import bigframe.workflows.runnable.VerticaRunnable
import bigframe.workflows.runnable.HiveRunnable
import bigframe.workflows.runnable.SharkRunnable
import bigframe.workflows.runnable.SparkSQLRunnable
import bigframe.workflows.BaseTablePath
import bigframe.workflows.BusinessIntelligence.relational.exploratory.PromotedProdHadoop
import bigframe.workflows.BusinessIntelligence.relational.exploratory.ReportSalesHadoop
import bigframe.workflows.BusinessIntelligence.text.exploratory.FilterTweetHadoop
import bigframe.workflows.BusinessIntelligence.text.exploratory.SenAnalyzeHadoop
import bigframe.workflows.BusinessIntelligence.text.exploratory.SenAnalyzeConstant
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConversions._
import bigframe.workflows.events.BigFrameListenerBus
import bigframe.workflows.events.WorkflowStartedEvent
import bigframe.workflows.events.WorkflowCompletedEvent
import bigframe.workflows.events.ComponentStartedEvent
import bigframe.workflows.events.ComponentCompletedEvent
import bigframe.workflows.events.QueryStartedEvent
import bigframe.workflows.events.QueryCompletedEvent

class WF_ReportSaleSentimentSparkSQL(basePath: BaseTablePath, num_iter: Int, val useOrc: Boolean) extends Query  with SparkSQLRunnable {

  def printDescription(): Unit = {}


  def prepareHiveTables(hc: HiveContext): Unit = {
    val tablePreparator = new PrepareTable_SparkSQL(basePath)

    if(useOrc == true) {
   		    tablePreparator.prepareTableImpl2(hc)
	    }
	    else {
	        tablePreparator.prepareTableImpl1(hc)
	    }

  }

  def cleanUpHiveImpl1(hc: HiveContext): Unit = {

    //val stmt = connection.createStatement()	

    val list_drop = Seq("DROP TABLE IF EXISTS promotionSelected",
      "DROP TABLE IF EXISTS promotedProduct",
      "DROP TABLE IF EXISTS RptSalesByProdCmpn",
      "DROP TABLE IF EXISTS relevantTweet",
      "DROP TABLE IF EXISTS senAnalyse",
      "DROP TABLE IF EXISTS tweetByUser",
      "DROP TABLE IF EXISTS tweetByProd",
      "DROP TABLE IF EXISTS sumFriendTweets",
      "DROP TABLE IF EXISTS mentionProb",
      "DROP TABLE IF EXISTS simUserByProd",
      "DROP TABLE IF EXISTS transitMatrix",
      "DROP TABLE IF EXISTS randSuffVec",
      "DROP TABLE IF EXISTS initialRank")

    list_drop.foreach(hc.hql(_))
    for (iteration <- 1 to num_iter) {

      val drop_twitterRank = "DROP TABLE IF EXISTS twitterRank" + iteration

      //stmt.execute(drop_twitterRank)
      hc.hql(drop_twitterRank)

    }
  }

  def cleanUpHive(hc: HiveContext): Unit = { 
    //cleanUpHiveImpl1(connection)
  }

  def runHiveImpl1(hc: HiveContext, eventBus: BigFrameListenerBus): Boolean = {
    
    eventBus.post(new WorkflowStartedEvent("SparkSQL"));

    eventBus.post(new ComponentStartedEvent("filter promotions", "spark"))

	val drop_promotionSelected = "DROP TABLE IF EXISTS promotionSelected"
	val create_promotionSelected = "CREATE TABLE promotionSelected (promo_id string, item_sk int," +
		"start_date_sk int, end_date_sk int)"

	/**
	* Choose all promotion except those contain NULL value.
	*/
	val query_promotionSelected = "INSERT INTO TABLE promotionSelected" +
		" SELECT p_promo_id, p_item_sk, p_start_date_sk, p_end_date_sk " +
		" FROM promotion " +
		" WHERE p_item_sk IS NOT NULL AND p_start_date_sk IS NOT NULL AND p_end_date_sk IS NOT NULL"

	eventBus.post(new QueryStartedEvent(query_promotionSelected, "spark"))

	hc.hql(drop_promotionSelected)
	hc.hql(create_promotionSelected)
	hc.hql(query_promotionSelected)

	eventBus.post(new QueryCompletedEvent(query_promotionSelected, "spark"))
    eventBus.post(new ComponentCompletedEvent("filter promotions", "spark"))
    eventBus.post(new ComponentStartedEvent("filter products", "spark"))

    val drop_promotedProduct = "DROP TABLE IF EXISTS promotedProduct"
	val drop_promotedProduct_v = "DROP VIEW IF EXISTS promotedProduct"
     /*val create_promotedProduct = "CREATE TABLE promotedProduct (item_sk string, product_name string, start_date_sk string, end_date_sk string) AS" +
		" SELECT i_item_sk, i_product_name, start_date_sk, end_date_sk " +
		" FROM item JOIN promotionSelected " +
		" ON item.i_item_sk = promotionSelected.item_sk" +
		" WHERE i_product_name IS NOT NULL"*/

	val create_promotedProduct = "CREATE TABLE promotedProduct (item_sk string, product_name string, start_date_sk string, end_date_sk string)"

	val insert_promotedProduct = "INSERT INTO TABLE promotedProduct SELECT i_item_sk, i_product_name, start_date_sk, end_date_sk " +
                " FROM item JOIN promotionSelected " +
                " ON item.i_item_sk = promotionSelected.item_sk" +
                " WHERE i_product_name IS NOT NULL"

	eventBus.post(new QueryStartedEvent(insert_promotedProduct, "spark"))

	hc.hql(drop_promotedProduct_v)
	hc.hql(drop_promotedProduct)
	hc.hql(create_promotedProduct)
	hc.hql(insert_promotedProduct)

	eventBus.post(new QueryCompletedEvent(insert_promotedProduct, "spark"))
    eventBus.post(new ComponentCompletedEvent("filter products", "spark"))
    eventBus.post(new ComponentStartedEvent("sales report", "spark"))
        
	val drop_RptSalesByProdCmpn = "DROP TABLE IF EXISTS RptSalesByProdCmpn"
 	val drop_RptSalesByProdCmpn_v = "DROP VIEW IF EXISTS RptSalesByProdCmpn"
	/*val create_RptSalesByProdCmpn = "CREATE VIEW RptSalesByProdCmpn (promo_id, item_sk, totalsales) AS" +
		" SELECT promotionSelected.promo_id, promotionSelected.item_sk, sum(price*quantity) as totalsales " +
		" FROM" +
		" (SELECT ws_sold_date_sk as sold_date_sk, ws_item_sk as item_sk, ws_sales_price as price, ws_quantity as quantity " +
		" FROM web_sales " +
		" UNION ALL" +
		" SELECT ss_sold_date_sk as sold_date_sk, ss_item_sk as item_sk, ss_sales_price as price, ss_quantity as quantity " +
		" FROM store_sales" +
		" UNION ALL" +
		" SELECT cs_sold_date_sk as sold_date_sk, cs_item_sk as item_sk, cs_sales_price as price, cs_quantity as quantity" +
		" FROM catalog_sales) sales" +
		" JOIN promotionSelected " +
		" ON sales.item_sk = promotionSelected.item_sk" +
		" WHERE " +
		" promotionSelected.start_date_sk <= sold_date_sk " +
		" AND" +
		" sold_date_sk <= promotionSelected.end_date_sk" +
		" GROUP BY" +
		" promotionSelected.promo_id, promotionSelected.item_sk "*/


 	val create_RptSalesByProdCmpn = "CREATE TABLE RptSalesByProdCmpn (promo_id string, item_sk string, totalsales string)"
   	val insert_RptSalesByProdCmpn = "INSERT INTO TABLE RptSalesByProdCmpn" +
                " SELECT promotionSelected.promo_id, promotionSelected.item_sk, sum(price*quantity) as totalsales " +
                " FROM" +
                " (SELECT ws_sold_date_sk as sold_date_sk, ws_item_sk as item_sk, ws_sales_price as price, ws_quantity as quantity " +
                " FROM web_sales " +
                " UNION ALL" +
                " SELECT ss_sold_date_sk as sold_date_sk, ss_item_sk as item_sk, ss_sales_price as price, ss_quantity as quantity " +
                " FROM store_sales" +
                " UNION ALL" +
                " SELECT cs_sold_date_sk as sold_date_sk, cs_item_sk as item_sk, cs_sales_price as price, cs_quantity as quantity" +
                " FROM catalog_sales) sales" +
                " JOIN promotionSelected " +
                " ON sales.item_sk = promotionSelected.item_sk" +
                " WHERE " +
                " promotionSelected.start_date_sk <= sold_date_sk " +
                " AND" +
                " sold_date_sk <= promotionSelected.end_date_sk" +
                " GROUP BY" +
                " promotionSelected.promo_id, promotionSelected.item_sk "

	eventBus.post(new QueryStartedEvent(insert_RptSalesByProdCmpn, "spark"))

	hc.hql(drop_RptSalesByProdCmpn)
	hc.hql(drop_RptSalesByProdCmpn_v)
	hc.hql(create_RptSalesByProdCmpn)
	hc.hql(insert_RptSalesByProdCmpn)

	eventBus.post(new QueryCompletedEvent(insert_RptSalesByProdCmpn, "spark"))
    eventBus.post(new ComponentCompletedEvent("sales report", "spark"))
    eventBus.post(new ComponentStartedEvent("filter tweets", "spark"))
		
	val drop_relevantTweet = "DROP TABLE IF EXISTS relevantTweet"
	//val drop_t1 = "DROP TABLE IF EXISTS t1"
	//val drop_t3 = "DROP TABLE IF EXISTS t3"
	val create_relevantTweet = "CREATE TABLE relevantTweet" +
		" (item_sk int, user_id int, text string)"

	val query_relevantTweet	= " INSERT INTO TABLE relevantTweet" +
" SELECT item_sk, user_id, text" +
" FROM " +
" (SELECT user.id as user_id, text, created_at, entities.hashtags[0] as hashtag" +
" FROM tweets" +
" WHERE size(entities.hashtags) > 0 ) t1 " +
" JOIN " +	
" (SELECT item_sk, product_name, start_date, d_date as end_date" +
" FROM " +
" (SELECT item_sk, product_name, d_date as start_date, end_date_sk" +
" FROM promotedProduct JOIN date_dim" +
" ON promotedProduct.start_date_sk = date_dim.d_date_sk) t2 " +
" JOIN date_dim " +
" ON t2.end_date_sk = date_dim.d_date_sk) t3" +
" ON t1.hashtag = t3.product_name" +
" WHERE isWithinDate(created_at, start_date, end_date)"     
        /*
	val t1_create = "CREATE TABLE t1" +
		" (user_id int, text string, created_at string, hashtag string)"

	val t1_insert = "INSERT INTO TABLE t1 " + 
		" SELECT user.id as user_id, text, created_at, entities.hashtags[0] as hashtag" +
		" FROM tweets" +
		" WHERE entities IS NOT NULL and entities.hashtags IS NOT NULL and size(entities.hashtags) > 0" 
	
	val t3_create = "CREATE TABLE t3" +
		"(item_sk int, product_name string, start_date string, end_date string)"

	val t3_insert = "INSERT INTO TABLE t3 SELECT item_sk, product_name, start_date, d_date as end_date FROM (SELECT item_sk, product_name, d_date as start_date, end_date_sk FROM promotedProduct JOIN date_dim ON promotedProduct.start_date_sk = date_dim.d_date_sk WHERE start_date_sk is not NULL and end_date_sk is not NULL AND d_date_sk is not NULL) t2 JOIN date_dim ON t2.end_date_sk = date_dim.d_date_sk WHERE d_date_sk is not NULL "

	val query_relevantTweet = "INSERT INTO TABLE relevantTweet" +
		" SELECT item_sk, user_id, text" +
		" FROM t1 JOIN t3 " + 
		" ON t1.hashtag = t3.product_name" + 
		" WHERE isWithinDate(created_at, start_date, end_date)"
*/

	eventBus.post(new QueryStartedEvent(query_relevantTweet, "spark"))

	hc.hql(drop_relevantTweet)
	hc.hql(create_relevantTweet)
	hc.hql(query_relevantTweet)

	eventBus.post(new QueryCompletedEvent(query_relevantTweet, "spark"))
    eventBus.post(new ComponentCompletedEvent("filter tweets", "spark"))
    eventBus.post(new ComponentStartedEvent("sentiment analysis", "spark"))
		
	val drop_senAnalyse = "DROP TABLE IF EXISTS senAnalyse"
	val drop_senAnalyse_v = "DROP VIEW IF EXISTS senAnalyse"
	val create_senAnalyse = "CREATE TABLE senAnalyse (item_sk string, user_id string, sentiment_score string)" 
	val insert_senAnalyse =	"INSERT INTO TABLE senAnalyse SELECT item_sk, user_id, sum(sentiment(text)) as sum_score" +
		" FROM relevantTweet" +
		" GROUP BY" +
		" item_sk, user_id"

	eventBus.post(new QueryStartedEvent(insert_senAnalyse, "spark"))

	hc.hql(drop_senAnalyse)
	hc.hql(drop_senAnalyse_v)
	hc.hql(create_senAnalyse)
	hc.hql(insert_senAnalyse)

	eventBus.post(new QueryCompletedEvent(insert_senAnalyse, "spark"))
    eventBus.post(new ComponentCompletedEvent("sentiment analysis", "spark"))
    eventBus.post(new ComponentStartedEvent("transition matrices", "spark"))
		
	val drop_tweetByUser = "DROP TABLE IF EXISTS tweetByUser"
	val create_tweetByUser = "CREATE TABLE tweetByUser (user_id int, num_tweets int)"

	val query_tweetByUser = "INSERT INTO TABLE tweetByUser" +
		" SELECT user_id, count(*)" +
		" FROM relevantTweet" +
		" GROUP BY" +
		" user_id"

	eventBus.post(new QueryStartedEvent(query_tweetByUser, "spark"))

	hc.hql(drop_tweetByUser)
	hc.hql(create_tweetByUser)
	hc.hql(query_tweetByUser)

	eventBus.post(new QueryCompletedEvent(query_tweetByUser, "spark"))

	val drop_tweetByProd = "DROP TABLE IF EXISTS tweetByProd"
	val create_tweetByProd = "CREATE TABLE tweetByProd (item_sk int, num_tweets int)"

	val query_tweetByProd = "INSERT INTO TABLE tweetByProd" +
		" SELECT item_sk, count(*)" +
		" FROM relevantTweet" +
		" GROUP BY" +
		" item_sk"

	eventBus.post(new QueryStartedEvent(query_tweetByProd, "spark"))

	hc.hql(drop_tweetByProd)
	hc.hql(create_tweetByProd)
	hc.hql(query_tweetByProd)

	eventBus.post(new QueryCompletedEvent(query_tweetByProd, "spark"))

	val drop_sumFriendTweets = "DROP TABLE IF EXISTS sumFriendTweets"
	val drop_sumFriendTweets_v = "DROP VIEW IF EXISTS sumFriendTweets"
	val drop_f = "DROP TABLE IF EXISTS f"
	val drop_result = "DROP TABLE IF EXISTS result"
	/*val create_sumFriendTweets = "CREATE TABLE sumFriendTweets (user_id int, num_friend_tweets int)" 
	val insert_sumFriendTweets = 	" INSERT INTO TABLE sumFriendTweets SELECT user_id, " +
		" CASE WHEN num_friend_tweets > 0 THEN num_friend_tweets" +
		" ELSE 0" +
		" END" +
		" FROM" +
		" (SELECT user_id, sum(friend_tweets) as num_friend_tweets" +
		" FROM tweetByUser LEFT OUTER JOIN" +
		" (SELECT follower_id, friend_id, num_tweets as friend_tweets" +
		" FROM tweetByUser JOIN twitter_graph" +
		" ON tweetByUser.user_id = twitter_graph.friend_id) f" +
		" ON tweetByUser.user_id = f.follower_id" +
		" GROUP BY " +
		" user_id) result"*/
	/*val insert_sumFriendTweets =  " INSERT INTO TABLE sumFriendTweets SELECT user_id, " +
                " CASE WHEN num_friend_tweets > 0 THEN num_friend_tweets" +
                " ELSE 0L" +
                " END" +
                " FROM" +
                " (SELECT user_id, sum(friend_tweets) as num_friend_tweets" +
                " FROM tweetByUser LEFT OUTER JOIN" +
                " (SELECT follower_id, friend_id, num_tweets as friend_tweets" +
                " FROM tweetByUser JOIN twitter_graph" +
                " ON tweetByUser.user_id = twitter_graph.friend_id) f" +
                " ON tweetByUser.user_id = f.follower_id" +
                " GROUP BY " +
                " user_id) result"*/

/*	val create_sumFriendTweets = "CREATE TABLE sumFriendTweets AS SELECT user_id as follower_id, (CASE WHEN num_friend_tweets > 0 THEN num_friend_tweets ELSE 0L END) as num_friend_tweets FROM (SELECT user_id, sum(friend_tweets) as num_friend_tweets FROM tweetByUser LEFT OUTER JOIN (SELECT follower_id, friend_id, num_tweets as friend_tweets FROM tweetByUser JOIN twitter_graph ON tweetByUser.user_id = twitter_graph.friend_id) f ON tweetByUser.user_id = f.follower_id GROUP BY  user_id) result"
*/

val f_create = "CREATE TABLE f (follower_id int, friend_id int, friend_tweets int)"

val f_insert = "INSERT INTO TABLE f SELECT follower_id, friend_id, num_tweets as friend_tweets FROM tweetByUser JOIN twitter_graph ON tweetByUser.user_id = twitter_graph.friend_id"

val result_create = "CREATE TABLE result (user_id int, num_friend_tweets int)"

val result_insert = "SELECT user_id, sum(f.num_tweets) FROM tweetByUser LEFT OUTER JOIN f ON tweetByUser.user_id = f.follower_id GROUP BY user_id"

val create_sumFriendTweets = "CREATE TABLE sumFriendTweets AS SELECT user_id as follower_id, (CASE WHEN num_friend_tweets > 0 THEN num_friend_tweets ELSE 0L END) as num_friend_tweets FROM result"


	hc.hql(drop_sumFriendTweets)
	hc.hql(drop_sumFriendTweets_v)
	hc.hql(drop_f)
	hc.hql(drop_result)

	hc.hql(f_create)
	eventBus.post(new QueryStartedEvent(f_insert, "spark"))
	hc.hql(f_insert)
	eventBus.post(new QueryCompletedEvent(f_insert, "spark"))

	hc.hql(result_create)
	eventBus.post(new QueryStartedEvent(result_create, "spark"))
	hc.hql(result_insert)
	eventBus.post(new QueryCompletedEvent(result_create, "spark"))
	
	eventBus.post(new QueryStartedEvent(create_sumFriendTweets, "spark"))
	hc.hql(create_sumFriendTweets)
	eventBus.post(new QueryCompletedEvent(create_sumFriendTweets, "spark"))

	val drop_mentionProb = "DROP TABLE IF EXISTS mentionProb"
	val create_mentionProb = "CREATE TABLE mentionProb (item_sk int, user_id int, prob float)"

	val query_mentionProb = "INSERT INTO TABLE mentionProb" +
		" SELECT item_sk, tweetByUser.user_id, r.num_tweets/tweetByUser.num_tweets" +
		" FROM tweetByUser JOIN " +
		" (SELECT item_sk, user_id, count(*) as num_tweets" +
		" FROM relevantTweet" +
		" GROUP BY" +
		" item_sk, user_id) r" +
		" ON tweetByUser.user_id = r.user_id"

	eventBus.post(new QueryStartedEvent(query_mentionProb, "spark"))

	hc.hql(drop_mentionProb)
	hc.hql(create_mentionProb)
	hc.hql(query_mentionProb)

	eventBus.post(new QueryCompletedEvent(query_mentionProb, "spark"))

	val drop_simUserByProd = "DROP TABLE IF EXISTS simUserByProd"
	val drop_simUserByProd_v = "DROP VIEW IF EXISTS sumUserByProd"
	val create_simUserByProd = "CREATE TABLE simUserByProd " +
		" (item_sk string, follower_id string, friend_id string, similarity string)" 
	val insert_simUserByProd = 	"INSERT INTO TABLE simUserByProd SELECT f.item_sk, follower_id, friend_id, (1 - ABS(follower_prob - prob)) as similarity" +
		" FROM " +
		" (SELECT item_sk, follower_id, friend_id, prob as follower_prob" +
		" FROM mentionProb JOIN twitter_graph " +
		" ON mentionProb.user_id = twitter_graph.follower_id) f" +
		" JOIN mentionProb " +
		" ON f.friend_id = mentionProb.user_id AND f.item_sk=mentionProb.item_sk"

	eventBus.post(new QueryStartedEvent(insert_simUserByProd, "spark"))

	hc.hql(drop_simUserByProd)
	hc.hql(drop_simUserByProd_v)
	hc.hql(create_simUserByProd)
	hc.hql(insert_simUserByProd)

	eventBus.post(new QueryCompletedEvent(insert_simUserByProd, "spark"))

	val drop_transitMatrix = "DROP TABLE IF EXISTS transitMatrix"
	val create_transitMatrix = "CREATE TABLE transitMatrix (item_sk int, follower_id int, friend_id int, transit_prob float)"

	val query_transitMatrix = "INSERT INTO TABLE transitMatrix" +
		" SELECT item_sk, follower_id, friend_id, " +
		" CASE WHEN follower_id != friend_id THEN num_tweets/num_friend_tweets*similarity" +
		" ELSE 0.0" +
		" END" +
		" FROM" +
		" (SELECT item_sk, t1.follower_id, friend_id, similarity, num_friend_tweets" +
		" FROM simUserByProd t1 JOIN sumFriendTweets t2" +
		" ON t1.follower_id = t2.follower_id) t3" +
		" JOIN tweetByUser" +
		" ON t3.friend_id = tweetByUser.user_id"

	eventBus.post(new QueryStartedEvent(query_transitMatrix, "spark"))

	hc.hql(drop_transitMatrix)
	hc.hql(create_transitMatrix)
	hc.hql(query_transitMatrix)

	eventBus.post(new QueryCompletedEvent(query_transitMatrix, "spark"))

	val drop_randSufferVec = "DROP TABLE IF EXISTS randSuffVec"
	val create_randSuffVec = "CREATE TABLE randSuffVec (item_sk int, user_id int, prob float)"

	val query_randSuffVec = "INSERT INTO TABLE randSuffVec " +
	" SELECT t1.item_sk, user_id, t1.num_tweets/t2.num_tweets" +
	" FROM" +
	" (SELECT item_sk, user_id, count(*) as num_tweets" +
	" FROM relevantTweet" +
	" GROUP BY" +
	" item_sk, user_id) t1" +
	" JOIN tweetByProd t2" +
	" ON t1.item_sk = t2.item_sk"

	eventBus.post(new QueryStartedEvent(query_randSuffVec, "spark"))

	hc.hql(drop_randSufferVec)
	hc.hql(create_randSuffVec)
	hc.hql(query_randSuffVec)

	eventBus.post(new QueryCompletedEvent(query_randSuffVec, "spark"))
	eventBus.post(new ComponentCompletedEvent("transition matrices", "spark"))
	eventBus.post(new ComponentStartedEvent("twitter rank", "spark"))

	val drop_initalRank = "DROP TABLE IF EXISTS initialRank"
	val create_initialRank = "CREATE TABLE initialRank (item_sk int, user_id int, rank_score float)"

	val query_initialRank = "INSERT INTO TABLE initialRank" +
		" SELECT t2.item_sk, user_id, 1.0/num_users as rank_score" +
		" FROM " +
		" (SELECT item_sk, count(*) as num_users" +
		" FROM" +
		" (SELECT item_sk, user_id" +
		" FROM relevantTweet" +
		" GROUP BY" +
		" item_sk, user_id) t1" +
		" GROUP BY" +
		" item_sk) t2 JOIN mentionProb" +
		" ON t2.item_sk = mentionProb.item_sk"

	eventBus.post(new QueryStartedEvent(query_initialRank, "spark"))

	hc.hql(drop_initalRank)
	hc.hql(create_initialRank)
	hc.hql(query_initialRank)

	eventBus.post(new QueryCompletedEvent(query_initialRank, "spark"))

	val alpha = 0.85
	for (iteration <- 1 to num_iter) {

	val twitterRank_previous = if (iteration == 1) "initialRank" else "twitterRank" + (iteration - 1)
	val drop_twitterRank = "DROP TABLE IF EXISTS twitterRank" + iteration
	val create_twitterRank = "CREATE TABLE twitterRank" + iteration + " (item_sk int, user_id int, rank_score float)"

	val query_twitterRank = "INSERT INTO TABLE twitterRank" + iteration +
	  " SELECT t4.item_sk, t4.user_id, " +
	  " CASE WHEN sum_follower_score > 0 THEN " + alpha + " * sum_follower_score + " + (1 - alpha) + " * prob" +
	  " ELSE " + (1 - alpha) + " * prob" +
	  " END" +
	  " FROM" +
	  " (SELECT t1.item_sk, follower_id, sum(transit_prob * rank_score) as sum_follower_score" +
	  " FROM transitMatrix t1 JOIN " + twitterRank_previous + " t2" +
	  " ON t1.friend_id = t2.user_id AND t1.item_sk = t2.item_sk " +
	  " GROUP BY " +
	  " t1.item_sk, follower_id) t3" +
	  " RIGHT OUTER JOIN randSUffVec t4" +
	  " ON t3.item_sk = t4.item_sk AND t3.follower_id = t4.user_id"

	eventBus.post(new QueryStartedEvent(query_twitterRank, "spark"))

	hc.hql(drop_twitterRank)
	hc.hql(create_twitterRank)
	hc.hql(query_twitterRank)

	eventBus.post(new QueryCompletedEvent(query_twitterRank, "spark"))
	}

    eventBus.post(new ComponentCompletedEvent("twitter rank", "spark"))
    eventBus.post(new ComponentStartedEvent("report generation", "spark"))
		
	val drop_RptSAProdCmpn = "DROP TABLE IF EXISTS RptSAProdCmpn"
	val create_RptSAProdCmpn = "CREATE TABLE RptSAProdCmpn (promo_id string, item_sk int, totalsales float , total_sentiment float)"

	val twitterRank = "twitterRank" + num_iter

	val query_RptSAProdCmpn = "INSERT INTO TABLE RptSAProdCmpn" +
	" SELECT promo_id, t.item_sk, totalsales, total_sentiment" +
	" FROM " +
	" (SELECT senAnalyse.item_sk, sum(rank_score * sentiment_score) as total_sentiment" +
	" FROM senAnalyse JOIN " + twitterRank +
	" ON senAnalyse.item_sk = " + twitterRank + ".item_sk " +
	" AND senAnalyse.user_id = " + twitterRank + ".user_id" +
	" GROUP BY" +
	" senAnalyse.item_sk) t" +
	" JOIN RptSalesByProdCmpn " +
	" ON t.item_sk = RptSalesByProdCmpn.item_sk"

	eventBus.post(new QueryStartedEvent(query_RptSAProdCmpn, "spark"))

	hc.hql(drop_RptSAProdCmpn)
	hc.hql(create_RptSAProdCmpn)
	hc.hql(query_RptSAProdCmpn)

	eventBus.post(new QueryCompletedEvent(query_RptSAProdCmpn, "spark"))

    eventBus.post(new ComponentCompletedEvent("report generation", "spark"))
		
    eventBus.post(new WorkflowCompletedEvent("SparkSQL"));
	
	return true
  }

  /**
   * Run the benchmark query
   */
  def runSparkSQL(hc: HiveContext, eventBus: BigFrameListenerBus): Boolean = {
    return runHiveImpl1(hc, eventBus)
  }

}
