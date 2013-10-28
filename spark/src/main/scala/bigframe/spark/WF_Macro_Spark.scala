package bigframe.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._
import bigframe.spark.relational.MicroQueries
import bigframe.spark.text.TextExecutor
import bigframe.spark.graph.TwitterRankDriver
import bigframe.spark.graph.GraphUtils

class WF_Macro_Spark(val sc:SparkContext, val tpcds_path: String, 
    val text_path: String, val graph_path: String) {
  
	def runWorkflow() = {
	  val tpcdsExecutor: MicroQueries = new MicroQueries(sc, tpcds_path)
	  val textExecutor: TextExecutor = new TextExecutor(sc, text_path)
	  val graphExecutor: TwitterRankDriver = new TwitterRankDriver(sc, 
	      graph_path, text_path, tpcds_path)
	  
	  // get promotion table with key being item id
	  val promotions = tpcdsExecutor promotedProducts
	  
	  /** 
	   *  relational processing to get total sales for each product
	   *  tuples : item_sk -> (product_name, sales) 
	   *  can be done in a separate thread
	   */
	  val sales = tpcdsExecutor salesPerPromotion promotions
	  
	  // read all tweets
	  val allTweets = textExecutor.read()
	  
	  // filter tweets by items relevant to promotions
	  // tuples item_sk -> tweet
	  val relevantTweets = (allTweets map (t => (t.products(0), t))).join(
	      promotions map (t => (t._2(4), t._1))).map(t => t._2._1)
	      
	  val relevantTweetsWithItem = (relevantTweets map (
	      t => (t.products(0), t))).join(promotions map (
	          t => (t._2(4), t._1))).map(t => t._2._1 -> t._2._2)
	  
	  /**
	   *  Do graph processing on all the relevant tweets
	   *  can be done in a separate thread
	   */  
	  val twitterRanks = graphExecutor.microBench(relevantTweetsWithItem)
	  
	  // run sentiment analysis
	  val scoredTweets = textExecutor addSentimentScore relevantTweets
	  
	  val scoredTweetsWithUser = (scoredTweets map (
	      t => (t.products(0), t))).join(promotions map (
	          t => (t._2(4), t._1))).map(t => t._2._2 -> t._2._1).map { t => 
	            t._2.userID -> 
	            (t._1, t._2.products(0), t._2.created_at, t._2.score)
	  }
	  
	  // Use influence scores to weigh sentiment scores
	  // tuples (product_name -> (creation_date, score))
	  val weightedTweets = scoredTweetsWithUser join twitterRanks map {
	    t => t._2._1._2 -> (t._2._1._3, 
	        t._2._1._4 * new GraphUtils().influence(t._2._2, t._2._1._1))
	  }
	  
	  // read date_dim and cache it
	  val tpcdsDates = tpcdsExecutor.dateTuples().cache()
	  
	  // replace date_sk fields in promotions with actual dates
	  // tuples (product_name -> (promotion))
	  val promoDates = tpcdsExecutor.promotionsWithDates(promotions, tpcdsDates)
	  
	  val dateUtils = new DateUtils()
	  
	  // join promotion with tweets, filter tweets not within promotion dates
	  // tuples (product_name -> (promo_sk, score))
	  val sentimentsPerPromotion = promoDates.join(weightedTweets)
	  .mapValues (t => (t._1(1), t._1(2), t._1(3), t._2._1, t._2._2))
	  .filter (t => (dateUtils.isDateWithin(t._2._4, t._2._2, t._2._3)))
	  .mapValues (t => (t._1, t._5))
	  
  
	  // aggregate sentiment values for every promotion
	  val aggSentiments = sentimentsPerPromotion.reduceByKey(
	      (a, b) => (a._1, a._2 + b._2))
	       
	  /**
	   *  join relational output with text output
	   *  sales result is (item_id, (product_name, total_sales)) and 
	   *  sentiment result is (product_name, (promotion_id, total_sentiment)
	   *  TODO: Do a outer join
	   */ 
	  val joinedResult = (sales map {t => t._2._1 -> t._2._2}).join(
	      aggSentiments).map {t => t._2._2._1 -> (t._1, t._2._1, t._2._2._2)}

	  joinedResult
//	  promoDates
	}

}
