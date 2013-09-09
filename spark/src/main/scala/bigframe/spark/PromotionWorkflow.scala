package bigframe.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._

import bigframe.spark.relational.MicroQueries
import bigframe.spark.text.TweetReader

class PromotionWorkflow(val sc:SparkContext, val tpcds_path: String, 
    val text_path: String, val graph_path: String) {
    
	def runWorkflow() = {
	  val tpcdsExecutor: MicroQueries = new MicroQueries(sc, tpcds_path)
	  val textExecutor: TweetReader = new TweetReader(sc, text_path)
	  
	  // get promotion table with key being item id
	  val promotions = tpcdsExecutor promotionsMappedByItems
	  
	  /** 
	   *  relational processing to get total sales for each product
	   *  can be done in a separate thread
	   */
	  val sales = tpcdsExecutor salesPerPromotion promotions
	  
	  // read all tweets
	  val allTweets = textExecutor.read()
	  
	  /**
	   *  TODO: Do graph processing on all the tweets
	   *  can be done in a separate thread
	   */  
	  
	  // filter tweets by items relevant to promotions
	  val relevantTweets = (allTweets map (t => (t.product_id, t))).join(
	      promotions map (t => (t._1, t))).map(t => t._2._1)
//	  println("Relevant tweets: " + relevantTweets + ", count: " + relevantTweets.count())
	  
	  // run sentiment analysis
	  val scoredTweets = textExecutor addSentimentScore relevantTweets map (
	      t => (t.product_id, (t.created_at, t.sentiment)))
	  
	  // TODO: Use influence scores to weigh sentiment scores
	  
	  // read date_dim and cache it
	  val tpcdsDates = tpcdsExecutor.dateTuples().cache()
	  
	  // replace date_sk fields in promotions with actual dates
	  val promoDates = tpcdsExecutor.promotionsWithDates(promotions, tpcdsDates)
	  
	  val dateUtils = new DateUtils()
	  
	  // join promotion with tweets, filter tweets not within promotion dates
	  val sentimentsPerPromotion = promoDates.join(scoredTweets)
	  .mapValues (t => (t._1(1), t._1(2), t._1(3), t._2._1, t._2._2))
	  .filter (t => (dateUtils.isDateWithin(t._2._4, t._2._2, t._2._3)))
//	  .filter (t => true)
	  .mapValues (t => (t._1, t._5))
	  
  
	  // aggregate sentiment values for every promotion
	  val aggSentiments = sentimentsPerPromotion.reduceByKey( (a, b) => (a._1, a._2 + b._2) )
	  
	  /**
	   *  join relational output with text output
	   *  sales result is (item_id, (promotion_id, total_sales)) and 
	   *  sentiment result is (item_id, (promotion_id, total_sentiment)
	   */ 
	  val joinedResult = sales.join(aggSentiments).mapValues( t=> (t._1._1, t._1._2, t._2._2))

	  joinedResult
	}

}
