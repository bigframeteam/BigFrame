package bigframe.spark

import spark.SparkContext
import SparkContext._
import bigframe.spark.relational.MicroQueries
import bigframe.spark.text.TweetReader

class PromotionWorkflow(val sc:SparkContext, val tpcds_path: String, 
    val text_path: String, val graph_path: String) {

	def runWorkflow() = {
	  val tpcdsExecutor: MicroQueries = new MicroQueries(sc, tpcds_path)
	  val textExecutor: TweetReader = new TweetReader(sc, text_path)
	  
	  // get promotion table with key being item id
	  val promotions = tpcdsExecutor.promotionsMappedByItems()
	  
	  /** 
	   *  relational processing to get total sales for each product
	   *  can be done in a separate thread
	   */
	  val sales = tpcdsExecutor.salesPerPromotion(promotions)
	  
	  // read all tweets
	  val allTweets = textExecutor.read()
	  println("No. of all tweets = " + allTweets.length)
	  
	  /**
	   *  TODO: Do graph processing on all the tweets
	   *  can be done in a separate thread
	   */  
	  
	  // filter tweets by items relevant to promotions
	  // val itemList = List(promotion map (t => t._1))
	  // val relevantTweets = allTweets filter (t => itemList.contains(t.product_id) )
	  // println("No. of relevant tweets = " + relevantTweets.length)
	  
	  // run sentiment analysis
	  val scoredTweets = textExecutor makeRDD (textExecutor addSentimentScore allTweets)
	  println("scoredTweets: " + scoredTweets)
	  
	  // TODO: Use influence scores to weigh sentiment scores
	  
	  // join promotion with tweets, filter tweets not within promotion
	  val sentimentsPerPromotion = promotions.join(scoredTweets).mapValues (
	      t => (t._1(1), t._1(2), t._1(3), t._2._1, t._2._2)).filter (
	          t => (t._2._2 <= t._2._4 & t._2._4 <= t._2._3)).mapValues (t => (t._1, t._5))
	  
	  // aggregate sentiment values for every promotion
	  val groupedSentiments = sentimentsPerPromotion.reduceByKey( (a, b) => (a._1, a._2 + b._2) )
	  
	  /**
	   *  join relational output with text output
	   *  sales result is (item_id, (promotion_id, total_sales)) and 
	   *  sentiment result is (item_id, (promotion_id, total_sentiment)
	   */ 
	  val joinedResult = sales.join(groupedSentiments).mapValues( t=> (t._1._1, t._1._2, t._2._2))

	  // TODO: remove this part
	  println("**************RESULT**************")
//	  val result = joinedResult.collect()
//	  println("size: " + result.length)
//	  println("contents: \n" + result)
	  
	  joinedResult
	}

}