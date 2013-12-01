package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._
import bigframe.workflows.BaseTablePath
import bigframe.workflows.runnable.SparkRunnable
import bigframe.workflows.BusinessIntelligence.relational.exploratory.WF_ReportSalesSpark
import bigframe.workflows.BusinessIntelligence.text.exploratory.WF_SenAnalyzeSpark
import bigframe.workflows.BusinessIntelligence.RTG.graph.TwitterRankDriver
import bigframe.workflows.BusinessIntelligence.RTG.graph.GraphUtilsSpark
import bigframe.workflows.util.DateUtils

class WF_MacroRTGSpark(val basePath: BaseTablePath, val numIter: Int, val useBagel: Boolean, val dop: Integer, val optimizeMemory: Boolean) extends SparkRunnable {
    final var OUTPUT_PATH = "OUTPUT_PATH"
    private var output_path: String = System.getenv(OUTPUT_PATH) + "/spark/macro_rel_nested_graph"
  
	def runSpark(sc: SparkContext) : Boolean = {
      //TODO: make the fuction return false when exceptions are catched.
      
      val tpcdsExecutor: WF_ReportSalesSpark = new WF_ReportSalesSpark(basePath)
      val textExecutor: WF_SenAnalyzeSpark = new WF_SenAnalyzeSpark(basePath)
      val graphExecutor: TwitterRankDriver = new TwitterRankDriver(basePath)

      tpcdsExecutor.setSparkContext(sc)
      textExecutor.setSparkContext(sc)
      graphExecutor.setSparkContext(sc)
	  
	  // get promotion table with key being item id
	  val promotions = tpcdsExecutor promotedProducts
	  
	  /** 
	   *  relational processing to get total sales for each product
	   *  tuples : item_sk -> (promo_id, sales) 
	   *  can be done in a separate thread
	   */
	  val sales = tpcdsExecutor salesPerPromotion promotions
	  
	  // read all tweets
	  val allTweets = textExecutor.read().filter(t => t.products != null && t.products.length > 0)
	  
	  // filter tweets by items relevant to promotions
	  // tuples item_sk -> tweet
	  val relevantTweets = (allTweets map (t => (t.products(0), t)) coalesce(dop)).join(
	      promotions map (t => (t._2(4), t._1)), dop).map(t => t._2._1)
	      
	  val relevantTweetsWithItem = (relevantTweets map (
	      t => (t.products(0), t))).join(promotions map (
	          t => (t._2(4), t._1)), dop).map(t => t._2._1 -> t._2._2)
	 
	  /**
	   *  Do graph processing on all the relevant tweets
	   *  can be done in a separate thread
	   */  
	  val twitterRanks = graphExecutor.microBench(
	      relevantTweetsWithItem, numIter, useBagel, dop, optimizeMemory)
	  
	  // run sentiment analysis
	  val scoredTweets = textExecutor addSentimentScore relevantTweets
	  
	  val scoredTweetsWithUser = (scoredTweets map (
	      t => (t.products(0), t))).join(promotions map (
	          t => (t._2(4), t._1)), dop).map(t => t._2._2 -> t._2._1).map { t => 
	            t._2.userID -> 
	            (t._1, t._2.products(0), t._2.created_at, t._2.score)
	  }
	  
	  // Use influence scores to weigh sentiment scores
	  // tuples (product_name -> (creation_date, score))
	  val weightedTweets = scoredTweetsWithUser join twitterRanks map {
	    t => t._2._1._2 -> (t._2._1._3, 
	        t._2._1._4 * GraphUtilsSpark().influence(t._2._2, t._2._1._1))
	  }
	  
	  // read date_dim and cache it
	  val tpcdsDates = tpcdsExecutor.dateTuples().cache()
	  
	  // replace date_sk fields in promotions with actual dates
	  // tuples (product_name -> (promotion))
	  val promoDates = tpcdsExecutor.promotionsWithDates(promotions, tpcdsDates)
	  
	  val dateUtils = new DateUtils()
	  
	  // join promotion with tweets, filter tweets not within promotion dates
	  // tuples (product_name -> (promo_sk, score))
	  val sentimentsPerPromotion = promoDates.join(weightedTweets, dop)
	  .mapValues (t => (t._1(1), t._1(2), t._1(3), t._2._1, t._2._2))
	  .filter (t => (dateUtils.isDateWithin(t._2._4, t._2._2, t._2._3)))
	  .mapValues (t => (t._1, t._5))
	  
  
	  // aggregate sentiment values for every promotion
	  val aggSentiments = sentimentsPerPromotion.reduceByKey(
	      (a, b) => (a._1, a._2 + b._2), dop)

	  /**
	   *  join relational output with text output
	   *  sales result is (item_id, (promotion_id, total_sales)) and 
	   *  sentiment result is (product_name, (promotion_id, total_sentiment)
	   *  TODO: Do a outer join
	   */ 
	  val joinedResult = (sales map {t => t._2._1 -> t._2._2}).join(
	      aggSentiments map {t => t._2._1 -> (t._1, t._2._2)}, dop).map {
		t => t._1 -> (t._2._2._1, t._2._1, t._2._2._2)}

	  // save the output to hdfs
	  println("Workflow executed, writing the output to: " + output_path)
	  joinedResult.saveAsTextFile(output_path)

	  return true
	}

}
