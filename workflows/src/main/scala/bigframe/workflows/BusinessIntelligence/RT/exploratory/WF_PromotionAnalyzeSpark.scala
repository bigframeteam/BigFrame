package bigframe.workflows.BusinessIntelligence.RT.exploratory

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._

import bigframe.workflows.BaseTablePath
import bigframe.workflows.runnable.SparkRunnable
import bigframe.workflows.BusinessIntelligence.relational.exploratory.WF_ReportSalesSpark
import bigframe.workflows.BusinessIntelligence.text.exploratory.WF_SenAnalyzeSpark
import bigframe.workflows.util.DateUtils

class WF_PromotionAnalyzeSpark(basePath : BaseTablePath) extends SparkRunnable {
    final var OUTPUT_PATH = "OUTPUT_PATH"
    private var output_path: String = System.getenv(OUTPUT_PATH) + "/spark/macro_rel_nested"
    
	def runSpark(spark_context: SparkContext): Boolean  = {
		//TODO: make the fuction return false when exceptions are catched.

		val tpcdsExecutor: WF_ReportSalesSpark = new WF_ReportSalesSpark(basePath)
		val textExecutor: WF_SenAnalyzeSpark = new WF_SenAnalyzeSpark(basePath)

		tpcdsExecutor.setSparkContext(spark_context)
		textExecutor.setSparkContext(spark_context)

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
		val relevantTweets = (allTweets map (t => (t.productID, t))).join(
		  promotions map (t => (t._1, t))).map(t => t._2._1)
		//	  println("Relevant tweets: " + relevantTweets + ", count: " + relevantTweets.count())

		// run sentiment analysis
		val scoredTweets = textExecutor addSentimentScore relevantTweets map (
		  t => (t.productID, (t.created_at, t.score)))

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


		// save the output to hdfs
		println("Workflow executed, writing the output to: " + output_path)
		joinedResult.saveAsTextFile(output_path)

		//joinedResult
		//promoDates
	
		return true
	}
}
