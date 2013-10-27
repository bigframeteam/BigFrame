package bigframe.workflows.BusinessIntelligence.text.exploratory

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._
import com.codahale.jerkson.Json._

import bigframe.workflows.BaseTablePath
import bigframe.workflows.runnable.SparkRunnable
import bigframe.workflows.util.SenExtractorEnum
import bigframe.workflows.util.SenExtractorFactory
import bigframe.workflows.util.SentimentExtractor


class WF_SenAnalyzeSpark(basePath : BaseTablePath) extends SparkRunnable {
    final var OUTPUT_PATH = "OUTPUT_PATH"
    private var output_path: String = System.getenv(OUTPUT_PATH) + "/spark/nested"
    private var sc: SparkContext = _
    private val _tweets_path = basePath.nested_path

    
    /**
    * Reads tweets from specified path
    * Returns: RDD of all tweets 
    */
    def read(): RDD[(Tweet)] = {
        val tweetFile = sc.textFile(_tweets_path)

        val tweetsRDD = tweetFile.map(line => parse[Tweet](line))

        tweetsRDD
    }
   
    /**
    * Run sentiment analysis on all tweets
    */
    def addSentimentScore(tweets: RDD[(Tweet)]): RDD[(Tweet)] = {
    	val extractor = SenExtractorFactory.getSenAnalyze(SenExtractorEnum.SIMPLE)
    	
        return tweets.map( t => new Tweet(t.text, t.id, 
         t.created_at, t.user, t.entities, try{ 
         extractor.getSentiment(t.text).toDouble } catch { 
           case e: Exception => 0.0 }) )
    }
   
    /**
    * Reads all tweets, filters out tweets that do not mention specified 
    * products specified in regex, runs sentiment analysis on the qualified ones
    * Assumption: Tweet text is prepended with product id
    */
    override def runSpark(spark_context: SparkContext): Boolean = {
        try {

            println("Going to run text workflow")

            // set spark context
            setSparkContext(spark_context)

            // read all tweets
            val allTweets = read()

            // filter tweets not talking about specified products
            val regex = "[aA].*"
            val filteredTweets = allTweets filter (t => t.products(0).matches(regex))

            // extract sentiment for all filtered tweets
            val scoredTweets = addSentimentScore(filteredTweets) map (
               t => (t.products(0), (t.created_at, t.score)))

            // sum up the sentiment scores for each product
            val report = scoredTweets.reduceByKey((a, b) => (a._1, a._2 + b._2))
            .mapValues(t => t._2).sortByKey(true)
            //sc makeRDD Array("redundant")

            println("Workflow executed, writing the output to: " + output_path)
            report.saveAsTextFile(output_path)


        } catch{
            case ex: Exception => {
            	ex.printStackTrace()
                return false
            }
        }
        return true
    }

    def setSparkContext(spark_context: SparkContext) {
        sc = spark_context
    }
}