package bigframe.spark.text

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._
import com.codahale.jerkson.Json._

import bigframe.sentiment.NaiveSentimentExtractor

class TextExecutor(val sc:SparkContext, val path:String) {

   /**
    * Reads tweets from specified path
    * Returns: RDD of all tweets 
    */
   def read(): RDD[(Tweet)] = {
       val tweetFile = sc.textFile(path)

       val tweetsRDD = tweetFile.map(line => parse[Tweet](line))

       tweetsRDD
   }
      
   /**
    * Run sentiment analysis on all tweets
    */
   def addSentimentScore(tweets: RDD[(Tweet)]): RDD[(Tweet)] = {
     val extractor = new NaiveSentimentExtractor()
     
     return tweets.map(t => new Tweet(t.text, t.id, 
         t.created_at, t.user, t.entities, try{ 
         extractor.extract(t.text).toDouble } catch { 
           case e: Exception => 0.0 }))
   }
   
   /**
    * Reads all tweets, filters out tweets that do not mention specified 
    * products specified in regex, runs sentiment analysis on the qualified ones
    * Assumption: Tweet text is prepended with product id
    */
   def microBench(regex: String = ".*") = {
       // read all tweets
	   val allTweets = read()

	   // filter tweets not talking about specified products
	   val filteredTweets = allTweets filter (t => t.products(0).matches(regex))

	   // extract sentiment for all filtered tweets
	   val scoredTweets = addSentimentScore(filteredTweets) map (
	       t => (t.products(0), (t.created_at, t.score)))

	   // sum up the sentiment scores for each product
	   val report = scoredTweets.reduceByKey((a, b) => (a._1, a._2 + b._2))
	   .mapValues(t => t._2)

	   report
   }
}

