package bigframe.spark.text

import com.codahale.jerkson.Json._

import spark.SparkContext
import SparkContext._

import bigframe.sentiment.NaiveSentimentExtractor

/*
 Class to read tweets in JSon format.
*/
class TweetReader(val sc:SparkContext, val path:String) {

   /**
    * Reads tweets from specified path
    * Returns: RDD of all tweets 
    */
   def read(): Array[Tweet] = {
       val tweetFile = sc.textFile(path)

       val tweetsRDD = tweetFile.map(line => parse[Tweet](line))

       tweetsRDD.collect()
   }

   /**
    * Run sentiment analysis on all tweets
    */
   def addSentimentScore(tweets: Array[Tweet]): Array[Tweet] = {
       val extractor = new NaiveSentimentExtractor()
       return tweets.map( t => new Tweet(t.text, t.created_at, t.user, extractor.extract(t.text)) )
   }
   
   /**
    * Return an RDD with key being product id and value being Array(creation time, sentiment score)
    */
   def makeRDD[K: ClassManifest, V: ClassManifest](tweets: Array[Tweet]) = {
     sc makeRDD tweets map (t => (t.product_id, (t.created_at, t.sentiment)))
   }
   
   /**
    * Reads all tweets, filters out tweets that do not talk about first 100 products, runs sentiment analysis on the rest
    */
   def microBench() = {
	   val allTweets = read()

	   val filteredTweets = allTweets filter (t => (1 until 100 contains t.product_id.toInt))

	   println("size of filtered tweets: " + filteredTweets.length)
	   
	   val scoredTweets = makeRDD(addSentimentScore(filteredTweets))

	   // add sentiment scores
	   val report = scoredTweets.reduceByKey((a, b) => (a._1, a._2 + b._2)).mapValues(t => t._2)

	  // TODO: remove this part
	   println("**************RESULT**************")
//	   val result = report.collect()
//	   println("size: " + result.length)
//	   println("contents: \n" + result)
	   
	   report
   }
}


