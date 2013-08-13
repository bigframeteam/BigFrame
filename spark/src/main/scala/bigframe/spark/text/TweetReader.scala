package bigframe.spark.text

import com.codahale.jerkson.Json._

import spark.SparkContext
import SparkContext._

//import bigframe.sentiment.NaiveSentimentExtractor
import com.hp.hpl.sentimentanalysis.main.SentimentExtractor;

/*
 Class to read tweets in JSon format.
*/
class TweetReader(val sc:SparkContext, val path:String) {

   /**
    * Selectivity constraints
    * TODO: Find a way to specify them
    */
   val productStart = 1
   val productEnd = 100
  
   /**
    * Reads tweets from specified path
    * Returns: RDD of all tweets 
    */
   def read(): Array[Tweet] = {
       val tweetFile = sc.textFile(path)

       val tweetsRDD = tweetFile.map(line => parse[Tweet](line))

       //FIXME: Try to do without collect
       tweetsRDD.collect()
   }

   /**
    * Run sentiment analysis on all tweets
    */
   def addSentimentScore(tweets: Array[Tweet]): Array[Tweet] = {
//       val extractor = new NaiveSentimentExtractor()
       val extractor = new SentimentExtractor()
       return tweets.map( t => new Tweet(t.text, t.created_at, t.user, 
           try{ extractor.extract(t.text).toDouble } catch { case e: Exception => 0.0 }))
   }
   
   /**
    * Return an RDD with key being product id and value being Array(creation time, sentiment score)
    */
   def makeRDD[K: ClassManifest, V: ClassManifest](tweets: Array[Tweet]) = {
     sc makeRDD tweets map (t => (t.product_id, (t.created_at, t.sentiment)))
   }
   
   /**
    * Reads all tweets, filters out tweets that do not mention specified products, runs sentiment analysis on the rest
    * Assumption: Tweet text is prepended with product id
    */
   def microBench() = {
	   val allTweets = read()

	   val selected = productStart until productEnd
	   val filteredTweets = allTweets filter (t => (selected contains t.product_id.toInt))

	   println("size of filtered tweets: " + filteredTweets.length)
	   
	   val scoredTweets = makeRDD(addSentimentScore(filteredTweets))

	   println("All tweets: \n" + scoredTweets.collect())
	   
	   // add sentiment scores
	   val report = scoredTweets.reduceByKey((a, b) => (a._1, a._2 + b._2)).mapValues(t => t._2)
	   
	   report
   }
}


