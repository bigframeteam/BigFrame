package bigframe.spark

import com.codahale.jerkson.Json._

import spark.SparkContext
import SparkContext._

// import com.hp.hpl.sentimentanalysis.SentimentAnalysis.Attribute;
// import com.hp.hpl.sentimentanalysis.main.SentimentExtractor;

import bigframe.sentiment.NaiveSentimentExtractor;

/*
 Class to read tweets in JSon format.
*/
class TweetReader {

   private var sc:SparkContext = _

   /*
    Argument:
     Spark context
   */
   def init(sparkContext: SparkContext) {
       sc = sparkContext
   }

   /*
    Argument:
     Tweet file(s) path -- "hdfs://<ip/name>:9000/<path_to_file>"

    Returns:
     array of Tweet objects 
   */
   def read(path: String): Array[Tweet] = {
   println("starting to read: " + path)
       val tweetFile = sc.textFile(path)
       println("read file: " + tweetFile)

       val tweetsRDD = tweetFile.map(line => parse[Tweet](line))

       val tweets = tweetsRDD.collect()       

       return tweets
   }

   /*
    Run sentiment analysis on all tweets
   */
   def addSentimentScore(tweets: Array[Tweet]): Array[Tweet] = {
       val extractor = new NaiveSentimentExtractor()
       return tweets.map( t => new Tweet(t.text, t.created_at, extractor.extract(t.text)) )
   }
}


