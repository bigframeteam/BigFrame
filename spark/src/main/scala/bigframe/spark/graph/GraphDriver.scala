/**
 *
 */
package bigframe.spark.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._
import bigframe.spark.text.TextExecutor
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.bagel.Bagel

/**
 * @author mayuresh
 *
 */

class GraphDriver(val sc:SparkContext, val graph_path:String, 
    val tweet_path: String) {

  /**
   * Driver to read tweets
   */
  lazy val textDriver = new TextExecutor(sc, tweet_path)
  
  /**
   * utility methods
   */
  lazy val utils = new GraphUtils()

  /**
   * Graph induced on users who tweeted about the products of interest
   * Value also includes count of number of tweets by friend
   */
  var friends: RDD[(Int, Seq[(Int, Int)])] = null

  /**
   * Transition matrix giving probabilities of transition to a friend for 
   * every product
   */
  var transition: RDD[((Int, Int), Seq[(String,Double)])] = null
  
  /**
   * Teleportation vectors for every user
   */
  var teleport: RDD[(Int, Seq[(String, Double)])] = null
  
  /**
   * TwitterRank of users for every product
   */
  var twitterRank: RDD[(Int, Seq[(String, Double)])] = null
  
  /**
   * Reads graph edges, returns Map(follower -> friend)
   */
  def read() = {
    sc.textFile(graph_path) map {t => t.split('|')}
  }
  
  /**
   * Reads all tweets and filter those talking about products matching 'regex'
   */
  def readTweets(regex: String) = {
    textDriver.read filter (t => t.products(0).matches(regex))
  }
  
  /**
   * Builds transition matrix and teleportation matrix by reading graph 
   * relationships and tweets about products specified by 'regex'
   */
  def buildMatrices(regex: String) = {

    // count number of tweets by each user on products of interest
    val tweetsByUser = readTweets(regex) map {t => t.userID -> 1} reduceByKey (
        (a,b) => (a + b))
            
    // read all graph relationships and find a subgraph induced on users who
    // have tweeted about products of interest
    friends = utils.filterFriends(read map {t => t(0).toInt -> t(1).toInt}, 
        tweetsByUser) groupByKey () cache
    
    // ratios giving proportion of tweets received from a certain friend 
    // compared to total number of tweets received
    val ratios = utils.ratios(friends) 
    
    // users * products matrix giving counts of tweets 
    val counts = readTweets(regex) map {t => (
        (t.userID, t.productID) -> 1)} reduceByKey ((a,b) => (a + b))
        
    // re-organize counts to have key as 'user' and 'product' being pushed to 
    // the value
    val countsByUser = friends join (counts map {
          t => (t._1._1 -> (t._1._2, t._2))} groupByKey ()) mapValues (
              _._2) cache

    // how similar are two users in terms of what they are tweeting about
    // NOTE: Only considering products of interest. Ideally should look at
    // all the tweets, but this operation involves a cartesian product and 
    // hence too expensive.
    val similarity = utils.similarity(countsByUser)
     
    // transition probabilities as a product of ratios and similarity scores
    transition = utils.transitionProbabilities(ratios, similarity)
    
    // number of tweets for each product
    val countsPerProduct = countsByUser flatMap {
            t => t._2 map (s => s)} reduceByKey ((a,b) => (a + b)) cache
       
    // teleportation probabilities given by a fraction of number of tweets 
    // about a product coming from a user compared to total number of tweets 
    // on that product
    teleport = utils.teleportProbabilities(
        countsByUser, countsPerProduct) cache
    
  }
  
  
  /**
   * Computes twitter ranks of every user for each product
   * The users who have not published a single tweet on any product of interest 
   * are not considered in these computations.
   * Prerequisites: 
   * 1. relationship graph induced on 'interesting' users (friends) 
   * 2. transition and teleportation probabilities (transition and teleport)
   * Arguments:
   * 1. number of iterations
   * 2. gamma: parameter controlling transition and teleport contributions
   * Returns twitter ranks for every user and for every product 
   */
  def computeTR(numIter:Int = 20, gamma:Double = 0.85) = {
    
//    val tranProb = transition.collect.toMap 
//    val teleProb = teleport.collect.toMap
    
//    val verts = friends map {t => (t._1, 
//      new TRVertex(t._1, teleProb.getOrElse(t._1, List()), 
//          t._2 map {_._1} toList, true))} cache
      
    val emptyMsgs = sc.parallelize(List[(Int, TRMessage)]())

    utils.initializeCompute(numIter, gamma)
    utils.collectProbabilities(transition, teleport)
    val verts = utils.createVertices(friends) cache
    val partitions = verts.partitions.length
    println("Running Bagel iterative program with number of partitions: " + partitions)
    val result = Bagel.run(sc, verts, emptyMsgs, new TRCombiner, partitions)(
        utils.compute)
        
    result map (_._2) map {t => t.id -> t.ranks}
  }
  
  def microBench(regex: String = ".*") = {
    
    buildMatrices(regex)
    
    computeTR()
    
  }
  
}