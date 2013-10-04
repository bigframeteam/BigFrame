/**
 *
 */
package bigframe.spark.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._
import bigframe.spark.text.TextExecutor
import scala.collection.mutable.ArrayBuffer

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
   * Transition matrix giving probabilities of transition to a friend for 
   * every product
   */
  var transition: RDD[((Int, Int), Seq[(String,Double)])] = null
  
  /**
   * Teleportation vectors for every product
   */
  var teleport: RDD[(Int, Seq[(String, Double)])] = null
  
  /**
   * TwitterRank of users for every product
   */
  var twitterRank: RDD[(String, Seq[(String, Double)])] = null
  
  def read() = {
    sc.textFile(graph_path) map {t => t.split('|')}
  }
  
  def readTweets(regex: String) = {
    textDriver.read filter (t => t.products(0).matches(regex))
  }
  
  /**
   * Builds transition matrix and teleportation matrix by reading graph 
   * relationships and tweets about products specified by 'regex'
   */
  def buildMatrices(regex: String) = {

    val tweetsByUser = readTweets(regex) map {t => t.userID -> 1} reduceByKey (
        (a,b) => (a + b))
            
    val friends = utils.filterFriends(read map {t => t(0).toInt -> t(1).toInt}, 
        tweetsByUser) groupByKey () cache
    
    val ratios = utils.ratios(friends) 
    
    val counts = readTweets(regex) map {t => (
        (t.userID, t.productID) -> 1)} reduceByKey ((a,b) => (a + b))
        
    val countsByUser = friends join (counts map {
          t => (t._1._1 -> (t._1._2, t._2))} groupByKey ()) mapValues (
              _._2) cache

    val similarity = utils.similarity(countsByUser)
     
    transition = utils.transitionProbabilities(ratios, similarity)
    
    val countsPerProduct = countsByUser flatMap {
            t => t._2 map (s => s)} reduceByKey ((a,b) => (a + b)) cache
        
    teleport = utils.teleportProbabilities(
        countsByUser, countsPerProduct) cache
    
    transition

  }
  
  def microBench(regex: String = ".*") = {
    
    buildMatrices(regex)
    
  }
  
}