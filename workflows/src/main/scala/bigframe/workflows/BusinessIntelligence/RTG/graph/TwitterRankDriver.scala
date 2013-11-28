/**
 *
 */
package bigframe.workflows.BusinessIntelligence.RTG.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import bigframe.workflows.BusinessIntelligence.text.exploratory.Tweet
import bigframe.workflows.BaseTablePath
import bigframe.workflows.BusinessIntelligence.text.exploratory.WF_SenAnalyzeSpark
import bigframe.workflows.BusinessIntelligence.relational.exploratory.WF_ReportSalesSpark
import bigframe.workflows.BusinessIntelligence.text.exploratory.WF_SenAnalyzeSpark
import bigframe.workflows.BusinessIntelligence.relational.exploratory.WF_ReportSalesSpark
import org.apache.spark.bagel.Bagel


/**
 * @author mayuresh
 *
 */

class TwitterRankDriver(val basePath: BaseTablePath) {

  private val tpcds_path = basePath.relational_path
  private val tweet_path = basePath.nested_path
  private val graph_path = basePath.graph_path
  private var sc: SparkContext = _

  /**
   * Driver to read tweets
   */
  var textDriver: WF_SenAnalyzeSpark = null
  
  /**
   * Driver to read product table
   */
  var tpcdsDriver: WF_ReportSalesSpark = null
  
  /**
   * utility methods
   */
  lazy val utils = new GraphUtilsSpark()

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

  def setSparkContext(spark_context: SparkContext) {
	  sc = spark_context
	  
	  textDriver = new WF_SenAnalyzeSpark(basePath)
	  textDriver.setSparkContext(sc)
	  
	  tpcdsDriver = new WF_ReportSalesSpark(basePath)
	  tpcdsDriver.setSparkContext(sc)
  }  

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
    val tweets = textDriver.read filter (t => t.products(0).matches(regex))
    // add product id to every filtered tweet
    val products = tpcdsDriver.productNamesPerItem map (_.swap)
    tweets map {t => t.products(0) -> t} leftOuterJoin products map {
      case (k, (v, Some(w))) => (v, w) case (k, (v, None)) => (v, "-1")}
  }
  
  /**
   * Builds transition matrix and teleportation matrix by reading graph 
   * relationships and tweets about products specified by 'regex'
   */
  def buildMatrices(tweets: RDD[(Tweet, String)], dop: Integer) = {

    // count number of tweets by each user on products of interest
    val tweetsByUser = tweets map {t => t._1.userID -> 1} coalesce(dop) reduceByKey (
        (a,b) => (a + b))
        
    // twitter graph
    val graph = read map {t => t(0).toInt -> t(1).toInt} coalesce(dop)
            
    // read all graph relationships and find a subgraph induced on users who
    // have tweeted about products of interest
    friends = utils.filterFriends(graph, tweetsByUser) groupByKey () cache
    
    // ratios giving proportion of tweets received from a certain friend 
    // compared to total number of tweets received
    val ratios = utils.ratios(friends) 
    
    // users * products matrix giving counts of tweets 
    val counts = tweets map {t => (
        (t._1.userID, t._2) -> 1)} reduceByKey ((a,b) => (a + b))
        
    // re-organize counts to have key as 'user' and 'product' being pushed to 
    // the value
    val countsByUser = friends join (counts map {
          t => (t._1._1 -> (t._1._2, t._2))} groupByKey ()) mapValues (
              _._2) cache

    // how similar are two users in terms of what they are tweeting about
    // NOTE: Only considering products of interest. Ideally should look at
    // all the tweets, but this operation is too expensive.
    val similarity = utils.similarity(countsByUser, graph)
     
    // transition probabilities as a product of ratios and similarity scores
    transition = utils.transitionProbabilities(ratios, similarity) cache
    
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
  private def computeTR_bagel(numIter:Int = 10, gamma:Double = 0.85, 
      trans: Array[(Int, (Int, Seq[(String, Double)]))], 
      tele: Array[(Int, Seq[(String, Double)])]) = {
    
    val emptyMsgs = sc.parallelize(List[(Int, TRMessage)]())

    val verts = utils.createVertices(transition, teleport) cache
    val partitions = verts.partitions.length
    
//    utils.collectProbabilities(transition, teleport) // hack to use collected probabilities
    val result = Bagel.run(sc, verts, emptyMsgs, new TRCombiner(), partitions)(
        utils.compute(trans, tele, numIter, gamma))
    result map (_._2) map {t => t.id -> t.ranks}
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
   * 3. method to use; either of the standalone or bagel 
   */
  def computeTR(useBagel: Boolean = true, numIter:Int = 10, gamma:Double = 0.85) = {
    if(useBagel) {
      computeTR_bagel(numIter, gamma, null, null)
    }
    else {
    // save transition and teleport matrices as object files
    // TODO: remove these files after twitter rank computation
    val graphPath = basePath.graph_path
    val tempDir = graphPath.take(1 + graphPath.lastIndexOf('/'))
    val transitionPath = graphPath + "transition_matrix"
    println("Saving object file in: " + transitionPath)
    val teleportPath = graphPath + "teleport_matrix"
    println("Saving object file in: " + teleportPath)

    transition map {t => t._1._2 -> (t._1._1, t._2)} saveAsObjectFile(transitionPath)
    teleport saveAsObjectFile(teleportPath)

    // read these object files right back to ensure a shorter lineage for repeated access
    val transitionNew = sc.objectFile[(Int, (Int, Seq[(String, Double)]))](transitionPath).cache
    val teleportNew = sc.objectFile[(Int, Seq[(String, Double)])](teleportPath).cache

      computeTR_standalone(numIter, gamma, transitionNew, teleportNew)
    }

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
  private def computeTR_standalone(numIter:Int = 10, gamma:Double = 0.85,
      trans: RDD[(Int, (Int, Seq[(String, Double)]))], 
      tele: RDD[(Int, Seq[(String, Double)])]) = {
    
    // initial ranks same as teleport
    var ranks = tele
    
//    println("transition: ")
//    transition.collect().foreach(println)

//    println("initial ranks: ")
//    ranks.collect().foreach(println)

    for (iter <- 1 to numIter) {
      ranks = utils.iterateRank(ranks, trans, tele, gamma)
//      println("After iteration " + iter + ", ranks: ")
//      ranks.collect().foreach(println)
    }

    ranks
  }

  def microBench(regex: String = ".*", numIter: Int = 10, useBagel: Boolean = true, dop: Integer = 8) 
  : RDD[(Int, Seq[(String, Double)])] = {
    
    val tweets = readTweets(regex)
    
    microBench(tweets, numIter, useBagel, dop)
    
  }
  
  def microBench(tweets: RDD[(Tweet, String)], numIter: Int, useBagel: Boolean, dop: Integer) 
  : RDD[(Int, Seq[(String, Double)])] = {
    
    buildMatrices(tweets, dop)
    
    computeTR(useBagel, numIter)
    
  }
  
}

