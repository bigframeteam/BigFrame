package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import bigframe.workflows.runnable.SparkRunnable
import bigframe.workflows.Query
import bigframe.workflows.BaseTablePath
import bigframe.workflows.BusinessIntelligence.text.exploratory.Entities
import bigframe.workflows.BusinessIntelligence.text.exploratory.User
import bigframe.workflows.BusinessIntelligence.text.exploratory.Tweet

import bigframe.workflows.BusinessIntelligence.graph.exploratory.WF_TwitterRankBagel

import bigframe.workflows.util.DateUtils
import bigframe.workflows.util.TREdge
import bigframe.workflows.util.TRVertex
import bigframe.workflows.util.TRMessage
import bigframe.workflows.util.SenExtractorSimple
import bigframe.workflows.util.SentimentExtractor

import scala.math
import scala.collection.mutable.ArrayBuffer

import com.codahale.jerkson.Json._

class WF_ReportSaleSentimentSpark(basePath: BaseTablePath, hadoop_home: String, numItr: Int, 
		alpha: Double = 0.85, numPartition: Int, useParquet: Boolean = true)
			extends Query with SparkRunnable  with Serializable {

	val itemHDFSPath = basePath.relational_path + "/item"
	val web_salesHDFSPath = basePath.relational_path + "/web_sales"
	val catalog_salesHDFSPath = basePath.relational_path + "/catalog_sales"
	val store_salesHDFSPath = basePath.relational_path + "/store_sales"
	val promotionHDFSPath = basePath.relational_path + "/promotion"
	val customerHDFSPath =  basePath.relational_path + "/customer"
	val date_dimHDFSPath = basePath.relational_path + "/date_dim"
	
	val twitter_grapgHDFSPath = basePath.graph_path
	val tweets_HDFSPath = basePath.nested_path	
	
	
	def printDescription(): Unit = {}
	
	
	override def prepareSpark(sc: SparkContext): Unit = {
		val preparer = new PrepareData_Spark(basePath)
		
		preparer.prepareData(sc)
	}
	
	def runSparkParquet(sc: SparkContext): Boolean = {
		
			
		
		true
	}
	
	def runSparkNoParquet(sc: SparkContext): Boolean = {
			/**
		 * Select all promoted products with this format: (item_sk, (promo_id, date_start_sk, date_end_sk))
		 */
		
		val promotions = sc.textFile(promotionHDFSPath)
							.map( line => {
										val promo = line.split("\\|", -1)
										(promo(4), (promo(1), promo(2), promo(3)))
								}).filter( promo => !promo._1.isEmpty() && !promo._2._2.isEmpty && !promo._2._3.isEmpty)
								.groupBy(_._1).map{case (k, v) => (k, v.map(_._2))}.collect.toMap
								
		val promotions_bc = sc.broadcast(promotions)		
		/**
		 * Get the sales of promoted products with this format: (item_sk, (sold_date_sk, price, quantity))
		 */
		
		val websales =  sc.textFile(web_salesHDFSPath)
							.map( line => {
										val websale = line.split("\\|", -1)
										(websale(3), (websale(0), websale(18), websale(21)))		
							}).filter( websale => !websale._1.isEmpty() && !websale._2._1.isEmpty 
									&& !websale._2._2.isEmpty && !websale._2._3.isEmpty)
								
		val storesales =  sc.textFile(store_salesHDFSPath)
							.map( line => {
										val storesale = line.split("\\|", -1)
										(storesale(2), (storesale(0), storesale(10), storesale(13)))		
							}).filter( storesale => !storesale._1.isEmpty() && !storesale._2._1.isEmpty 
									&& !storesale._2._2.isEmpty && !storesale._2._3.isEmpty)
					
		val catalogsales =  sc.textFile(catalog_salesHDFSPath)
								.map( line => {
											val catalogSale = line.split("\\|", -1)
											(catalogSale(15), (catalogSale(0), catalogSale(18), catalogSale(21)))		
								}).filter( catalogSale => !catalogSale._1.isEmpty() && !catalogSale._2._1.isEmpty 
									&& !catalogSale._2._2.isEmpty && !catalogSale._2._3.isEmpty)
		
		val allsales = websales union storesales union catalogsales
		
		// Filter out those item not in promotion, and then aggregate total sales by (item_sk, promo_id)		
		val sales_report = allsales.filter(sale => promotions_bc.value.contains(sale._1))
									.flatMap( sale => {
											val valGet = promotions_bc.value.get(sale._1).get
											valGet.map(s  => (sale._1, (s, sale._2)))
									})
									// (item_sk, ((promo_id, date_start_sk, date_end_sk), (sold_date_sk, price, quantity)))
									.filter(promo_sale => promo_sale._2._2._1.toInt >= promo_sale._2._1._2.toInt 
										&& promo_sale._2._2._1.toInt <= promo_sale._2._1._3.toInt)
									.map(promo_sale => ((promo_sale._1, promo_sale._2._1._1), 
										promo_sale._2._2._2.toDouble * promo_sale._2._2._3.toDouble))
									.reduceByKey(_+_).coalesce(numPartition) // ((item_sk, promo_id), sales)	
			
		/**
		 *  Get relevant tweets mentioned the promoted products
		 */ 
							
		// Get the name of promoted products with this format: (product_name, (item_sk, date_start_sk, date_end_sk))
		val promotedProduct_name = sc.textFile(itemHDFSPath)
						.map( line => {
									val item = line.split("\\|", -1)
									print(item.length)
									(item(0), item(21))
						}).filter( !_._2.isEmpty).filter( item => promotions_bc.value.contains(item._1)) // (item_sk, (product_name, (promo_id, date_start_sk, date_end_sk)))
						.flatMap( item => {
											val valGet = promotions_bc.value.get(item._1).get
											valGet.map( t => (item._1, (item._2 ,t)) )
						}).map(item => (item._2._1, (item._1, item._2._2._2, item._2._2._3)))

						
//		val promotedProduct_name_bc = sc.broadcast(promotedProduct_name.collect)
		
		// Get the actual date in DD-mm-yyyy format
		val date = sc.textFile(date_dimHDFSPath)
									.map( line => {
										val date = line.split("\\|", -1)
										(date(0), date(2)) // (date_sk, date)
									})					
								
		val promoProduct_join_date = promotedProduct_name
										.map( promo => (promo._2._2, (promo._2._1, promo._2._3, promo._1)) ) //(date_start_sk, (item_sk, date_end_sk, product_name))
										.join(date) // (date_start_sk, ((item_sk, date_end_sk, product_name), date))
										.map( promo => (promo._2._1._2, (promo._2._1._1, promo._2._1._3, promo._2._2))) // (date_end_sk, (item_sk, product_name, date_start))
										.join(date) // (date_end_sk, ((item_sk, product_name, date_start), date))
										.map( promo => (promo._2._1._2, (promo._2._1._1, promo._2._1._3, promo._2._2))) // (product_name, (item_sk, date_start, date_end))
								
		val promoProduct_join_date_sc = sc.broadcast(promoProduct_join_date.groupBy(_._1).map{case (k, v) => (k, v.map(_._2))}.collect.toMap) // Map(product_name, Seq(item_sk, date_start, date_end))
								
		// Get all the relevant tweets with this format: (item_sk, user_id, text)						
		val relevant_tweets = sc.textFile(tweets_HDFSPath)
						.map( line => parse[Tweet](line))
						.filter( tweet => !tweet.entities.hashtags.isEmpty)
						.flatMap( tweet => {
								tweet.entities.hashtags.map( hashtag => (hashtag, (tweet.created_at, 
										tweet.text, tweet.userID)))
						}).filter(tweet => promoProduct_join_date_sc.value.contains(tweet._1))
//						.join(promoProduct_join_date) // (hashtag, ((created_at, text, userID), (item_sk, date_start, date_end)))
						.flatMap( tweet=> {
							val valGet = promoProduct_join_date_sc.value.get(tweet._1).get
							valGet.map(t => (tweet._1, (tweet._2, t)))
						})
						.filter( mentioned_tweet => {
											val dateUtil = new DateUtils()
											dateUtil.isDateWithin(mentioned_tweet._2._1._1, mentioned_tweet._2._2._2, mentioned_tweet._2._2._3)
								})
						.map(relevant_tweet => (relevant_tweet._2._2._1, relevant_tweet._2._1._3.toString, relevant_tweet._2._1._2)) // (item_sk, userID, text)
						.coalesce(numPartition * 10).cache

						
								
		
		/**
		 *  Do preparation jobs for TwitteRrank
		 */ 
						
		val tweetByUser = relevant_tweets.map(relevant_tweet => (relevant_tweet._2, 1)).reduceByKey(_+_).cache // (user_id, total_num_tweets)

		
		val tweetByProd = relevant_tweets.map(relevant_tweet => (relevant_tweet._1, 1)).reduceByKey(_+_).cache // (item_sk, total_num_tweets)

		val twitter_graph = sc.textFile(twitter_grapgHDFSPath).map( line => {
																		val pair = line.split("\\|", -1)
																		(pair(0), pair(1))
																}).cache // (friend_id, follower_id)
		
		val sumFriendTweets = tweetByUser.leftOuterJoin(
										(tweetByUser.join(twitter_graph)) // (friend_id, (num_friend_tweet, follower_id))
										.map( t => (t._2._2, t._2._1)) // (follower_id, num_friend_tweet))
									) // (follower_id, (num_tweet, num_friend_tweet))
									.map( t => {
										t._2._2 match {
											case None => (t._1, 0)
											case Some(x: Int) => (t._1, x)
										}
									})
									.reduceByKey(_+_) // (follower_id, total_friend_tweets)
									
		val mentionProb = relevant_tweets.map( tweet => ((tweet._1, tweet._2), 1))
										.reduceByKey(_+_).map(tweet => (tweet._1._2, (tweet._1._1, tweet._2))) // (user_id, (item_sk, num_tweets))
										.join(tweetByUser) // (user_id, ((item_sk, num_tweet), total_num_tweets))
										.map( t => ((t._2._1._1,t._1), t._2._1._2/t._2._2.toDouble)).cache // ((item_sk. user_id), prob)
									
		val simUserByProd = mentionProb.map(t => (t._1._2, (t._1._1, t._2))).join(twitter_graph.map(t => (t._2, t._1))) // (follower_id, ((item_sk, prob), friend_id))
										.map(t => ((t._2._1._1, t._2._2), (t._1, t._2._1._2) )) // ((item_sk, friend_id), (follower_id, follower_prob))
										.join(mentionProb) // ((item_sk, friend_id), ((follower_id, follower_prob), friend_prob))
										.map( t => (t._2._1._1, (t._1._1, t._1._2, 1 - math.abs(t._2._1._2 - t._2._2)))) // (follower_id, (item_sk, friend_id,  similarity))
										
		val transitMatrix = simUserByProd.join(sumFriendTweets) // (follower_id, ((item_sk, friend_id,  similarity), total_friend_tweets))
										.map( t => (t._2._1._2, (t._1, t._2._1._1, t._2._1._3, t._2._2))) // (friend_id, (follower_id, item_sk, similarity, total_friend_tweets))
										.join(tweetByUser) //(friend_id, ((follower_id, item_sk, similarity, total_friend_tweets), friend_tweets))
										.map(cell => {
											if(cell._2._1._1 == cell._1) ((cell._2._1._2, cell._2._1._1),(cell._1, cell._2._2/(cell._2._1._3*cell._2._1._4)))
											else ((cell._2._1._2, cell._1),(cell._2._1._1, 0.0))
										}).cache // ((item_sk, friend_id), (follower_id, transit_prob))
		
		val randSufferVec = relevant_tweets.map( tweet => ((tweet._1, tweet._2), 1)) // ((item_sk, user_id),1)
											.reduceByKey(_+_)
											.map(t => (t._1._1, (t._1._2, t._2))) // (item_sk, (user_id, total_tweets_for_user_in_item_sk))
											.join(tweetByProd) // (item_sk, (user_id, total_tweets_for_user_in_item_sk), total_tweets_in_item_sk)
											.map(t => ((t._1, t._2._1._1), t._2._1._2/t._2._2.toDouble)).cache // ((item_sk, user_id), rand_jump_prob)
		
		val initialRank  = relevant_tweets.map(tweet => (tweet._1, tweet._2)) // (item_sk, user_id)
											.distinct()
											.groupByKey()
											.map(t => (t._1, t._2.length)) // (item_sk, total_num_tweets)
											.join(mentionProb.map( t=> (t._1._1, (t._1._2, t._2)))) // (item_sk, (total_num_tweets, (user_id, num_tweets)))
											.map(t => ((t._1, t._2._2._1), (t._2._2._2/t._2._1.toDouble))).cache // ((item_sk, user_id), rank)
											
		
		/**
		 *  Compute TwitterRank for each user and product based
		 */ 
		
		var twitterRank_old = initialRank
		var twitterRank_new = initialRank
		
		for ( iter <- 1 to numItr) {
			twitterRank_new = transitMatrix.join(twitterRank_old) // ((item_sk, friend_id), ((follower_id, transit_prob), rank))
												.map(t => ((t._1._1, t._2._1._1), t._2._1._2* t._2._2))
												.reduceByKey(_+_) // ((item_sk, follower_id), sum_rank)
												.rightOuterJoin(randSufferVec) // ((item_sk, follower_id), (sum_rank, rand_jump_prob))
												.map( t => {
													t._2._1 match {
														case None => (t._1, (1- alpha) * t._2._2)
														case Some(x: Double) => (t._1, alpha * x + (1- alpha) * t._2._2)
													}	
												}).cache
			twitterRank_old = twitterRank_new								
		}
											
		
		/**
		 *  Compute the sentiment score and aggregate them with TwitterRank as the weight
		 */ 
		
		val extractor: SentimentExtractor = new SenExtractorSimple
		
		val sentiments = relevant_tweets.map( tweet => ((tweet._1, tweet._2), extractor.getSentiment(tweet._3))) // ((item_sk, user_id), sent_score)
										.reduceByKey(_+_)
										.join(twitterRank_new) // (((item_sk, user_id), (sent_score, rank))
										.map( t => (t._1._1, t._2._1*t._2._2))
										.reduceByKey(_+_) // (item_sk, weighted_sentiment_score)
		
		/**
		 *  Associate the sales and sentiment score for all promoted products
		 */ 
		
		val result = sentiments.join(sales_report.map(t => (t._1._1,(t._1._2, t._2)))) // (item_sk, (sentiment_score, (promo_id, sales)))	
								.map(t => (t._1, t._2._2._1, t._2._2._2,  t._2._1)).coalesce(1)	// (item_sk, promo_id, sales, sentiment_score)	
		
		result.saveAsTextFile("hdfs://dbg12:9000/spark_output")
		
		true 
	}
	
	
	
	def runSpark(sc: SparkContext): Boolean = { 
		
		if(useParquet) return runSparkParquet(sc)	else return runSparkNoParquet(sc)
	
	}



}