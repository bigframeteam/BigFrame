package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import java.util.concurrent.{Executors, ExecutorService}
import java.util.concurrent.Future

import bigframe.workflows.Query
import bigframe.workflows.runnable.HadoopRunnable
import bigframe.workflows.BaseTablePath
import bigframe.workflows.BusinessIntelligence.relational.exploratory.PromotedProdHadoop
import bigframe.workflows.BusinessIntelligence.relational.exploratory.ReportSalesHadoop
import bigframe.workflows.BusinessIntelligence.text.exploratory.FilterTweetHadoop

import bigframe.workflows.BusinessIntelligence.text.exploratory.SenAnalyzeHadoop
import bigframe.workflows.BusinessIntelligence.text.exploratory.SenAnalyzeConstant;

import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConversions._

class WF_ReportSaleSentiment(basePath: BaseTablePath) extends Query with HadoopRunnable {

	def printDescription(): Unit = {}

	override def runHadoop(mapred_config: Configuration): java.lang.Boolean = {	
		val promotedSKs = Set(new java.lang.Integer(1), new java.lang.Integer(2), new java.lang.Integer(3))
		val iteration = 2
		
		val job_getPromotedProd = new PromotedProdHadoop(basePath.relational_path, promotedSKs, mapred_config)
		val job_reportSales = new ReportSalesHadoop(basePath.relational_path, promotedSKs, mapred_config )
		val job_filterTweets = new FilterTweetHadoop(basePath.nested_path, mapred_config);
		val job_senAnalyze = new SenAnalyzeHadoop(SenAnalyzeConstant.FILTERED_TWEETS_PATH, mapred_config)
		val job_tweetByUser = new TweetByUserHadoop(mapred_config)
		val job_tweetByProd = new TweetByProductHadoop(mapred_config)
		val job_sumFriendTweet = new SumFriendsTweetsHadoop(basePath.graph_path, mapred_config)
		val job_userPairProb = new UserPairProbHadoop(mapred_config)
		val job_rankSufferVec = new RandSufferVectorHadoop(mapred_config)
		val job_initialRank = new ComputeInitialRankHadoop(mapred_config)
		val job_simBetweenUser = new SimUserByProdHadoop(basePath.graph_path, mapred_config)
		val job_transitMatrix = new TransitMatrixHadoop(basePath.graph_path, mapred_config)
		val job_splitByProd = new SplitByProdHadoopImpl2(mapred_config)
		val job_twitterRank = new TwitterRankImpl2(iteration, mapred_config)
		val job_joinSenAndInflu = new JoinSenAndInfluHadoop(mapred_config)
		val job_groupSenByProd = new GroupSenByProdHadoop(mapred_config)
		val job_joinSaleAnsSen = new JoinSaleAndSenHadoop(mapred_config)

		val pool: ExecutorService = Executors.newFixedThreadPool(4)
				
		
		val future_getPromotedProd = pool.submit(job_getPromotedProd)
		val future_reportSale = pool.submit(job_reportSales)
		
		
		if(future_getPromotedProd.get()) {
			val future_filterTweets = pool.submit(job_filterTweets)
			
			if(future_filterTweets.get()) {
				val future_senAnalyze = pool.submit(job_senAnalyze)
				val future_tweetByUser = pool.submit(job_tweetByUser)
				val future_tweetByProd = pool.submit(job_tweetByProd)
				
				if(future_tweetByUser.get() && future_tweetByProd.get()) {
					val future_sumFriendTweet = pool.submit(job_sumFriendTweet)
					val future_userPairProb = pool.submit(job_userPairProb)
					val future_rankSufferVec = pool.submit(job_rankSufferVec)
					val future_initialRank = pool.submit(job_initialRank)
					
					if(future_sumFriendTweet.get() && future_userPairProb.get()
							&& future_rankSufferVec.get() && future_initialRank.get()) {
						val future_simBetweenUser = pool.submit(job_simBetweenUser)
						
						if(future_simBetweenUser.get()) {
							val future_transitMatrix = pool.submit(job_transitMatrix)
							
							if(future_transitMatrix.get()) {
								val future_splitByProd = pool.submit(job_splitByProd)
								
								if(future_splitByProd.get()) {
									val future_twitterRank = pool.submit(job_twitterRank)
									
									if(future_twitterRank.get() && future_senAnalyze.get()) {
										val future_joinSenAndInflu = pool.submit(job_joinSenAndInflu)
										
										if(future_joinSenAndInflu.get()) {
											val future_groupSenByProd = pool.submit(job_groupSenByProd)
											
											if(future_groupSenByProd.get()) {
												val future_joinSaleAnsSen = pool.submit(job_joinSaleAnsSen)
												
												if(future_joinSaleAnsSen.get()) {
													pool.shutdown()
													return true
												}														
											}
										}					
									}						
								}
							}
						}
					}
				}			
			}
		}
			
		pool.shutdown()
		return false
	}
}