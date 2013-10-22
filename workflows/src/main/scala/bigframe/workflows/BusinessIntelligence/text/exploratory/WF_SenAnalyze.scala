/**
 *
 */
package bigframe.workflows.BusinessIntelligence.text.exploratory

import java.util.concurrent.{Executors, ExecutorService}
import java.util.concurrent.Future

import bigframe.workflows.Query
import bigframe.workflows.runnable.HadoopRunnable
import bigframe.workflows.BaseTablePath
//import bigframe.workflows.BusinessIntelligence.text.exploratory.SenAnalyzeHadoop;

import org.apache.hadoop.conf.Configuration

/**
 * A Class can run sentiment analysis on different engines.
 * 
 * @author andy
 *
 */
class WF_SenAnalyze(basePath : BaseTablePath) extends Query with HadoopRunnable {

	val tweet_dir = basePath.nested_path
	
	def printDescription(): Unit = {}

	/**
	 * Use a executor to submit the query. It is useful when
	 * the query is a complex DAG.
	 */
	override def runHadoop(mapred_config: Configuration): java.lang.Boolean = {
		val SenAnalyzeHadoop = new SenAnalyzeHadoop(tweet_dir, mapred_config)
		
		val pool: ExecutorService = Executors.newFixedThreadPool(1)
				
		val future = pool.submit(SenAnalyzeHadoop)
		pool.shutdown()
		
		if(future.get()) true else false
	}
}