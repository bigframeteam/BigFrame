package bigframe.workflows

import java.util.concurrent.Callable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import bigframe.workflows.runnable.HadoopRunnable

/**
 * All hadoop job should extends from this class.
 */
abstract class HadoopJob(var _mapred_config: Configuration) extends HadoopRunnable with Callable[java.lang.Boolean] {	
	
	/**
	 * Get a new copy of the map reduce configuration
	 */
	var mapred_config = new Configuration(_mapred_config)
	
	
	/**
	 * Submit the job by an executor. The current thread will not be blocked.
	 */
	def call(): java.lang.Boolean = runHadoop(mapred_config)
	
}