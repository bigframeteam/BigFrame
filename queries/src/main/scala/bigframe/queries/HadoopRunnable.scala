package bigframe.queries;

import org.apache.hadoop.conf.Configuration;

/**
 * Implement this interface such that a query can be run on hadoop.
 * 
 * @author andy
 *
 */
trait HadoopRunnable {

	def run(mapred_config: Configuration): Unit
}
