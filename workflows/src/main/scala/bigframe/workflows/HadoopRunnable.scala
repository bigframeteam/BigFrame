package bigframe.workflows

import java.io.IOException

import org.apache.hadoop.conf.Configuration

/**
 * Implement this interface such that a query can be run on hadoop.
 * 
 * @author andy
 *
 */
trait HadoopRunnable {

	/**
	 * Run true is the query finish normally.
	 */
	def run(mapred_config: Configuration = null): java.lang.Boolean
}
