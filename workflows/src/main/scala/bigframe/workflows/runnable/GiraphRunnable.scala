package bigframe.workflows.runnable

import org.apache.hadoop.conf.Configuration

/**
 * Implement this interface such that a query can be run on Giraph.
 * 
 * @author andy
 */
trait GiraphRunnable {
	
	/*
	 * Run the benchmark query
	 */
	def runGiraph(mapred_config: Configuration): Boolean
}