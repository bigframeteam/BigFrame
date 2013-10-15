package bigframe.workflows

import java.sql.Connection

/**
 * Implement this interface such that a query can be run on shark.
 * 
 * @author andy
 */
trait SharkRunnable {
  
	/**
	 * Prepare the base tables before actually run the shark query
	 */
	def prepareTables(connection: Connection): Unit
	
	/**
	 * Run the benchmakr query
	 */
	def run(connection: Connection): Boolean
}