package bigframe.workflows.runnable

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
	def prepareSharkTables(connection: Connection): Unit
	
	/**
	 * Run the benchmakr query
	 */
	def runShark(connection: Connection): Boolean
}