package bigframe.workflows.runnable

import java.sql.Connection

/**
 * Implement this interface such that a query can be run on Vertica.
 * 
 * @author andy
 */
trait VerticaRunnable {

	/**
	 * Prepare the base tables before actually run the vertica query
	 */
	def prepareVerticaTables(connection: Connection): Unit
	
	/**
	 * Run the benchmark query
	 */
	def runVertica(connection: Connection): Boolean
	
	def cleanUpVertica(connection: Connection): Unit
}