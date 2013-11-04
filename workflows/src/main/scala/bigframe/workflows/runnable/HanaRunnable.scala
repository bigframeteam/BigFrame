package bigframe.workflows.runnable

import java.sql.Connection

/**
 * Implement this interface such that a query can be run on Hana.
 * 
 * @author Seunghyun Lee
 */

trait HanaRunnable {

	/**
	 * Prepare the base tables before actually run the Hana query
	 */
	def prepareHanaTables(connection: Connection): Unit
	
	/**
	 * Run the benchmark query
	 */
	def runHana(connection: Connection): Boolean
}