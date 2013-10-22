package bigframe.workflows.runnable

import java.sql.Connection

/**
 * Implement this interface such that a query can be run on Vertica.
 * 
 * @author andy
 */
trait VerticaRunnable {

	/**
	 * Run the benchmark query
	 */
	def runVertica(connection: Connection): Boolean
}