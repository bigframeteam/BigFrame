package bigframe.workflows.runnable

import java.sql.Connection
import shark.{SharkContext, SharkEnv}

/**
 * Implement this if you need the query run in shark+Bagel.
 * 
 * @author andy
 */
trait SharkBagelRunnable {

	/**
	 * Prepare the base tables before actually run the shark query
	 */
	def prepareSharkBagelTables(sc: SharkContext): Unit
	
	/**
	 * Run the benchmark querysc: SharkContext
	 */
	def runSharkBagel(sc: SharkContext): Boolean
	
	
	def cleanUpSharkBagel(sc: SharkContext): Unit
}