package bigframe.workflows.runnable

import java.sql.Connection
import bigframe.workflows.events.BigFrameListenerBus

/**
 * Implement this interface such that a query can be run on hive.
 * 
 * @author andy
 *
 */
trait HiveRunnable {
	
	/*
	 * Prepeare the basic tables before run the Hive query
	 */
	def prepareHiveTables(connection: Connection): Unit
	
	/**
	 * Run the benchmark query
	 */
	def runHive(connection: Connection, 
	    eventBus: BigFrameListenerBus): Boolean
	
	def cleanUpHive(connection: Connection): Unit
}
