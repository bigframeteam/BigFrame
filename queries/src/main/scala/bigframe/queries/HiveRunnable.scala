package bigframe.queries

import java.sql.Connection

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
	def prepareTables(connection: Connection): Unit
	
	/**
	 * Run the benchmark query
	 */
	def run(connection: Connection): Unit
}
