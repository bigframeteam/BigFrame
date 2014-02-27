package bigframe.workflows.runnable

import java.sql.Connection

trait ImpalaRunnable {

		/*
	 * Prepeare the basic tables before run the Hive query
	 */
	def prepareImpalaTables(connection: Connection): Unit
	
	/**
	 * Run the benchmark query
	 */
	def runImpala(connection: Connection): Boolean
	
	def cleanUpImpala(connection: Connection): Unit
}