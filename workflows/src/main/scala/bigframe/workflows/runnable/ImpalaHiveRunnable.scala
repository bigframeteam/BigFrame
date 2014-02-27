package bigframe.workflows.runnable

import java.sql.Connection

trait ImpalaHiveRunnable {
		/*
	 * Prepeare the basic tables before run the Hive query
	 */
	def prepareImpalaHiveTables(impala_connect: Connection, hive_connect: Connection): Unit
	
	/**
	 * Run the benchmark query
	 */
	def runImpalaHive(impala_connect: Connection, hive_connect: Connection): Boolean
	
	def cleanUpImpalaHive(impala_connect: Connection, hive_connect: Connection): Unit
}