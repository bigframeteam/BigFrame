package bigframe.workflows.runnable

import org.apache.giraph.conf.GiraphConfiguration

import java.sql.Connection

/**
 * Implement this interface such that a query can be run on Giraph.
 * 
 * @author andy
 */
trait HiveGiraphRunnable {
	
  
	/*
	 * Prepeare the basic tables before run the HiveGiraph query
	 */
	def prepareHiveGiraphTables(connection: Connection): Unit
  
	/*
	 * Run the benchmark query
	 */
	def runHiveGiraph(giraph_config: GiraphConfiguration, connection: Connection): Boolean
}