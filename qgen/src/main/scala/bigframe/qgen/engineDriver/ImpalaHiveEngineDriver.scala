package bigframe.qgen.engineDriver

import bigframe.bigif.WorkflowInputFormat
import bigframe.bigif.BigConfConstants;
import bigframe.workflows.runnable.ImpalaHiveRunnable

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement

object ImpalaHiveEngineDriver {
	val driverName = "org.apache.hive.jdbc.HiveDriver";
	val LOG  = LogFactory.getLog(classOf[ImpalaHiveEngineDriver]);
}

class ImpalaHiveEngineDriver(workIF: WorkflowInputFormat) extends EngineDriver(workIF) {

	var impala_connect: Connection = _
	var hive_connect: Connection = _
	var queries = List[ImpalaHiveRunnable]()
	
	
	def numOfQueries(): Int = queries.length

	
	def addQuery(query: ImpalaHiveRunnable) = {queries = query :: queries}
	


	def init(): Unit = {
		
		try {
			Class.forName(ImpalaEngineDriver.driverName)
			

			ImpalaHiveEngineDriver.LOG.info("Connectiong to JDBC server for Impala!!!");
			impala_connect = DriverManager.getConnection(workIF.getImpalaJDBCServer(), "", "");
    	  
			ImpalaHiveEngineDriver.LOG.info("Connectiong to JDBC server for Hive!!!");
			hive_connect = DriverManager.getConnection(workIF.getHiveJDBCServer(), workIF.getHiveJDBCUserName(), 
					workIF.getHiveJDBCPassword());
			
			if(impala_connect == null || hive_connect == null) {
				ImpalaHiveEngineDriver.LOG.error("Cannot connect to JDBC server! " +
						"Make sure the HiveServer is running!")
				System.exit(1);
			}
			else
				ImpalaHiveEngineDriver.LOG.info("Successful!!!")
			
			val UDF_JAR = workIF.getProp().get(BigConfConstants.BIGFRAME_UDF_JAR);
			
			val hive_stmt = hive_connect.createStatement()
			
			hive_stmt.execute("create temporary function sentiment as \'bigframe.workflows.util.SenExtractorHive\'");
			hive_stmt.execute("create temporary function isWithinDate as \'bigframe.workflows.util.WithinDateHive\'");
			
			if(!workIF.getSkipPrepareTable())
				queries.foreach{
					ImpalaHiveEngineDriver.LOG.info("Prepare tables...")
					_.prepareImpalaHiveTables(impala_connect, hive_connect)
				}
		
		} catch  {
			case e: ClassNotFoundException => ImpalaHiveEngineDriver.LOG.error("Impala JDBC Driver " +
					"is not found in the classpath")
			case e: SQLException => e.printStackTrace()
		}
		
	}

	def run(): Unit = {
		
		ImpalaHiveEngineDriver.LOG.info("Running Impala+Hadoop queries...")
		
		queries.foreach(q => {
				if(q.runImpalaHive(impala_connect, hive_connect))
					ImpalaHiveEngineDriver.LOG.info("Query Finished")
				else
					ImpalaHiveEngineDriver.LOG.info("Query failed")
			})
	}

	def cleanup(): Unit = {
		queries.foreach{
			ImpalaHiveEngineDriver.LOG.info("Clean up Impala+Hadoop tables...")
			_.cleanUpImpalaHive(impala_connect, hive_connect)
		}
		
	}

}