package bigframe.qgen.engineDriver

import bigframe.bigif.WorkflowInputFormat
import bigframe.bigif.BigConfConstants;
import bigframe.workflows.runnable.ImpalaRunnable

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement


object ImpalaEngineDriver {
	val driverName = "org.apache.hive.jdbc.HiveDriver";
	val LOG  = LogFactory.getLog(classOf[ImpalaEngineDriver]);
}

class ImpalaEngineDriver(workIF: WorkflowInputFormat) extends EngineDriver(workIF) {

	var connection: Connection = _
	var queries = List[ImpalaRunnable]()
	
	
	def numOfQueries(): Int = queries.length

	
	def addQuery(query: ImpalaRunnable) = {queries = query :: queries}
		
	
	def init(): Unit = {
		
		try {
			Class.forName(ImpalaEngineDriver.driverName)
			

			ImpalaEngineDriver.LOG.info("Connectiong to Impala JDBC server!!!");
			connection = DriverManager.getConnection(workIF.getImpalaJDBCServer(), "hdfs", "");
    	  
			if(connection == null) {
				ImpalaEngineDriver.LOG.error("Cannot connect to JDBC server! " +
						"Make sure the HiveServer is running!")
				System.exit(1);
			}
			else
				ImpalaEngineDriver.LOG.info("Successful!!!")
			
			val UDF_JAR = workIF.get(BigConfConstants.BIGFRAME_UDF_JAR);
			
			val stmt = connection.createStatement()
//			stmt.execute("DELETE JAR " + UDF_JAR)
//			ImpalaEngineDriver.LOG.info("Adding UDF JAR " + UDF_JAR + " to hive server")
//			if(stmt.execute("ADD JAR " + UDF_JAR)) {
//				ImpalaEngineDriver.LOG.info("Adding UDF JAR successful!")
//				stmt.execute("create temporary function sentiment as \'bigframe.workflows.util.SenExtractorHive\'")
//				stmt.execute("create temporary function isWithinDate as \'bigframe.workflows.util.WithinDateHive\'")
////				stmt.execute("set hive.auto.convert.join=false");
//				stmt.execute("set mapred.reduce.tasks=40")
//			}
//			else {
//				ImpalaEngineDriver.LOG.error("Adding UDF JAR failed!")
//			}
			
			if(!workIF.getSkipPrepareTable())
				queries.foreach{
					ImpalaEngineDriver.LOG.info("Prepare tables...")
					_.prepareImpalaTables(connection)
				}
		
		} catch  {
			case e: ClassNotFoundException => ImpalaEngineDriver.LOG.error("Impala JDBC Driver is not found in the classpath")
			case e: SQLException => e.printStackTrace()
		}
	}


	def run(): Unit = {
		queries.foreach{
			ImpalaEngineDriver.LOG.info("Running Impala Query...")
			_.runImpala(connection)
		}
		
	}

	def cleanup(): Unit = {}

}
