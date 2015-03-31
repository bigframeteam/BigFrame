package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import bigframe.workflows.runnable.ImpalaRunnable
import bigframe.workflows.Query
import bigframe.workflows.BaseTablePath

import java.sql.Connection

class WF_ReportSaleSentimentImpala(basePath: BaseTablePath) extends Query with ImpalaRunnable {

	def prepareImpalaTables(connection: Connection): Unit = {
		val preparor = new PrepareTable_Impala(basePath)
		
		preparor.prepareTableTextFileFormat(connection)
		
	}

	def runImpala(connection: Connection): Boolean = { 
		
		try {
		val stmt = connection.createStatement()
	
		

		
		val res = stmt.executeQuery("SELECT count(*) FROM web_sales")
		
		while (res.next()) {
			 System.out.println(res.getInt(1))
		}
//		while(results.next()) {
//			println(results)
//		}
		
		} catch {
			case e: java.sql.SQLException => e.printStackTrace()
		}
		true 
		
	}

	def cleanUpImpala(connection: Connection): Unit = {}

	def printDescription(): Unit = {}

}