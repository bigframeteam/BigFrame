package bigframe.queries.BusinessIntelligence.relational.exploratory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import bigframe.queries.BaseTablePath
import bigframe.queries.HiveRunnable
import bigframe.queries.SharkRunnable
import bigframe.util.Constants

/**
 * The real implementation of Q1 in BI application domain. 
 * To make it runnable on a specific engine, one should implement
 * the corresponding Runnable interface.
 * 
 * @author andy
 *
 */
class Q1(basePath : BaseTablePath) extends Q1_HiveDialect(basePath : BaseTablePath) with HiveRunnable with SharkRunnable {
	
	override def printDescription(): Unit = {
		// TODO Auto-generated method stub
		
	}


	override def prepareTables(connection: Connection): Unit = {
		try {
			val stmt = connection.createStatement()
			
			prepareBaseTable(stmt)
			
		} catch {
		  
		  case sqle :SQLException => sqle.printStackTrace()

		}
		
	}
	
	
	override def run(connection: Connection): Unit = {
		try {
			val stmt = connection.createStatement();
			
			runBenchQuery(stmt);
			
		} catch {
		  
		  case sqle :SQLException => sqle.printStackTrace()

		}
	}

}
