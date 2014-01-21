package bigframe.qgen.engineDriver

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

import bigframe.bigif.BigConfConstants
import bigframe.bigif.WorkflowInputFormat
import bigframe.workflows.runnable.SharkBagelRunnable

import shark.SharkContext
import shark.SharkEnv

import org.apache.spark.SparkContext

class SharkBagelEngineDriver(workIF: WorkflowInputFormat) extends EngineDriver(workIF) {

	var queries: List[SharkBagelRunnable] = scala.List()
	
	val LOG = LogFactory.getLog(classOf[SharkBagelEngineDriver]);
	
//	val scClassName = classOf[SharkContext].getName
//	
//	val scclass = Class.forName("shark.SharkContext")
	
	override def numOfQueries(): Int = {
		// TODO Auto-generated method stub
		return queries.length
	}

	def addQuery(query: SharkBagelRunnable) = {
		queries = query :: queries
	}
	
	def initSharkContext(): SharkContext = {
		/**
		 * Initialize the shark context.
		 */
		
//		val sparkMaster = workIF.getSparkMaster()
//		val appName = "BigFrame Benchmark"
//		val sparkHome = workIF.getSparkHome()
//		
//		val	jar_path_string = System.getenv(BigConfConstants.WORKFLOWS_JAR)
//		
//		val sparkContext = new SparkContext(sparkMaster, appName, 
//						sparkHome, Seq(jar_path_string))
//		
//		SharkEnv.sc = sparkContext
//		
////		SharkEnv.initWithSharkContext(appName)
//		val sc = SharkEnv.initWithSharkContext("test")
		
		SharkEnv.initWithSharkContext("shark-example")
		val sc = SharkEnv.sc.asInstanceOf[SharkContext]
		
		return sc
	}
	
	override def init() = {
		
		LOG.info("Prepare Shark+Bagel Tables");
		
		try {
			Class.forName("shark.SharkContext")
			val sc = initSharkContext()
			queries.foreach(q => q.prepareSharkBagelTables(sc))
			
		} catch {
			case e: ClassNotFoundException => LOG.error("Shark jar is not found in the classpath")
		}
	}
	
	override def run() = {
		LOG.info("Running Shark+Bagel Query");
		
		try {
			Class.forName("shark.SharkContext")
			val sc = initSharkContext()
			queries.foreach(q => {
				if(q.runSharkBagel(sc))
					LOG.info("Query Finished")
				else
					LOG.info("Query failed")
			})
			
		} catch {
			case e: ClassNotFoundException => LOG.error("Shark jar is not found in the classpath")
		}
		
	}

	override def cleanup() = {
		// TODO Auto-generated method stub

	}

}
