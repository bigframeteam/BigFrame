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
	private var spark_dop: Integer = workIF.getSparkDoP()
	var queries: List[SharkBagelRunnable] = scala.List()
	
	val LOG = LogFactory.getLog(classOf[SharkBagelEngineDriver]);
	
//	val scClassName = classOf[SharkContext].getName
//	
//	val scclass = Class.forName("shark.SharkContext")

	var sc: SharkContext = null

	override def numOfQueries(): Int = {
		// TODO Auto-generated method stub
		return queries.length
	}

	def addQuery(query: SharkBagelRunnable) = {
		queries = query :: queries
	}
	
	/**
	* Initialize the shark context according to Shark 0.8
	*/
	def initSharkContext(): SharkContext = {
		
		if(sc != null)
			return sc
	
		val	jar_path_string = System.getenv(BigConfConstants.WORKFLOWS_JAR)

		System.setProperty("MASTER", workIF.getSparkMaster())
		System.setProperty("spark.shuffle.consolidateFiles", "true")
		
		SharkEnv.stop
		SharkEnv.initWithSharkContext("BigFrame Benchmark")
		sc = SharkEnv.sc.asInstanceOf[SharkContext]
		sc.addJar(jar_path_string)
		
		sc
	}
	
	override def init() = {
		
		LOG.info("Prepare Shark+Bagel Tables");
		
		try {
			
			Class.forName("shark.SharkContext")
			
			if(!workIF.getSkipPrepareTable()) {
				val sc = initSharkContext()
				queries.foreach(q => q.prepareSharkBagelTables(sc))
			}
			
		} catch {
			case e: ClassNotFoundException => LOG.error("Shark jar is not found in the classpath")
		}
	}
	
	override def run() = {
		LOG.info("Running Shark+Bagel Query")
		
		try {
			Class.forName("shark.SharkContext")
			val sc = initSharkContext()
			
			sc.runSql("create temporary function sentiment as \'bigframe.workflows.util.SenExtractorHive\'")
			sc.runSql("create temporary function isWithinDate as \'bigframe.workflows.util.WithinDateHive\'")
			sc.runSql("set mapred.reduce.tasks=" + spark_dop)
			
			queries.foreach(q => {
				spark_dop = workIF.getSparkDoP()
				//System.setProperty("spark.default.parallelism", spark_dop.toString())
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
