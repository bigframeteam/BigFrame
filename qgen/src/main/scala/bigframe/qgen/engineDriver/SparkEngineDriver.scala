package bigframe.qgen.engineDriver

import java.util.ArrayList
import java.util.List
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import bigframe.workflows.BusinessIntelligence.text.exploratory._
import bigframe.workflows.BusinessIntelligence.relational.exploratory._
import bigframe.bigif.WorkflowInputFormat
import bigframe.workflows.runnable.SparkRunnable
import java.io.File
import bigframe.workflows.events.BigFrameListenerBus


class SparkEngineDriver(workIF: WorkflowInputFormat) extends EngineDriver(workIF) {
  
//	final var SPARK_CONNECTION = "SPARK_CONNECTION_STRING"
//	final var SPARK_HOME = "SPARK_HOME"

	final var JAR_PATH = "WORKFLOWS_JAR"
	final var LOG: Log =
LogFactory.getLog(classOf[SparkEngineDriver])

	private var spark_connection_string: String = ""
	private var spark_home_string: String = ""
	private var jar_path_string: String = ""
	private var spark_local_dir: String = ""
//	private var spark_use_bagel: Boolean = true
//	private var spark_dop: Integer = 8
	private var memory_fraction: Float = 0.66f
	private var compress_memory: Boolean = false

	private var queries: java.util.List[SparkRunnable] = new java.util.ArrayList[SparkRunnable]()

	def numOfQueries(): Int = {
	  	queries.size()
	}
	
	def init() {
	  	readEnvVars()
	}
	
	def run(eventBus: BigFrameListenerBus) {	
		init()
		LOG.info("Running Spark Query");
		for(query: SparkRunnable <- queries) {
			System.setProperty("spark.local.dir", spark_local_dir)
			System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			// Following statement is ineffective, commenting it
			// System.setProperty("spark.default.parallelism", spark_dop)
			System.setProperty("spark.storage.memoryFraction", memory_fraction.toString())
			System.setProperty("spark.rdd.compress", compress_memory.toString())
			System.setProperty("spark.shuffle.compress", compress_memory.toString())
			System.setProperty("spark.broadcast.compress", compress_memory.toString())
			val sc = new SparkContext(spark_connection_string, "BigFrame", 
						spark_home_string, Seq(jar_path_string))
			if(query.runSpark(sc)) {
				LOG.info("Query Finished");
			}
			else {
				LOG.error("Query failed");
			}
		}
	}

	
	def cleanup() {
	  
	}

	def addQuery(q: SparkRunnable) {
		// TODO Auto-generated method stub
		queries.add(q)
	}

	private def readEnvVars() {
		spark_connection_string = workIF.getSparkMaster()
		jar_path_string = System.getenv(JAR_PATH)
		spark_home_string = workIF.getSparkHome()
		spark_local_dir = workIF.getSparkLocalDir()
//		spark_use_bagel = workIF.getSparkUseBagel()
//		spark_dop = workIF.getSparkDop()
		memory_fraction = workIF.getSparkMemoryFraction()
		compress_memory = workIF.getSparkCompressMemory()
	}	
}
