package bigframe.qgen.engineDriver

import java.util.ArrayList
import java.util.List
import scala.collection.JavaConversions._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._

import org.apache.log4j.Logger
import bigframe.workflows.BusinessIntelligence.text.exploratory._
import bigframe.workflows.BusinessIntelligence.relational.exploratory._
import bigframe.bigif.WorkflowInputFormat
import bigframe.workflows.runnable.SparkRunnable
import java.io.File


class SparkEngineDriver(workIF: WorkflowInputFormat) extends EngineDriver(workIF) {
  
	final var SPARK_CONNECTION = "SPARK_CONNECTION_STRING"
	final var SPARK_HOME = "SPARK_HOME"

	final var JAR_PATH = "WORKFLOWS_JAR"
	final var LOG: Logger = Logger.getLogger(this.getClass.getName)

	private var spark_connection_string: String = ""
	private var spark_home_string: String = ""
	private var jar_path_string: String = ""

	private var queries: java.util.List[SparkRunnable] = new java.util.ArrayList[SparkRunnable]()

	def numOfQueries(): Int = {
	  	queries.size()
	}
	
	def init() {
	  	readEnvVars()
	}
	
	def run() {	
		init()
	  	println("Running Spark Query");
		for(query: SparkRunnable <- queries) {
			println("Setting the context")
			val sc = new SparkContext(spark_connection_string, "BigFrame", 
						spark_home_string, Seq(jar_path_string))
			println("Set the context: " + sc)
			if(query.run(sc)) {
				println("Query Finished");
			}
			else {
				println("Query failed");
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
		spark_connection_string = System.getenv(SPARK_CONNECTION)
		jar_path_string = System.getenv(JAR_PATH)
		spark_home_string = System.getenv(SPARK_HOME)
	}	
}