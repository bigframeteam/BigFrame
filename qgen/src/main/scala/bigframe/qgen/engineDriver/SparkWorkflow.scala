package bigframe.qgen.engineDriver

import java.util.ArrayList
import java.util.List
import scala.collection.JavaConversions._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._

import org.apache.log4j.Logger
import bigframe.queries.BusinessIntelligence.nested.exploratory._
import bigframe.queries.BusinessIntelligence.relational.exploratory._
import bigframe.bigif.WorkflowInputFormat
import bigframe.queries.SparkRunnable
import java.io.File

class SparkWorkflow(workIF: WorkflowInputFormat) extends Workflow(workIF) {
  
  	final var VARIETY_KEY: String = "variety"
	final var MIXED_VARIETY: String = "mixed"
	final var RELATIONAL_VARIETY: String = "relational"
	final var TEXT_VARIETY: String = "text"
	final var GRAPH_VARIETY: String = "graph"
	final var OUTPUT_KEY: String = "output"
	final var SPARK_CONNECTION = "SPARK_CONNECTION_STRING"
	final var TPCDS_PATH = "TPCDS_PATH"
	final var TEXT_PATH = "TEXT_PATH"
	final var GRAPH_PATH = "GRAPH_PATH"
	final var SPARK_HOME = "SPARK_HOME"
	final var JAR_PATH = "QUERIES_JAR"
	final var LOG: Logger = Logger.getLogger(this.getClass.getName)

	private var variety: String = MIXED_VARIETY
	private var spark_connection_string: String = ""
	private var tpcds_path_string: String = ""
	private var text_path_string: String = ""
	private var graph_path_string: String = ""
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
			query.run(sc)
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
		tpcds_path_string = System.getenv(TPCDS_PATH)
		text_path_string = System.getenv(TEXT_PATH)
		graph_path_string = System.getenv(GRAPH_PATH)
		jar_path_string = System.getenv(JAR_PATH)
		spark_home_string = System.getenv(SPARK_HOME)
		println(spark_connection_string)
		println(spark_home_string)
		println(jar_path_string)
	}	
}