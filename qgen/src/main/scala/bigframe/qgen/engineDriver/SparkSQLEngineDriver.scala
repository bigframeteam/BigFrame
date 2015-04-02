package bigframe.qgen.engineDriver

import java.util.ArrayList
import java.util.List
import scala.collection.JavaConversions._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import SparkContext._

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import bigframe.workflows.BusinessIntelligence.text.exploratory._
import bigframe.workflows.BusinessIntelligence.relational.exploratory._
import bigframe.bigif.WorkflowInputFormat
import bigframe.workflows.runnable.SparkSQLRunnable
import java.io.File



class SparkSQLEngineDriver(workIF: WorkflowInputFormat) extends EngineDriver(workIF) {
  
// final var SPARK_CONNECTION = "SPARK_CONNECTION_STRING"
// final var SPARK_HOME = "SPARK_HOME"

final var JAR_PATH = "WORKFLOWS_JAR"
final var LOG: Log =
LogFactory.getLog(classOf[SparkSQLEngineDriver])

private var spark_connection_string: String = ""
private var spark_home_string: String = ""
private var jar_path_string: String = ""
private var spark_local_dir: String = ""
// private var spark_use_bagel: Boolean = true
// private var spark_dop: Integer = 8
private var memory_fraction: Float = 0.66f
private var compress_memory: Boolean = false

private var queries: java.util.List[SparkSQLRunnable] = new java.util.ArrayList[SparkSQLRunnable]()

private var hc: HiveContext = null
def numOfQueries(): Int = {
queries.size()
}

def init() {
readEnvVars()

System.setProperty("spark.local.dir", spark_local_dir)
//System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
// Following statement is ineffective, commenting it
// System.setProperty("spark.default.parallelism", spark_dop)
System.setProperty("spark.storage.memoryFraction", memory_fraction.toString())
System.setProperty("spark.rdd.compress", compress_memory.toString())
System.setProperty("spark.shuffle.compress", compress_memory.toString())
//System.setProperty("spark.broadcast.compress", compress_memory.toString())
System.setProperty("spark.eventLog.enabled", "true")
System.setProperty("spark.eventLog.dir", spark_local_dir + "/event_log")

val sc = createSparkContext()//new SparkContext(spark_connection_string, "BigFrame",
//spark_home_string, Seq(jar_path_string))

hc = new HiveContext(sc)

LOG.info("Preparing tables...");
for(query: SparkSQLRunnable <- queries) {
      query.prepareHiveTables(hc);
}

}

def run() {	
// init()
 LOG.info("Running SparkSQL Query");
System.out.println("QUERIES:");
System.out.println(queries);
 for(query: SparkSQLRunnable <- queries) {
 
  if(query.runSparkSQL(hc)) {
    LOG.info("Query Finished");
  }
  else {
    LOG.error("Query failed");
  }
 }
}

  def createSparkContext(): SparkContext = {
    val execUri = System.getenv("SPARK_EXECUTOR_URI")
    val jars = Seq(jar_path_string)
    val conf = new SparkConf()
      .setMaster(spark_connection_string)
      .setSparkHome(spark_home_string)
      .setAppName("BigFrame")
      .setJars(jars)
    if (execUri != null) {
      conf.set("spark.executor.uri", execUri)
    }
    val sparkContext = new SparkContext(conf)
    LOG.info("Created spark context..")
    sparkContext
  }

def cleanup() {

}

def addQuery(q: SparkSQLRunnable) {
  queries.add(q)
}

private def readEnvVars() {
  spark_connection_string = workIF.getSparkMaster()
  jar_path_string = System.getenv(JAR_PATH)
  spark_home_string = workIF.getSparkHome()
  spark_local_dir = workIF.getSparkLocalDir()
  // spark_use_bagel = workIF.getSparkUseBagel()
  // spark_dop = workIF.getSparkDop()
  memory_fraction = workIF.getSparkMemoryFraction()
  compress_memory = workIF.getSparkCompressMemory()
}	
}
