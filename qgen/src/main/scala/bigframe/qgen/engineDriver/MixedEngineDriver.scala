/**
 *
 */
package bigframe.qgen.engineDriver

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList
import java.util.List
import scala.collection.JavaConversions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import SparkContext._
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import bigframe.bigif.BigConfConstants;
import bigframe.bigif.WorkflowInputFormat
import bigframe.workflows.runnable.MixedRunnable
import bigframe.workflows.events.BigFrameListenerBus
import org.apache.spark.scheduler.SparkListener

/**
 * @author mayuresh
 *
 */
class MixedEngineDriver(workIF: WorkflowInputFormat) extends EngineDriver(workIF) {

  	final var JAR_PATH = "WORKFLOWS_JAR"
	final var LOG: Log = LogFactory.getLog(classOf[MixedEngineDriver])

	private var spark_connection_string: String = ""
	private var spark_home_string: String = ""
	private var jar_path_string: String = ""
	private var spark_local_dir: String = ""
	// private var spark_use_bagel: Boolean = true
	// private var spark_dop: Integer = 8
	private var memory_fraction: Float = 0.66f
	private var compress_memory: Boolean = false

	private var connection: Connection = null
	private var stmt: Statement = null
	private val driverName = "org.apache.hadoop.hive.jdbc.HiveDriver"

	private var queries: java.util.List[MixedRunnable] = new java.util.ArrayList[MixedRunnable]()

	private var hc: HiveContext = null
	def numOfQueries(): Int = {
			queries.size()
	}

	def init(sparkListener: SparkListener) {
		hc.sparkContext.addSparkListener(sparkListener)
	}

	def init() {
		try {
			Class.forName(driverName);
        } catch { case e: ClassNotFoundException =>
        	// TODO Auto-generated catch block
        	e.printStackTrace();
        	System.exit(1);
        }

		try {
			LOG.info("Connectiong to Hive JDBC server!!!");
			connection = DriverManager.getConnection(workIF.getHiveJDBCServer(), "", "");
			if(connection == null) {
				LOG.error("Cannot connect to JDBC server! " +
						"Make sure the HiveServer is running!");
				System.exit(1);
			}
			else
				LOG.info("Successful!!!");
			
			val UDF_JAR = workIF.getProp().get(BigConfConstants.BIGFRAME_UDF_JAR);
			
			stmt = connection.createStatement();
			stmt.execute("DELETE JAR " + UDF_JAR);
			LOG.info("Adding UDF JAR " + UDF_JAR + " to hive server");
			if(stmt.execute("ADD JAR " + UDF_JAR)) {
				LOG.info("Adding UDF JAR successful!");
			}
			else {
				LOG.error("Adding UDF JAR failed!");
			}
			init(connection);
		
		} catch { case e: SQLException =>
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}

	}

	def init(conn: Connection) {
		try {
			if(conn == null) {
				System.out.println("Null connection");
				System.exit(1);
			}
			connection = conn;
			if(stmt == null) {
				stmt = connection.createStatement();
			}
		} catch { case e: SQLException =>
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		
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
		for(query: MixedRunnable <- queries) {
			query.prepareHiveTables(hc, connection);
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

	def run(eventBus: BigFrameListenerBus) {	
		// init()
		LOG.info("Running mixed workflow");
		System.out.println("QUERIES:");
		System.out.println(queries);
		for(query: MixedRunnable <- queries) {

			if(query.runMixedFlow(hc, connection, eventBus)) {
				LOG.info("Query Finished");
			}
			else {
				LOG.error("Query failed");
			}
		}
	}

	def cleanup() {
		for(query <- queries) {
			query.cleanUp(hc, connection);
		}

		try {
			if(stmt != null) {
				stmt.close();
			}
			if(connection != null) {
				connection.close();
			}
		} catch { case e: Exception =>
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	def addQuery(q: MixedRunnable) {
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