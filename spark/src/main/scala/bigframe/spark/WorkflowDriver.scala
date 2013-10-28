package bigframe.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._

import bigframe.spark.text._
import bigframe.spark.relational.MicroQueries
import bigframe.spark.graph.TwitterRankDriver

import java.io.File

/*
Class to run entire workflow
 */
object WorkflowDriver {

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
	final var JAR_PATH = "BIGFRAME_JAR"

	private var variety: String = MIXED_VARIETY
	private var spark_connection_string: String = ""
	private var tpcds_path_string: String = ""
	private var text_path_string: String = ""
	private var graph_path_string: String = ""
	private var spark_home_string: String = ""
	private var jar_path_string: String = ""
	  
	private var output_path: String = "/tmp/"

	private def readArg(arg: String) {
	  try {
		val tokens = arg.split("=")
		val key = tokens(0)
		val value = tokens(1)
		if (VARIETY_KEY.equals(key)) {
		  try {
		    variety = if(List(MIXED_VARIETY, RELATIONAL_VARIETY, TEXT_VARIETY, 
		        GRAPH_VARIETY).contains(value)) value else MIXED_VARIETY
		  } catch {
		  	case e: Exception => println("ERROR: variety invalid, using \"mixed\" instead")
		  }
		}
		
		if(OUTPUT_KEY.equals(key)) {
		  try {
		    val file = new File(value)
//			if (file.getParentFile().exists() & file.getParentFile().isDirectory()) {
			  output_path = value
//		  	}
		  } catch {
		    case e:Exception => println("ERROR: output path invalid, using \\tmp instead")
		  }
		}
	  } catch {
	    case e: Exception => 
	  }
	}
	
	private def printUsage() {
		println("Arguments:\n" +
				"1. variety=<variety_value> where variety_value is one of the " +
				"\"mixed\", \"relational\", \"text\", or \"graph\"\n" +
				"2. output=<output_path>")	  
	}

	private def readEnvVars() {
		spark_connection_string = System.getenv(SPARK_CONNECTION)
		tpcds_path_string = System.getenv(TPCDS_PATH)
		text_path_string = System.getenv(TEXT_PATH)
		graph_path_string = System.getenv(GRAPH_PATH)
		jar_path_string = System.getenv(JAR_PATH)
		spark_home_string = System.getenv(SPARK_HOME)
	}
	
	private def exitMessage(arg: String) {
	    println(arg + " is not set. Exiting!")	  
	}
	
	private def validateParams() {
	  if ("".equals(spark_home_string)) {
	    exitMessage(SPARK_HOME)
	    System.exit(0)
	  }
	  if ("".equals(jar_path_string)) {
	    exitMessage(JAR_PATH)
	    System.exit(0)
	  }
	  if("".equals(spark_connection_string)) {
	    exitMessage(SPARK_CONNECTION)
	    System.exit(0)
	  }
	  if("".equals(tpcds_path_string) && 
	      (variety.matches(RELATIONAL_VARIETY) || variety.matches(MIXED_VARIETY))) {
	    exitMessage(TPCDS_PATH)
	    System.exit(0)
	  }
	  if("".equals(text_path_string) && !variety.matches(RELATIONAL_VARIETY)) {
	    exitMessage(TEXT_PATH)
	    System.exit(0)
	  }
	  if("".equals(graph_path_string) && 
	      (variety.matches(GRAPH_VARIETY) || variety.matches(MIXED_VARIETY))) {
	    exitMessage(GRAPH_PATH)
	    System.exit(0)
	  }
	  
	}

	def runWorkflow (sc: SparkContext):RDD[_] = {
		if (variety.equals(MIXED_VARIETY)) {
			println("Going to run mixed workflow")
			val promotion = new WF_Macro_Spark(sc, tpcds_path_string, text_path_string, 
					graph_path_string)
			return promotion.runWorkflow()
		}
		if(variety.equals(RELATIONAL_VARIETY)) {
			println("Going to run relational workflow")
			val executor = new MicroQueries(sc, tpcds_path_string)
			return executor.microBench()
		}
		if (variety.equals(TEXT_VARIETY)) {
			println("Going to run text workflow")
			val executor = new TextExecutor(sc, text_path_string)
			return executor.microBench("[aA].*")
		}
		if (variety.equals(GRAPH_VARIETY)) {
			println("Going to run graph workflow")
			val executor = new TwitterRankDriver(sc, graph_path_string, 
			    text_path_string, tpcds_path_string)
			return executor.microBench("a.*")
		}
		sc makeRDD Array("redundant")
	}
	
	/*
    Arguments:
     1. Spark connection string -- "spark://<ip/name>:7070"
     2. TPCDS path -- "hdfs://<ip/name>:9000/<path_to_TPCDS>"
     3. Tweet file(s) path -- "hdfs://<ip/name>:9000/<path_to_file>"
	 */
	def main(args: Array[String]) {
		if(args.length < 1)
		{
		  printUsage()
		  System.exit(0)
		}
		
		readEnvVars()
		args foreach (arg => readArg(arg))
		validateParams()
		
		println("Setting the context")
		val sc = new SparkContext(spark_connection_string, "BigFrame",
				spark_home_string, Seq(jar_path_string))
		println("Set the context: " + sc)

		val output = runWorkflow(sc)

		println("Workflow executed, writing the output to: " + output_path)
		output.saveAsTextFile(output_path)

		// unstructured part
//		val reader = new TweetReader(sc, text_path_string)
//		reader.init(sc)
//		val tweets = reader.read(text_path_string)
		// run sentiment analysis
//		val tweets = Array( new Tweet("1 I abandoned study", "today"), new Tweet("2 Feeling proud", "today"))
//		val tweetsWithSentiment = reader.addSentimentScore(tweets)

		//println("tweets: ")
//		tweetsWithSentiment foreach {a => println(a)}

		System.exit(0)
	}

}
