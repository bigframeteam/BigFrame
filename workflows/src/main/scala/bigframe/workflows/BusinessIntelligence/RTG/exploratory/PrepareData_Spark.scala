package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import com.google.common.io.Files

import java.io.File
import java.lang.Iterable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import SparkContext._

import org.apache.hadoop.mapreduce.Job
import bigframe.workflows.BaseTablePath

import bigframe.util.parser.JsonParser

import org.json.simple.JSONArray
import org.json.simple.JSONObject

object PrepareData_Spark extends Serializable {
	val promo_dir = "promo_avro"
//	val tempDir = Files.createTempDir()
//	val promo_dir = new File(tempDir, "output").getAbsolutePath
//	println(promo_dir)
	val tweet_dir = "tweet_avro"
}

class PrepareData_Spark(basePath: BaseTablePath) extends Serializable {

	val itemHDFSPath = basePath.relational_path + "/item"
	val web_salesHDFSPath = basePath.relational_path + "/web_sales"
	val catalog_salesHDFSPath = basePath.relational_path + "/catalog_sales"
	val store_salesHDFSPath = basePath.relational_path + "/store_sales"
	val promotionHDFSPath = basePath.relational_path + "/promotion"
	val customerHDFSPath =  basePath.relational_path + "/customer"
	val date_dimHDFSPath = basePath.relational_path + "/date_dim"
	
	val twitter_grapgHDFSPath = basePath.graph_path
	val tweets_HDFSPath = basePath.nested_path	
	
		
	def prepareData(sc: SparkContext) = {
		
	}
		
}