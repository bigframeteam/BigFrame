package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import java.sql.Connection
import java.sql.SQLException

import org.apache.spark.SparkContext
import org.apache.spark.bagel._
import org.apache.spark.bagel.Bagel._

import bigframe.workflows.runnable.SharkBagelRunnable
import bigframe.workflows.Query
import bigframe.workflows.BaseTablePath
import bigframe.workflows.util.TRVertex
import bigframe.workflows.util.TRMessage
import bigframe.workflows.util.HDFSUtil

import shark.{SharkContext, SharkEnv}

import bigframe.workflows.BusinessIntelligence.graph.exploratory.WF_TwitterRankBagel

class WF_ReportSaleSentimentSharkBagel(basePath: BaseTablePath, hadoop_home: String, hiveWarehouse: String, 
		numItr: Int, val useRC: Boolean, val numPartition: Int = 10)
		extends Query with SharkBagelRunnable {

	val twitterRank_path = basePath.relational_path  + "/../twitterrank\'"
	val hdfsUtil = new HDFSUtil(hadoop_home)
	
	def printDescription(): Unit = {}
	
	/*
	 * Prepeare the basic tables before run the Shark query
	 */
	override def prepareSharkBagelTables(sc: SharkContext): Unit = {
		val itemHDFSPath = basePath.relational_path + "/item"
		val web_salesHDFSPath = basePath.relational_path + "/web_sales"
		val catalog_salesHDFSPath = basePath.relational_path + "/catalog_sales"
		val store_salesHDFSPath = basePath.relational_path + "/store_sales"
		val promotionHDFSPath = basePath.relational_path + "/promotion"
		val customerHDFSPath =  basePath.relational_path + "/customer"
		val date_dimHDFSPath = basePath.relational_path + "/date_dim"
		
		val twitter_grapgHDFSPath = basePath.graph_path
		val tweets_HDFSPath = basePath.nested_path
		
		println(sc.runSql("show tables"))
		
		val dropWebSales = "DROP TABLE web_sales"
			
		val createWebSales = "create external table web_sales" + 
					"(" + 
					    "ws_sold_date_sk           int," + 
					    "ws_sold_time_sk           int," + 
					    "ws_ship_date_sk           int," + 
					    "ws_item_sk                int," + 
					    "ws_bill_customer_sk       int," + 
					    "ws_bill_cdemo_sk          int," + 
					    "ws_bill_hdemo_sk          int," + 
					    "ws_bill_addr_sk           int," + 
					    "ws_ship_customer_sk       int," + 
					    "ws_ship_cdemo_sk          int," + 
					    "ws_ship_hdemo_sk          int," + 
					    "ws_ship_addr_sk           int," + 
					    "ws_web_page_sk            int," + 
					    "ws_web_site_sk            int," + 
					    "ws_ship_mode_sk           int," + 
					    "ws_warehouse_sk           int," + 
					    "ws_promo_sk               int," + 
					    "ws_order_number           int," + 
					    "ws_quantity               int," + 
					    "ws_wholesale_cost         float," + 
					    "ws_list_price             float," + 
					    "ws_sales_price            float," + 
					    "ws_ext_discount_amt       float," + 
					    "ws_ext_sales_price        float," + 
					    "ws_ext_wholesale_cost     float," + 
					    "ws_ext_list_price         float," + 
					    "ws_ext_tax                float," + 
					    "ws_coupon_amt             float," + 
					    "ws_ext_ship_cost          float," + 
					    "ws_net_paid               float," + 
					    "ws_net_paid_inc_tax       float," + 
					    "ws_net_paid_inc_ship      float," + 
					    "ws_net_paid_inc_ship_tax  float," + 
					    "ws_net_profit             float" +                 
					")" +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + web_salesHDFSPath + "\'"
					
//		sc.runSql(dropWebSales)
//		sc.runSql(createWebSales)
		
		println(sc.runSql("show tables"))
	}
	
	def cleanUpSharkBagelImpl1(sc: SharkContext): Unit = {
		
	}
	
	override def cleanUpSharkBagel(sc: SharkContext): Unit = {
		cleanUpSharkBagelImpl1(sc)
	}
	
	
	def runSharkBagelImpl1(sc: SharkContext) : Boolean = {
			
		return true
	}
	
	
	/**
	 * Run the benchmark query
	 */
	override def runSharkBagel(sc: SharkContext): Boolean = {
		
			return runSharkBagelImpl1(sc)

	}

}