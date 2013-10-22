package bigframe.workflows.BusinessIntelligence.relational.exploratory


import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement
import java.io.IOException
import org.apache.hadoop.conf.Configuration
import bigframe.workflows.BaseTablePath
import bigframe.workflows.Query
import bigframe.workflows.runnable.HiveRunnable
import bigframe.workflows.runnable.SharkRunnable
import bigframe.workflows.runnable.HadoopRunnable


import scala.collection.JavaConversions._

/**
 * Q1 for BI domain, the SQL are specified in hive.
 * @author andy
 *
 */
class WF_ReportSales(basePath : BaseTablePath) extends Query with HiveRunnable with SharkRunnable with HadoopRunnable{

	private val itemHDFSPath = basePath.relational_path + "/item"
	private val web_salesHDFSPath = basePath.relational_path + "/web_sales"
	private val catalog_salesHDFSPath = basePath.relational_path + "/catalog_sales"
	private val store_salesHDFSPath = basePath.relational_path + "/store_sales"
	private val promotionHDFSPath = basePath.relational_path + "/promotion"
	private val customerHDFSPath =  basePath.relational_path + "/customer"
	
	
	override def printDescription(): Unit = {
		// TODO Auto-generated method stub
		
	}
	
	@throws(classOf[SQLException])
	def prepareBaseTable(stmt : Statement ): Unit = {
		val dropWebSales = "DROP TABLE web_sales"
		val dropStoreSales = "DROP TABLE store_sales"  
		val dropCatalogSales = "DROP TABLE catalog_sales"
		val dropItem = "DROP TABLE item"
		val dropPromot = "DROP TABLE promotion"
		val dropCust = "DROP TABLE customer"
		val dropRptSales = "DROP TABLE RptSalesByProdCmpn"
		
		stmt.execute(dropWebSales)
		stmt.execute(dropCatalogSales)
		stmt.execute(dropStoreSales)
		stmt.execute(dropItem)
		stmt.execute(dropPromot)
		stmt.execute(dropCust)
		stmt.execute(dropRptSales)
		
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
		
		val createCatalogSales = "create external table catalog_sales" +
					"(" +
					    "cs_sold_date_sk           int                       ," +
					    "cs_sold_time_sk           int                       ," +
					    "cs_ship_date_sk           int                       ," +
					    "cs_bill_customer_sk       int                       ," +
					    "cs_bill_cdemo_sk          int                       ," +
					    "cs_bill_hdemo_sk          int                       ," +
					    "cs_bill_addr_sk           int                       ," +
					    "cs_ship_customer_sk       int                       ," +
					    "cs_ship_cdemo_sk          int                       ," +
					    "cs_ship_hdemo_sk          int                       ," +
					    "cs_ship_addr_sk           int                       ," +
					    "cs_call_center_sk         int                       ," +
					    "cs_catalog_page_sk        int                       ," +
					    "cs_ship_mode_sk           int                       ," +
					    "cs_warehouse_sk           int                       ," +
					    "cs_item_sk                int               		," +
					    "cs_promo_sk               int                       ," +
					    "cs_order_number           int               		," +
					    "cs_quantity               int                       ," +
					    "cs_wholesale_cost         float                  ," +
					    "cs_list_price             float                  ," +
					    "cs_sales_price            float                  ," +
					    "cs_ext_discount_amt       float                  ," +
					    "cs_ext_sales_price        float                  ," +
					    "cs_ext_wholesale_cost     float                  ," +
					    "cs_ext_list_price         float                  ," +
					    "cs_ext_tax                float                  ," +
					    "cs_coupon_amt             float                  ," +
					    "cs_ext_ship_cost          float                  ," +
					    "cs_net_paid               float                  ," +
					    "cs_net_paid_inc_tax       float                  ," +
					    "cs_net_paid_inc_ship      float                  ," +
					    "cs_net_paid_inc_ship_tax  float                  ," +
					    "cs_net_profit             float                  " +
					")" +
					"row format delimited fields terminated by \'|\' " + "\n" + 
					"location " + "\'" + catalog_salesHDFSPath + "\'"
		
					
		val createStoreSales = "create external table store_sales" +
					"(" +
					    "ss_sold_date_sk           int                       ," +
					    "ss_sold_time_sk           int                       ," +
					    "ss_item_sk                int               		," +
					    "ss_customer_sk            int                       ," +
					    "ss_cdemo_sk               int                       ," +
					    "ss_hdemo_sk               int                       ," +
					    "ss_addr_sk                int                       ," +
					    "ss_store_sk               int                       ," +
					    "ss_promo_sk               int                       ," +
					    "ss_ticket_number          int               		," +
					    "ss_quantity               int                       ," +
					    "ss_wholesale_cost         float                  ," +
					    "ss_list_price             float                  ," +
					    "ss_sales_price            float                  ," +
					    "ss_ext_discount_amt       float                  ," +
					    "ss_ext_sales_price        float                  ," +
					    "ss_ext_wholesale_cost     float                  ," +
					    "ss_ext_list_price         float                  ," +
					    "ss_ext_tax                float                  ," +
					    "ss_coupon_amt             float                  ," +
					    "ss_net_paid               float                  ," +
					    "ss_net_paid_inc_tax       float                  ," +
					    "ss_net_profit             float                  " +
					")" +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location" + "\'" + store_salesHDFSPath + "\'"

					
		val createItem = "create external table item" + 
					"(" +
					    "i_item_sk                 int," + 
					    "i_item_id                 string," + 
					    "i_rec_start_date          timestamp," + 
					    "i_rec_end_date            timestamp," + 
					    "i_item_desc               string," + 
					    "i_current_price           float," + 
					    "i_wholesale_cost          float," +
					    "i_brand_id                int," +
					    "i_brand                   string," +
					    "i_claws_id                int," +
					    "i_class                   string," +
					    "i_category_id             int," +
					    "i_category                string," +
					    "i_manufact_id             int," +
					    "i_manufact                string," +
					    "i_size                    string," +
					    "i_formulation             string," +
					    "i_color                   string," +
					    "i_units                   string," +
					    "i_container               string," +
					    "i_manager_id              int," +
					    "i_product_name            string" +
					")"  +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + itemHDFSPath + "\'"
		
		val createCust = "create external table customer" + 
				"(" +
					    "c_customer_sk             int," +
					    "c_customer_id             string," +
					    "c_current_cdemo_sk        int," +
					    "c_current_hdemo_sk        int," +
					    "c_current_addr_sk         int," +
					    "c_first_shipto_date_sk    int," +
					    "c_first_sales_date_sk     int," +
					    "c_salutation              string," +
					    "c_first_name              string," +
					    "c_last_name               string," +
					    "c_preferred_cust_flag     string," +
					    "c_birth_day               int," +
					    "c_birth_month             int," +
					    "c_birth_year              int," +
					    "c_birth_country           string," +
					    "c_login                   string," +
					    "c_email_address           string," +
					    "c_last_review_date_sk        string" +
					")" + 
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + customerHDFSPath + "\'"
		
		val createPromot = "create external table promotion" + 
					"(" +
					    "p_promo_sk                int," +
					    "p_promo_id                string," +
					    "p_start_date_sk           int," +
					    "p_end_date_sk             int," +
					    "p_item_sk                 int," +
					    "p_cost                    float," +
					    "p_response_target         int," +
					    "p_promo_name              string," +
					    "p_channel_dmail           string," +
					    "p_channel_email           string," +
					    "p_channel_catalog         string," +
					    "p_channel_tv              string," +
					    "p_channel_radio           string," +
					    "p_channel_press           string," +
					    "p_channel_event           string," +
					    "p_channel_demo            string," +
					    "p_channel_details         string," +
					    "p_purpose                 string," +
					    "p_discount_active         string " +
					")" + 
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + promotionHDFSPath + "\'"
		
		val createRptSales = "CREATE TABLE RptSalesByProdCmpn (p_promo_id string, " +
							"i_item_sk int, i_product_name string, totalsales float)"
		
		stmt.execute(createWebSales)
		stmt.execute(createCatalogSales)
		stmt.execute(createStoreSales)
		stmt.execute(createItem)
		stmt.execute(createCust)
		stmt.execute(createPromot)
		stmt.execute(createRptSales)
	}
	
	@throws(classOf[SQLException])
	def runBenchQuery(stmt: Statement) : Boolean = {
	  
		//TO DO: Modify the query based on the new workflow.
		//	1. Select a set of promotions.
		val benchmarkQuery = "INSERT INTO TABLE RptSalesByProdCmpn" + "\n" +
								"SELECT p_promo_id, i_item_sk, i_product_name, totalsales" + "\n" +
								"FROM (" + "\n" +
								"	SELECT p_promo_id, p_item_sk, sum(price*quantity) as totalsales" + "\n" +
								"	FROM" + "\n" +
									"	(SELECT ws_sold_date_sk as sold_date_sk, ws_item_sk as item_sk, ws_sales_price as price, ws_quantity as quantity" + "\n" +
									"	FROM web_sales" + "\n" +
									"	UNION ALL" + "\n" +
									"	SELECT ss_sold_date_sk as sold_date_sk, ss_item_sk as item_sk, ss_sales_price as price, ss_quantity as quantity" + "\n" +
									"	FROM store_sales" + "\n" +
									"	UNION ALL" + "\n" +
									"	SELECT cs_sold_date_sk as sold_date_sk, cs_item_sk as item_sk, cs_sales_price as price, cs_quantity as quantity" + "\n" +
									"	FROM catalog_sales) sales" + "\n" +
									"	JOIN promotion p " + "\n" +
									"	ON (sales.item_sk = p.p_item_sk)" + "\n" +
									"WHERE " + "\n" +
									"	p_start_date_sk <= sold_date_sk " + "\n" +
									"	AND" + "\n" +
									"	sold_date_sk <= p_end_date_sk" + "\n" +
									"GROUP BY" + "\n" +
									"	p_promo_id, p_item_sk " + "\n" +
									") s" + "\n" +
									"JOIN item i" + "\n" +
									"ON (s.p_item_sk = i.i_item_sk)"
		
		return stmt.execute(benchmarkQuery)
	}
	
	/**
	 * prepare base table for hive query
	 */
	override def prepareTables(connection: Connection): Unit = {
		try {
			val stmt = connection.createStatement()
			
			prepareBaseTable(stmt)
			
		} catch {
		  
		  case sqle :SQLException => sqle.printStackTrace()

		}
		
	}
	
	/**
	 * Submit the query to hiveserver.
	 */
	override def runHive(connection: Connection): Boolean = {
		try {
			val stmt = connection.createStatement();
			
			return runBenchQuery(stmt);
			
		} catch {
			case sqle :
				SQLException => sqle.printStackTrace()
				return false

		}
	}
	
	/**
	 * Submit the query to hiveserver by shark.
	 *
	 */
	override def runShark(connection: Connection): Boolean = {
		
		/**
		 *  Shark is compatible with Hive.
		 */
		runHive(connection);
	}
	
	
	/**
	 * Run the query in Hadoop.
	 */
	override def runHadoop(mapred_config: Configuration): java.lang.Boolean = {
		try {
			val promotedSKs = Set(new java.lang.Integer(1), new java.lang.Integer(2), new java.lang.Integer(3))
			val reportsales = new ReportSalesHadoop(basePath.relational_path, promotedSKs, mapred_config)
			return reportsales.runHadoop(mapred_config)
			
		} catch {
			case ioe: 
				IOException => ioe.printStackTrace() 
				return false
			case inte: 
				InterruptedException => inte.printStackTrace()
				return false
			case clznotFe: 
				ClassNotFoundException => clznotFe.printStackTrace()
				return false
		}
	  
	}
}

