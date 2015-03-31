package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import java.sql.Connection
import java.sql.SQLException

import bigframe.workflows.BaseTablePath

class PrepareTable_Impala(basePath: BaseTablePath)  {
	
	val itemHDFSPath = basePath.relational_path + "/item"
	val web_salesHDFSPath = basePath.relational_path + "/web_sales"
	val catalog_salesHDFSPath = basePath.relational_path + "/catalog_sales"
	val store_salesHDFSPath = basePath.relational_path + "/store_sales"
	val promotionHDFSPath = basePath.relational_path + "/promotion"
	val customerHDFSPath =  basePath.relational_path + "/customer"
	val date_dimHDFSPath = basePath.relational_path + "/date_dim"
	
	val twitter_grapgHDFSPath = basePath.graph_path
	val tweets_HDFSPath = basePath.nested_path
	
	/**
	 * Normal Text File
	 */
	def prepareTableTextFileFormat(connection: Connection): Unit = {
		println("Creating Text files...")
		
		val impala_stmt = connection.createStatement()
		
		val dropWebSalesTmp = "DROP TABLE IF EXISTS web_sales_tmp"
		
		impala_stmt.execute(dropWebSalesTmp)
		
		val createWebSalesTmp = "create external table web_sales_tmp" + 
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

		impala_stmt.execute(createWebSalesTmp)
					
		val dropWebSales = "DROP TABLE IF EXISTS web_sales"
		impala_stmt.execute(dropWebSales)
		
		val createWebSales = "CREATE TABLE web_sales " +
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
					")"

		impala_stmt.execute(createWebSales)

		val insertWebSales = "INSERT INTO TABLE web_sales SELECT * FROM web_sales_tmp"
		
		impala_stmt.executeUpdate(insertWebSales)
			
		val res = impala_stmt.executeQuery("SELECT count(*) FROM web_sales")
		
		while (res.next()) {
			 System.out.println(res.getInt(1))
		}

	}
	
	/**
	 * Parquet File format
	 */
	def prepareTableParquetFormat(connection: Connection): Unit = {
		
	}
	
}