package bigframe.queries.BusinessIntelligence.relational.exploratory;

import java.sql.SQLException
import java.sql.Statement

import bigframe.queries.BaseTablePath
import bigframe.queries.Query

/**
 * Q1 for BI domain, the SQL are specified in hive.
 * @author andy
 *
 */
abstract class Q1_HiveDialect(basePath : BaseTablePath) extends Query {

	protected var itemHDFSPath = basePath.relational_path + "/item"
	protected var web_salesHDFSPath = basePath.relational_path + "/web_sales"
	protected var promotionHDFSPath = basePath.relational_path + "/promotion"
	protected var customerHDFSPath =  basePath.relational_path + "/customer"
	
	
	@throws(classOf[SQLException])
	def prepareBaseTable(stmt : Statement ): Unit = {
		val dropTable1 = "DROP TABLE web_sales"
		val dropTable2 = "DROP TABLE item";
		val dropTable3 = "DROP TABLE promotion"
		val dropTable4 = "DROP TABLE customer"
		val dropTable5 = "DROP TABLE RptSalesByProdCmpn"
		
		stmt.execute(dropTable1)
		stmt.execute(dropTable2)
		stmt.execute(dropTable3)
		stmt.execute(dropTable4)
		stmt.execute(dropTable5)
		
		val createTable1 = "create external table web_sales" + "\n" +
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
					")" + "\n" +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + web_salesHDFSPath + "\'"
		
		val createTable2 = "create external table item" + "\n" +
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
					")" + "\n" +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + itemHDFSPath + "\'"
		
		val createTable3 = "create external table customer" + "\n" +
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
					    "c_last_review_date        string" +
					")" + "\n" +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + customerHDFSPath + "\'"
		
		val createTable4 = "create external table promotion" + "\n" +
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
					")" + "\n" +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + promotionHDFSPath + "\'"
		
		val createTable5 = "CREATE TABLE RptSalesByProdCmpn (p_promo_id string, " +
							"i_item_sk int, totalsales float)"
		
		stmt.execute(createTable1)
		stmt.execute(createTable2)
		stmt.execute(createTable3)
		stmt.execute(createTable4)
		stmt.execute(createTable5)
	}
	
	@throws(classOf[SQLException])
	def runBenchQuery(stmt: Statement) : Unit = {
		val benchmarkQuery = "INSERT INTO TABLE RptSalesByProdCmpn" + "\n" +
				"SELECT p_promo_id, i_item_sk, totalsales " + "\n" +
				"FROM" + "\n" +
					"( SELECT p_promo_id, p_item_sk, sum(ws_sales_price * ws_quantity) as totalsales" + "\n" +
					"FROM " + "\n" +
					"	( SELECT p_promo_id, p_item_sk, p_start_date_sk, p_end_date_sk" + "\n" +
					"	FROM promotion) p" + "\n" +
					"	JOIN web_sales w" + "\n" +
					"	ON (p.p_item_sk = w.ws_item_sk)" + "\n" +
					"WHERE " + "\n" +
					"	p_start_date_sk <= ws_sold_date_sk " + "\n" +
					"	AND" + "\n" +
					"	ws_sold_date_sk <= p_end_date_sk" + "\n" +
					"GROUP BY" + "\n" +
					"	p_promo_id, p_item_sk ) s" + "\n" +
					"JOIN item i" + "\n" +
					"ON (s.p_item_sk = i.i_item_sk)"
		
		stmt.execute(benchmarkQuery)
	}
}
