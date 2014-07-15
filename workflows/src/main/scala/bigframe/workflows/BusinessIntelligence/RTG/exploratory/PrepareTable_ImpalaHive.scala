package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import bigframe.workflows.BaseTablePath

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

import java.sql.Connection

object PrepareTable_ImpalaHive {
	val LOG  = LogFactory.getLog(classOf[PrepareTable_ImpalaHive]);
}

class PrepareTable_ImpalaHive(basePath: BaseTablePath) {

		
	val itemHDFSPath = basePath.relational_path + "/item"
	val web_salesHDFSPath = basePath.relational_path + "/web_sales"
	val catalog_salesHDFSPath = basePath.relational_path + "/catalog_sales"
	val store_salesHDFSPath = basePath.relational_path + "/store_sales"
	val promotionHDFSPath = basePath.relational_path + "/promotion"
	val customerHDFSPath =  basePath.relational_path + "/customer"
	val date_dimHDFSPath = basePath.relational_path + "/date_dim"
	
	val twitter_grapgHDFSPath = basePath.graph_path
	val tweets_HDFSPath = basePath.nested_path
	
	def web_sales_schema = 	"(" + 
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
	
	def catalog_sales_schema =  "(" +
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
					")"
					    
	def store_sales_schema = "(" +
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
					")" 

	def promotion_schema = 	"(" +
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
					")" 
		
	def item_schema = "(" +
					    "i_item_sk                 int," + 
					    "i_item_id                 string," + 
					    "i_rec_start_date          string," + 
					    "i_rec_end_date            string," + 
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
					")" 
		
	def customer_schema = "(" +
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
					")"
		
	def date_dim_schema = "	(" +
				"		d_date_sk 			int," +
				"		d_date_id 			string," +
				"		d_date 				string," +
				"		d_month_seq 		int," +
				"		d_week_seq 			int," +
		    	"		d_quarter_seq 		int," +
		    	"		d_year 				int," +
		    	"		d_dow 				int," +
		    	"		d_moy 				int," +
		    	"		d_dom 				int," +
		    	"		d_qoy 				int," +
		    	"		d_fy_year 			int," + 
		    	"		d_fy_quarter_seq 	int," +
		    	"		d_fy_week_seq 		int," +
		    	"		d_day_name 			string," + 
		    	"		d_quarter_name 		string," +
		    	"		d_holiday 			string," + 
		    	"		d_weekend 			string," + 
		    	"		d_following_holiday string," +
		    	"		d_first_dom 		int," + 
		    	"		d_last_dom 			int," + 
		    	"		d_same_day_ly 		int," + 
		    	"		d_same_day_lq 		int," + 
		    	"		d_current_day 		string," + 
		    	"		d_current_week 		string," +
		    	"		d_current_month 	string," + 
		    	"		d_current_quarter 	string," +
		    	"		d_current_year 		string"  +
		    	"	)"
		
	def twitter_graph_schema = "	(" +
				"		friend_id	int," +
				"		follower_id	int" +
				"	)"
		
	def tweets_schema = "	(" +
				"		contributors string," +
				"		coordinates string," +
				"		created_at string," +
				"		entities struct<hashtags:array<string>, " +
				"						urls:array<struct<expanded_url:string, " +
				"											indices:array<int>, " +
				"											url:string>>, " +
				"						user_mentions:array<struct<id:string," +
				"													id_str:string, " +
				"													indices:array<int>, " +
				"													name:string, " +
				"													screen_name:string>>>," +
				"		favorited string," +
				"		geo string," +
				"		id bigint," +
				"		id_str string," +
				"		in_reply_to_screen_name string," +
				"		in_reply_to_status_id string," +
				"		in_reply_to_status_id_str string," +
				"		in_reply_to_user_id string," +
				"		in_reply_to_user_id_str string," +
				"		place string," +
				"		retweet_count string," +
				"		retweeted string," +
				"		source string," +
				"		text string," +
				"		truncated string," +
				"		user struct<contributors_enabled:string, " +
				"					created_at:string, " +
				"					description:string, " +
				"					favourites_count:string, " +
				"					follow_request_sent:string, " +
				"					followers_count:string, " +
				"					`following`:string, " +
				"					friends_count:string, " +
				"					geo_enabled:string, " +
				"					id:int, " +
				"					id_str:string, " +
				"					lang:string, " +
				"					listed_count:string, " +
				"					`location`:string, " +
				"					name:string, " +
				"					notifications:string, " +
				"					profile_background_color:string, " +
				"					profile_background_image_url:string, " +
				"					profile_background_tile:string," +
				"					profile_image_url:string," +
				" 					profile_link_color:string, " +
				"					profile_sidebar_border_color:string, " +
				"					profile_sidebar_fill_color:string, " +
				"					profile_text_color:string, " +
				"					profile_use_background_image:string, " +
				"					protected:string, screen_name:string, " +
				"					show_all_inline_media:string, " +
				"					statuses_count:string, " +
				"					time_zone:string, " +
				"					url:string, " +
				"					utc_offset:string, " +
				"					verified:string>" +
				"	)"
					   
	/**
	 * Normal Text File
	 */
	def prepareTableTextFileFormat(impala_connect: Connection, hive_connect: Connection): Unit = {
//			val preparor = new PrepareTable_Hive(basePath)
//			preparor.prepareTableTextFile(hive_connect)
		PrepareTable_ImpalaHive.LOG.info("Creating Text files...")
		
		val hive_stmt = hive_connect.createStatement()
		val impala_stmt = impala_connect.createStatement()
		
		
		val dropWebSales = "DROP TABLE IF EXISTS web_sales"
		val dropStoreSales = "DROP TABLE IF EXISTS store_sales"  
		val dropCatalogSales = "DROP TABLE IF EXISTS catalog_sales"
		val dropItem = "DROP TABLE IF EXISTS item"
		val dropPromot = "DROP TABLE IF EXISTS promotion"
		val dropCust = "DROP TABLE IF EXISTS customer"
		val dropDateDim = "DROP TABLE IF EXISTS date_dim"
		val dropGraph = "DROP TABLE IF EXISTS twitter_graph"
		val dropTweets = "DROP TABLE IF EXISTS tweets"

		
		impala_stmt.execute(dropWebSales)
		impala_stmt.execute(dropCatalogSales)
		impala_stmt.execute(dropStoreSales)
		impala_stmt.execute(dropItem)
		impala_stmt.execute(dropPromot)
		impala_stmt.execute(dropCust)
		hive_stmt.execute(dropDateDim)
		impala_stmt.execute(dropGraph)
		hive_stmt.execute(dropTweets)
		
		val createWebSales = "create external table web_sales" + web_sales_schema +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + web_salesHDFSPath + "\'"
		
		val createCatalogSales = "create external table catalog_sales" + catalog_sales_schema +
					"row format delimited fields terminated by \'|\' " + "\n" + 
					"location " + "\'" + catalog_salesHDFSPath + "\'"
		
					
		val createStoreSales = "create external table store_sales" + store_sales_schema +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location" + "\'" + store_salesHDFSPath + "\'"

					
		val createItem = "create external table item" + item_schema +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + itemHDFSPath + "\'"
		
		val createCust = "create external table customer" + customer_schema + 
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + customerHDFSPath + "\'"
		
		val createPromot = "create external table promotion" + promotion_schema + 
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + promotionHDFSPath + "\'"
					
		val createDateDim = "create external table date_dim" + date_dim_schema +
				"	row format delimited fields terminated by \'|\' " +
				"	location " + "\'" + date_dimHDFSPath + "\'"
					
		val createGraph = "create external table twitter_graph" + twitter_graph_schema +
				"	row format delimited fields terminated by \'|\'" +
				"	location \'" + twitter_grapgHDFSPath +"\'"
				
		/**
		 * TO DO, create json format table
		 */
		val createTweets = "create external table tweets" + tweets_schema +
				"	ROW FORMAT SERDE \'com.cloudera.hive.serde.JSONSerDe\'" +
				"	location \'" + tweets_HDFSPath +"\'"
					
		hive_stmt.execute(createTweets)
		impala_stmt.execute(createWebSales)
		impala_stmt.execute(createCatalogSales)
		impala_stmt.execute(createStoreSales)
		impala_stmt.execute(createItem)
		impala_stmt.execute(createCust)
		impala_stmt.execute(createPromot)
		hive_stmt.execute(createDateDim)
		impala_stmt.execute(createGraph)

	}
	
	
	/**
	 * ORC File format
	 */
	
	def prepareTableORCFileFormat(impala_connect: Connection, hive_connect: Connection, 
			isBaseTableCompress: Boolean) = {
				
		PrepareTable_ImpalaHive.LOG.info("Creating ORC file...")
		
		val hive_stmt = hive_connect.createStatement();
		
		
		val dropWebSalesTmp = "DROP TABLE IF EXISTS web_sales_tmp"
		val dropStoreSalesTmp = "DROP TABLE IF EXISTS store_sales_tmp"  
		val dropCatalogSalesTmp = "DROP TABLE IF EXISTS catalog_sales_tmp"
		val dropItemTmp = "DROP TABLE IF EXISTS item_tmp"
		val dropPromotTmp = "DROP TABLE IF EXISTS promotion_tmp"
		val dropCustTmp = "DROP TABLE IF EXISTS customer_tmp"
		val dropDateDimTmp = "DROP TABLE IF EXISTS date_dim_tmp"
		val dropGraphTmp = "DROP TABLE IF EXISTS twitter_graph_tmp"
		val dropTweetsTmp = "DROP TABLE IF EXISTS tweets_tmp"

		
		hive_stmt.execute(dropWebSalesTmp)
		hive_stmt.execute(dropCatalogSalesTmp)
		hive_stmt.execute(dropStoreSalesTmp)
		hive_stmt.execute(dropItemTmp)
		hive_stmt.execute(dropPromotTmp)
		hive_stmt.execute(dropCustTmp)
		hive_stmt.execute(dropDateDimTmp)
		hive_stmt.execute(dropGraphTmp)
		hive_stmt.execute(dropTweetsTmp)
		
		val createWebSalesTmp = "create external table web_sales_tmp" + web_sales_schema +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + web_salesHDFSPath + "\'"
		
		val createCatalogSalesTmp = "create external table catalog_sales_tmp" + catalog_sales_schema +
					"row format delimited fields terminated by \'|\' " + "\n" + 
					"location " + "\'" + catalog_salesHDFSPath + "\'"
		
					
		val createStoreSalesTmp = "create external table store_sales_tmp" +store_sales_schema +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location" + "\'" + store_salesHDFSPath + "\'"

					
		val createItemTmp = "create external table item_tmp" + item_schema +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + itemHDFSPath + "\'"
		
		val createCustTmp = "create external table customer_tmp" + customer_schema + 
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + customerHDFSPath + "\'"
		
		val createPromotTmp = "create external table promotion_tmp" + promotion_schema + 
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + promotionHDFSPath + "\'"
					
		val createDateDimTmp = "create external table date_dim_tmp" + date_dim_schema +
				"	row format delimited fields terminated by \'|\' " +
				"	location " + "\'" + date_dimHDFSPath + "\'"
					
		val createGraphTmp = "create external table twitter_graph_tmp" + twitter_graph_schema +
				"	row format delimited fields terminated by \'|\'" +
				"	location \'" + twitter_grapgHDFSPath +"\'"
				
		/**
		 * TO DO, create json format table
		 */
		val createTweetsTmp = "create external table tweets_tmp" + tweets_schema +
				"	ROW FORMAT SERDE \'com.cloudera.hive.serde.JSONSerDe\'" +
				"	location \'" + tweets_HDFSPath +"\'"
					
		hive_stmt.execute(createWebSalesTmp)
		hive_stmt.execute(createCatalogSalesTmp)
		hive_stmt.execute(createStoreSalesTmp)
		hive_stmt.execute(createItemTmp)
		hive_stmt.execute(createCustTmp)
		hive_stmt.execute(createPromotTmp)
		hive_stmt.execute(createDateDimTmp)
		hive_stmt.execute(createGraphTmp)
		hive_stmt.execute(createTweetsTmp)
		
		
		val dropWebSales = "DROP TABLE IF EXISTS web_sales"
		val dropStoreSales = "DROP TABLE IF EXISTS store_sales"  
		val dropCatalogSales = "DROP TABLE IF EXISTS catalog_sales"
		val dropItem = "DROP TABLE IF EXISTS item"
		val dropPromot = "DROP TABLE IF EXISTS promotion"
		val dropCust = "DROP TABLE IF EXISTS customer"
		val dropDateDim = "DROP TABLE IF EXISTS date_dim"
		val dropGraph = "DROP TABLE IF EXISTS twitter_graph"
		val dropTweets = "DROP TABLE IF EXISTS tweets"

		
		hive_stmt.execute(dropWebSales)
		hive_stmt.execute(dropCatalogSales)
		hive_stmt.execute(dropStoreSales)
		hive_stmt.execute(dropItem)
		hive_stmt.execute(dropPromot)
		hive_stmt.execute(dropCust)
		hive_stmt.execute(dropDateDim)
		hive_stmt.execute(dropGraph)
		hive_stmt.execute(dropTweets)
		
		if(isBaseTableCompress) {
			hive_stmt.execute("SET hive.exec.compress.output=true")
			hive_stmt.execute("SET mapred.output.compression.type=BLOCK")
			hive_stmt.execute("SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
		}
		
		val createWebSales = "CREATE TABLE web_sales STORED AS orc as select * from web_sales_tmp"
		val createCatalogSales = "CREATE TABLE catalog_sales STORED AS orc as select * from catalog_sales_tmp"
		val createStoreSales = "CREATE TABLE store_sales STORED AS orc as select * from store_sales_tmp"
		val createItem = "CREATE TABLE item STORED AS orc as select * from item_tmp"
		val createPromot = "CREATE TABLE promotion STORED AS orc as select * from promotion_tmp"
		val createCust = "CREATE TABLE customer STORED AS orc as select * from customer_tmp"
		val createDateDim = "CREATE TABLE date_dim STORED AS orc as select * from date_dim_tmp"
		val createGraph = "CREATE TABLE twitter_graph STORED AS orc as select * from twitter_graph_tmp"
		val createTweets = "CREATE TABLE tweets STORED AS orc as select * from tweets_tmp"
	
		hive_stmt.execute(createTweets)
		hive_stmt.execute(createWebSales)
		hive_stmt.execute(createCatalogSales)
		hive_stmt.execute(createStoreSales)
		hive_stmt.execute(createItem)
		hive_stmt.execute(createPromot)
		hive_stmt.execute(createCust)
		hive_stmt.execute(createDateDim)
		hive_stmt.execute(createGraph)

	}
	
		
	/**
	 * Parquet File format
	 */
	
	def prepareTableParquetFormat(impala_connect: Connection, hive_connect: Connection, 
			isBaseTableCompress: Boolean) = {
				
		PrepareTable_ImpalaHive.LOG.info("Creating Parquet files...")
		
		val hive_stmt = hive_connect.createStatement()
		val impala_stmt = impala_connect.createStatement()
		
		val dropWebSalesTmp = "DROP TABLE IF EXISTS web_sales_tmp"
		val dropStoreSalesTmp = "DROP TABLE IF EXISTS store_sales_tmp"  
		val dropCatalogSalesTmp = "DROP TABLE IF EXISTS catalog_sales_tmp"
		val dropItemTmp = "DROP TABLE IF EXISTS item_tmp"
		val dropPromotTmp = "DROP TABLE IF EXISTS promotion_tmp"
		val dropCustTmp = "DROP TABLE IF EXISTS customer_tmp"
		val dropDateDimTmp = "DROP TABLE IF EXISTS date_dim_tmp"
		val dropGraphTmp = "DROP TABLE IF EXISTS twitter_graph_tmp"
		val dropTweetsTmp = "DROP TABLE IF EXISTS tweets_tmp"

		
		impala_stmt.execute(dropWebSalesTmp)
		impala_stmt.execute(dropCatalogSalesTmp)
		impala_stmt.execute(dropStoreSalesTmp)
		impala_stmt.execute(dropItemTmp)
		impala_stmt.execute(dropPromotTmp)
		impala_stmt.execute(dropCustTmp)
		impala_stmt.execute(dropDateDimTmp)
		impala_stmt.execute(dropGraphTmp)
		hive_stmt.execute(dropTweetsTmp)
		
		val createWebSalesTmp = "create external table web_sales_tmp" + web_sales_schema +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + web_salesHDFSPath + "\'"
		
		val createCatalogSalesTmp = "create external table catalog_sales_tmp" + catalog_sales_schema +
					"row format delimited fields terminated by \'|\' " + "\n" + 
					"location " + "\'" + catalog_salesHDFSPath + "\'"
		
					
		val createStoreSalesTmp = "create external table store_sales_tmp" +store_sales_schema +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location" + "\'" + store_salesHDFSPath + "\'"

					
		val createItemTmp = "create external table item_tmp" + item_schema +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + itemHDFSPath + "\'"
		
		val createCustTmp = "create external table customer_tmp" + customer_schema + 
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + customerHDFSPath + "\'"
		
		val createPromotTmp = "create external table promotion_tmp" + promotion_schema + 
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + promotionHDFSPath + "\'"
					
		val createDateDimTmp = "create external table date_dim_tmp" + date_dim_schema +
				"	row format delimited fields terminated by \'|\' " +
				"	location " + "\'" + date_dimHDFSPath + "\'"
					
		val createGraphTmp = "create external table twitter_graph_tmp" + twitter_graph_schema +
				"	row format delimited fields terminated by \'|\'" +
				"	location \'" + twitter_grapgHDFSPath +"\'"
				
		/**
		 * TO DO, create json format table
		 */
		val createTweetsTmp = "create external table tweets_tmp" + tweets_schema +
				"	ROW FORMAT SERDE \'com.cloudera.hive.serde.JSONSerDe\'" +
				"	location \'" + tweets_HDFSPath +"\'"
					
		impala_stmt.execute(createWebSalesTmp)
		impala_stmt.execute(createCatalogSalesTmp)
		impala_stmt.execute(createStoreSalesTmp)
		impala_stmt.execute(createItemTmp)
		impala_stmt.execute(createCustTmp)
		impala_stmt.execute(createPromotTmp)
		impala_stmt.execute(createDateDimTmp)
		impala_stmt.execute(createGraphTmp)
		hive_stmt.execute(createTweetsTmp)
		
		
		val dropWebSales = "DROP TABLE IF EXISTS web_sales"
		val dropStoreSales = "DROP TABLE IF EXISTS store_sales"  
		val dropCatalogSales = "DROP TABLE IF EXISTS catalog_sales"
		val dropItem = "DROP TABLE IF EXISTS item"
		val dropPromot = "DROP TABLE IF EXISTS promotion"
		val dropCust = "DROP TABLE IF EXISTS customer"
		val dropDateDim = "DROP TABLE IF EXISTS date_dim"
		val dropGraph = "DROP TABLE IF EXISTS twitter_graph"
		val dropTweets = "DROP TABLE IF EXISTS tweets"

		
		impala_stmt.execute(dropWebSales)
		impala_stmt.execute(dropCatalogSales)
		impala_stmt.execute(dropStoreSales)
		impala_stmt.execute(dropItem)
		impala_stmt.execute(dropPromot)
		impala_stmt.execute(dropCust)
		impala_stmt.execute(dropDateDim)
		impala_stmt.execute(dropGraph)
		hive_stmt.execute(dropTweets)
		
		if(isBaseTableCompress) {
			hive_stmt.execute("SET hive.exec.compress.output=true")
			hive_stmt.execute("SET mapred.output.compression.type=BLOCK")
			hive_stmt.execute("SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
		}
		
		val createWebSales = "CREATE TABLE web_sales STORED AS parquetfile as select * from web_sales_tmp"
		val createCatalogSales = "CREATE TABLE catalog_sales STORED AS parquetfile as select * from catalog_sales_tmp"
		val createStoreSales = "CREATE TABLE store_sales STORED AS parquetfile as select * from store_sales_tmp"
		val createItem = "CREATE TABLE item STORED AS parquetfile as select * from item_tmp"
		val createPromot = "CREATE TABLE promotion STORED AS parquetfile as select * from promotion_tmp"
		val createCust = "CREATE TABLE customer STORED AS parquetfile as select * from customer_tmp"
		val createDateDim = "CREATE TABLE date_dim STORED AS parquetfile as select * from date_dim_tmp"
		val createGraph = "CREATE TABLE twitter_graph STORED AS parquetfile as select * from twitter_graph_tmp"
		val createTweets = "CREATE TABLE tweets " +
				"			ROW FORMAT SERDE \'parquet.hive.serde.ParquetHiveSerDe\'" +
				"			STORED AS " +
				"			INPUTFORMAT \"parquet.hive.DeprecatedParquetInputFormat\"" +
				"			OUTPUTFORMAT \"parquet.hive.DeprecatedParquetOutputFormat\"" +
				"			AS select * from tweets_tmp"
	
		hive_stmt.execute(createTweets)
		impala_stmt.execute(createWebSales)
		impala_stmt.execute(createCatalogSales)
		impala_stmt.execute(createStoreSales)
		impala_stmt.execute(createItem)
		impala_stmt.execute(createPromot)
		impala_stmt.execute(createCust)
		impala_stmt.execute(createDateDim)
		impala_stmt.execute(createGraph)

	}
	
	/**
	 * Mixed Parquet and ORC File format
	 */
	
	def prepareTableMixedParquetORCFormat(impala_connect: Connection, hive_connect: Connection, 
			isBaseTableCompress: Boolean) = {
				
		PrepareTable_ImpalaHive.LOG.info("Creating mixed Parquet and ORC files...")
		
		val hive_stmt = hive_connect.createStatement()
		val impala_stmt = impala_connect.createStatement()
		
		val dropWebSalesTmp = "DROP TABLE IF EXISTS web_sales_tmp"
		val dropStoreSalesTmp = "DROP TABLE IF EXISTS store_sales_tmp"  
		val dropCatalogSalesTmp = "DROP TABLE IF EXISTS catalog_sales_tmp"
		val dropItemTmp = "DROP TABLE IF EXISTS item_tmp"
		val dropPromotTmp = "DROP TABLE IF EXISTS promotion_tmp"
		val dropCustTmp = "DROP TABLE IF EXISTS customer_tmp"
		val dropDateDimTmp = "DROP TABLE IF EXISTS date_dim_tmp"
		val dropGraphTmp = "DROP TABLE IF EXISTS twitter_graph_tmp"
		val dropTweetsTmp = "DROP TABLE IF EXISTS tweets_tmp"

		
		impala_stmt.execute(dropWebSalesTmp)
		impala_stmt.execute(dropCatalogSalesTmp)
		impala_stmt.execute(dropStoreSalesTmp)
		impala_stmt.execute(dropItemTmp)
		impala_stmt.execute(dropPromotTmp)
		impala_stmt.execute(dropCustTmp)
		hive_stmt.execute(dropDateDimTmp)
		impala_stmt.execute(dropGraphTmp)
		hive_stmt.execute(dropTweetsTmp)
		
		val createWebSalesTmp = "create external table web_sales_tmp" + web_sales_schema +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + web_salesHDFSPath + "\'"
		
		val createCatalogSalesTmp = "create external table catalog_sales_tmp" + catalog_sales_schema +
					"row format delimited fields terminated by \'|\' " + "\n" + 
					"location " + "\'" + catalog_salesHDFSPath + "\'"
		
					
		val createStoreSalesTmp = "create external table store_sales_tmp" +store_sales_schema +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location" + "\'" + store_salesHDFSPath + "\'"

					
		val createItemTmp = "create external table item_tmp" + item_schema +
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + itemHDFSPath + "\'"
		
		val createCustTmp = "create external table customer_tmp" + customer_schema + 
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + customerHDFSPath + "\'"
		
		val createPromotTmp = "create external table promotion_tmp" + promotion_schema + 
					"row format delimited fields terminated by \'|\' " + "\n" +
					"location " + "\'" + promotionHDFSPath + "\'"
					
		val createDateDimTmp = "create external table date_dim_tmp" + date_dim_schema +
				"	row format delimited fields terminated by \'|\' " +
				"	location " + "\'" + date_dimHDFSPath + "\'"
					
		val createGraphTmp = "create external table twitter_graph_tmp" + twitter_graph_schema +
				"	row format delimited fields terminated by \'|\'" +
				"	location \'" + twitter_grapgHDFSPath +"\'"
				
		/**
		 * TO DO, create json format table
		 */
		val createTweetsTmp = "create external table tweets_tmp" + tweets_schema +
				"	ROW FORMAT SERDE \'com.cloudera.hive.serde.JSONSerDe\'" +
				"	location \'" + tweets_HDFSPath +"\'"
					
		impala_stmt.execute(createWebSalesTmp)
		impala_stmt.execute(createCatalogSalesTmp)
		impala_stmt.execute(createStoreSalesTmp)
		impala_stmt.execute(createItemTmp)
		impala_stmt.execute(createCustTmp)
		impala_stmt.execute(createPromotTmp)
		hive_stmt.execute(createDateDimTmp)
		impala_stmt.execute(createGraphTmp)
		hive_stmt.execute(createTweetsTmp)
		
		
		val dropWebSales = "DROP TABLE IF EXISTS web_sales"
		val dropStoreSales = "DROP TABLE IF EXISTS store_sales"  
		val dropCatalogSales = "DROP TABLE IF EXISTS catalog_sales"
		val dropItem = "DROP TABLE IF EXISTS item"
		val dropPromot = "DROP TABLE IF EXISTS promotion"
		val dropCust = "DROP TABLE IF EXISTS customer"
		val dropDateDim = "DROP TABLE IF EXISTS date_dim"
		val dropGraph = "DROP TABLE IF EXISTS twitter_graph"
		val dropTweets = "DROP TABLE IF EXISTS tweets"

		
		impala_stmt.execute(dropWebSales)
		impala_stmt.execute(dropCatalogSales)
		impala_stmt.execute(dropStoreSales)
		impala_stmt.execute(dropItem)
		impala_stmt.execute(dropPromot)
		impala_stmt.execute(dropCust)
		hive_stmt.execute(dropDateDim)
		impala_stmt.execute(dropGraph)
		hive_stmt.execute(dropTweets)
		
		if(isBaseTableCompress) {
			hive_stmt.execute("SET hive.exec.compress.output=true")
			hive_stmt.execute("SET mapred.output.compression.type=BLOCK")
			hive_stmt.execute("SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")
			
		}
		
		
		val createWebSales = "create table web_sales" + web_sales_schema + "STORED AS parquet"
		val createCatalogSales = "create table catalog_sales" + catalog_sales_schema + "STORED AS parquet"
		val createStoreSales = "create table store_sales" + store_sales_schema + "STORED AS parquet"
		val createItem = "create table item" + item_schema + "STORED AS parquet"
		val createCust = "create table customer" + customer_schema + "STORED AS parquet"
		val createPromot = "create table promotion" + promotion_schema + "STORED AS parquet"
		val createGraph = "create  table twitter_graph" + twitter_graph_schema + "STORED AS parquet"
	    val createDateDim = "CREATE TABLE date_dim STORED AS orc as select * from date_dim_tmp"
	    val createTweets = "CREATE TABLE tweets STORED AS orc as select * from tweets_tmp"

				
		hive_stmt.execute(createTweets)
		impala_stmt.execute(createWebSales)
		impala_stmt.execute(createCatalogSales)
		impala_stmt.execute(createStoreSales)
		impala_stmt.execute(createItem)
		impala_stmt.execute(createPromot)
		impala_stmt.execute(createCust)
		hive_stmt.execute(createDateDim)
		impala_stmt.execute(createGraph)
				
		
		val insertWebSales = "INSERT INTO TABLE web_sales select * from web_sales_tmp"
		val insertCatalogSales = "INSERT INTO TABLE catalog_sales  select * from catalog_sales_tmp"
		val insertStoreSales = "INSERT INTO TABLE store_sales  select * from store_sales_tmp"
		val insertItem = "INSERT INTO TABLE item select * from item_tmp"
		val insertPromot = "INSERT INTO TABLE promotion  select * from promotion_tmp"
		val insertCust = "INSERT INTO TABLE customer  select * from customer_tmp"
		val insertGraph = "INSERT INTO TABLE twitter_graph select * from twitter_graph_tmp"
	
		impala_stmt.executeUpdate(insertWebSales)
		impala_stmt.executeUpdate(insertCatalogSales)
		impala_stmt.executeUpdate(insertStoreSales)
		impala_stmt.executeUpdate(insertItem)
		impala_stmt.executeUpdate(insertPromot)
		impala_stmt.executeUpdate(insertCust)
		impala_stmt.executeUpdate(insertGraph)

		impala_stmt.close()
		hive_stmt.close()
		
		
		computeStat_Impala(impala_connect)
		
	}
	
	def computeStat_Impala (impala_connect: Connection) = {
		
		val impala_stmt = impala_connect.createStatement()
		
		PrepareTable_ImpalaHive.LOG.info("Computing impala table statistic...")
		val computeStatWebSales = "COMPUTE STATS web_sales"
		val computeStatCatalogSales = "COMPUTE STATS catalog_sales"
		val computeStatStoreSales = "COMPUTE STATS store_sales"
		val computeStatItem = "COMPUTE STATS item"
		val computeStatPromt = "COMPUTE STATS promotion"
		val computeStatCust = "COMPUTE STATS customer"
		val computeStatTwitterGraph = "COMPUTE STATS twitter_graph"

		impala_stmt.execute(computeStatWebSales)
		impala_stmt.execute(computeStatCatalogSales)
		impala_stmt.execute(computeStatStoreSales)
		impala_stmt.execute(computeStatItem)
		impala_stmt.execute(computeStatPromt)
		impala_stmt.execute(computeStatCust)
		impala_stmt.execute(computeStatTwitterGraph)
		
		impala_stmt.close()
	}
	
}