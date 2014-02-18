package bigframe.workflows.BusinessIntelligence.RTG.exploratory

import java.sql.Connection
import java.sql.SQLException

import bigframe.workflows.BaseTablePath

class PrepareTable_Hive(basePath: BaseTablePath) {

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
	def prepareTableImpl1(connection: Connection): Unit = {

		val stmt = connection.createStatement()


		val dropWebSales = "DROP TABLE IF EXISTS web_sales"
		val dropStoreSales = "DROP TABLE IF EXISTS store_sales"  
		val dropCatalogSales = "DROP TABLE IF EXISTS catalog_sales"
		val dropItem = "DROP TABLE IF EXISTS item"
		val dropPromot = "DROP TABLE IF EXISTS promotion"
		val dropCust = "DROP TABLE IF EXISTS customer"
		val dropDateDim = "DROP TABLE IF EXISTS date_dim"
		val dropGraph = "DROP TABLE IF EXISTS twitter_graph"
		val dropTweets = "DROP TABLE IF EXISTS tweets"


		stmt.execute(dropWebSales)
		stmt.execute(dropCatalogSales)
		stmt.execute(dropStoreSales)
		stmt.execute(dropItem)
		stmt.execute(dropPromot)
		stmt.execute(dropCust)
		stmt.execute(dropDateDim)
		stmt.execute(dropGraph)
		stmt.execute(dropTweets)

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

		val createDateDim = "create external table date_dim" +
				"	(" +
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
		    	"	)" +
				"	row format delimited fields terminated by \'|\' " +
				"	location " + "\'" + date_dimHDFSPath + "\'"

		val createGraph = "create external table twitter_graph" +
				"	(" +
				"		friend_id	int," +
				"		follower_id	int" +
				"	)" +
				"	row format delimited fields terminated by \'|\'" +
				"	location \'" + twitter_grapgHDFSPath +"\'"

		/**
		 * TO DO, create json format table
		 */
		val createTweets = "create external table tweets" +
				"	(" +
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
				"	)" +
				"	ROW FORMAT SERDE \'org.openx.data.jsonserde.JsonSerDe\'" +
				"	location \'" + tweets_HDFSPath +"\'"

		stmt.execute(createTweets)
		stmt.execute(createWebSales)
		stmt.execute(createCatalogSales)
		stmt.execute(createStoreSales)
		stmt.execute(createItem)
		stmt.execute(createCust)
		stmt.execute(createPromot)
		stmt.execute(createDateDim)
		stmt.execute(createGraph)


	}

	/**
	 * ORC file is used
	 */
	def prepareTableImpl2(connection: Connection): Unit = {

		println("Creating ORC file...")

		val stmt = connection.createStatement();


		val dropWebSalesTmp = "DROP TABLE IF EXISTS web_sales_tmp"
		val dropStoreSalesTmp = "DROP TABLE IF EXISTS store_sales_tmp"  
		val dropCatalogSalesTmp = "DROP TABLE IF EXISTS catalog_sales_tmp"
		val dropItemTmp = "DROP TABLE IF EXISTS item_tmp"
		val dropPromotTmp = "DROP TABLE IF EXISTS promotion_tmp"
		val dropCustTmp = "DROP TABLE IF EXISTS customer_tmp"
		val dropDateDimTmp = "DROP TABLE IF EXISTS date_dim_tmp"
		val dropGraphTmp = "DROP TABLE IF EXISTS twitter_graph_tmp"
		val dropTweetsTmp = "DROP TABLE IF EXISTS tweets_tmp"


		stmt.execute(dropWebSalesTmp)
		stmt.execute(dropCatalogSalesTmp)
		stmt.execute(dropStoreSalesTmp)
		stmt.execute(dropItemTmp)
		stmt.execute(dropPromotTmp)
		stmt.execute(dropCustTmp)
		stmt.execute(dropDateDimTmp)
		stmt.execute(dropGraphTmp)
		stmt.execute(dropTweetsTmp)

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

		val createCatalogSalesTmp = "create external table catalog_sales_tmp" +
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


		val createStoreSalesTmp = "create external table store_sales_tmp" +
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


		val createItemTmp = "create external table item_tmp" + 
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

		val createCustTmp = "create external table customer_tmp" + 
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

		val createPromotTmp = "create external table promotion_tmp" + 
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

		val createDateDimTmp = "create external table date_dim_tmp" +
				"	(" +
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
		    	"	)" +
				"	row format delimited fields terminated by \'|\' " +
				"	location " + "\'" + date_dimHDFSPath + "\'"

		val createGraphTmp = "create external table twitter_graph_tmp" +
				"	(" +
				"		friend_id	int," +
				"		follower_id	int" +
				"	)" +
				"	row format delimited fields terminated by \'|\'" +
				"	location \'" + twitter_grapgHDFSPath +"\'"

		/**
		 * TO DO, create json format table
		 */
		val createTweetsTmp = "create external table tweets_tmp" +
				"	(" +
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
				"	)" +
				"	ROW FORMAT SERDE \'org.openx.data.jsonserde.JsonSerDe\'" +
				"	location \'" + tweets_HDFSPath +"\'"

		stmt.execute(createWebSalesTmp)
		stmt.execute(createCatalogSalesTmp)
		stmt.execute(createStoreSalesTmp)
		stmt.execute(createItemTmp)
		stmt.execute(createCustTmp)
		stmt.execute(createPromotTmp)
		stmt.execute(createDateDimTmp)
		stmt.execute(createGraphTmp)
		stmt.execute(createTweetsTmp)


		val dropWebSales = "DROP TABLE IF EXISTS web_sales"
		val dropStoreSales = "DROP TABLE IF EXISTS store_sales"  
		val dropCatalogSales = "DROP TABLE IF EXISTS catalog_sales"
		val dropItem = "DROP TABLE IF EXISTS item"
		val dropPromot = "DROP TABLE IF EXISTS promotion"
		val dropCust = "DROP TABLE IF EXISTS customer"
		val dropDateDim = "DROP TABLE IF EXISTS date_dim"
		val dropGraph = "DROP TABLE IF EXISTS twitter_graph"
		val dropTweets = "DROP TABLE IF EXISTS tweets"


		stmt.execute(dropWebSales)
		stmt.execute(dropCatalogSales)
		stmt.execute(dropStoreSales)
		stmt.execute(dropItem)
		stmt.execute(dropPromot)
		stmt.execute(dropCust)
		stmt.execute(dropDateDim)
		stmt.execute(dropGraph)
		stmt.execute(dropTweets)

		val createWebSales = "CREATE TABLE web_sales STORED AS orc as select * from web_sales_tmp"
		val createCatalogSales = "CREATE TABLE catalog_sales STORED AS orc as select * from catalog_sales_tmp"
		val createStoreSales = "CREATE TABLE store_sales STORED AS orc as select * from store_sales_tmp"
		val createItem = "CREATE TABLE item STORED AS orc as select * from item_tmp"
		val createPromot = "CREATE TABLE promotion STORED AS orc as select * from promotion_tmp"
		val createCust = "CREATE TABLE customer STORED AS orc as select * from customer_tmp"
		val createDateDim = "CREATE TABLE date_dim STORED AS orc as select * from date_dim_tmp"
		val createGraph = "CREATE TABLE twitter_graph STORED AS orc as select * from twitter_graph_tmp"
		val createTweets = "CREATE TABLE tweets STORED AS orc as select * from tweets_tmp"

		stmt.execute(createTweets)
		stmt.execute(createWebSales)
		stmt.execute(createCatalogSales)
		stmt.execute(createStoreSales)
		stmt.execute(createItem)
		stmt.execute(createPromot)
		stmt.execute(createCust)
		stmt.execute(createDateDim)
		stmt.execute(createGraph)




	}


	/**
	 * RC file is used and also enable Snappy Compression.
	 */
	def prepareTableImpl3(connection: Connection): Unit = {

		println("Creating RC file...")

		val stmt = connection.createStatement();


		val dropWebSalesTmp = "DROP TABLE IF EXISTS web_sales_tmp"
		val dropStoreSalesTmp = "DROP TABLE IF EXISTS store_sales_tmp"  
		val dropCatalogSalesTmp = "DROP TABLE IF EXISTS catalog_sales_tmp"
		val dropItemTmp = "DROP TABLE IF EXISTS item_tmp"
		val dropPromotTmp = "DROP TABLE IF EXISTS promotion_tmp"
		val dropCustTmp = "DROP TABLE IF EXISTS customer_tmp"
		val dropDateDimTmp = "DROP TABLE IF EXISTS date_dim_tmp"
		val dropGraphTmp = "DROP TABLE IF EXISTS twitter_graph_tmp"
		val dropTweetsTmp = "DROP TABLE IF EXISTS tweets_tmp"


		stmt.execute(dropWebSalesTmp)
		stmt.execute(dropCatalogSalesTmp)
		stmt.execute(dropStoreSalesTmp)
		stmt.execute(dropItemTmp)
		stmt.execute(dropPromotTmp)
		stmt.execute(dropCustTmp)
		stmt.execute(dropDateDimTmp)
		stmt.execute(dropGraphTmp)
		stmt.execute(dropTweetsTmp)

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

		val createCatalogSalesTmp = "create external table catalog_sales_tmp" +
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


		val createStoreSalesTmp = "create external table store_sales_tmp" +
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


		val createItemTmp = "create external table item_tmp" + 
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

		val createCustTmp = "create external table customer_tmp" + 
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

		val createPromotTmp = "create external table promotion_tmp" + 
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

		val createDateDimTmp = "create external table date_dim_tmp" +
				"	(" +
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
		    	"	)" +
				"	row format delimited fields terminated by \'|\' " +
				"	location " + "\'" + date_dimHDFSPath + "\'"

		val createGraphTmp = "create external table twitter_graph_tmp" +
				"	(" +
				"		friend_id	int," +
				"		follower_id	int" +
				"	)" +
				"	row format delimited fields terminated by \'|\'" +
				"	location \'" + twitter_grapgHDFSPath +"\'"

		/**
		 * TO DO, create json format table
		 */
		val createTweetsTmp = "create external table tweets_tmp" +
				"	(" +
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
				"	)" +
				"	ROW FORMAT SERDE \'org.openx.data.jsonserde.JsonSerDe\'" +
				"	location \'" + tweets_HDFSPath +"\'"

		stmt.execute(createWebSalesTmp)
		stmt.execute(createCatalogSalesTmp)
		stmt.execute(createStoreSalesTmp)
		stmt.execute(createItemTmp)
		stmt.execute(createCustTmp)
		stmt.execute(createPromotTmp)
		stmt.execute(createDateDimTmp)
		stmt.execute(createGraphTmp)
		stmt.execute(createTweetsTmp)


		val dropWebSales = "DROP TABLE web_sales"
		val dropStoreSales = "DROP TABLE store_sales"  
		val dropCatalogSales = "DROP TABLE catalog_sales"
		val dropItem = "DROP TABLE item"
		val dropPromot = "DROP TABLE promotion"
		val dropCust = "DROP TABLE customer"
		val dropDateDim = "DROP TABLE date_dim"
		val dropGraph = "DROP TABLE twitter_graph"
		val dropTweets = "DROP TABLE tweets"


		stmt.execute(dropWebSales)
		stmt.execute(dropCatalogSales)
		stmt.execute(dropStoreSales)
		stmt.execute(dropItem)
		stmt.execute(dropPromot)
		stmt.execute(dropCust)
		stmt.execute(dropDateDim)
		stmt.execute(dropGraph)
		stmt.execute(dropTweets)

		stmt.execute("SET hive.exec.compress.output=true")
		stmt.execute("SET mapred.output.compression.type=BLOCK")
		stmt.execute("SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec")

		val createWebSales = "CREATE TABLE web_sales STORED AS rcfile as select * from web_sales_tmp"
		val createCatalogSales = "CREATE TABLE catalog_sales STORED AS rcfile as select * from catalog_sales_tmp"
		val createStoreSales = "CREATE TABLE store_sales STORED AS rcfile as select * from store_sales_tmp"
		val createItem = "CREATE TABLE item STORED AS rcfile as select * from item_tmp"
		val createPromot = "CREATE TABLE promotion STORED AS rcfile as select * from promotion_tmp"
		val createCust = "CREATE TABLE customer STORED AS rcfile as select * from customer_tmp"
		val createDateDim = "CREATE TABLE date_dim STORED AS rcfile as select * from date_dim_tmp"
		val createGraph = "CREATE TABLE twitter_graph STORED AS rcfile as select * from twitter_graph_tmp"
		val createTweets = "CREATE TABLE tweets STORED AS rcfile as select * from tweets_tmp"

		stmt.execute(createTweets)
		stmt.execute(createWebSales)
		stmt.execute(createCatalogSales)
		stmt.execute(createStoreSales)
		stmt.execute(createItem)
		stmt.execute(createPromot)
		stmt.execute(createCust)
		stmt.execute(createDateDim)
		stmt.execute(createGraph)


	}
}
