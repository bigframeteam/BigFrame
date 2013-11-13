package bigframe.util.dataloader.hana;

import java.sql.SQLException;
import java.sql.Statement;

import bigframe.bigif.WorkflowInputFormat;

public class HanaTpcdsLoader extends HanaDataLoader {

	public HanaTpcdsLoader(WorkflowInputFormat workIF) {
		super(workIF);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean createTable() {
		String customer = " create column table customer( " +
				"c_customer_sk int, c_customer_id char(16), " +
				"c_current_cdemo_sk int, c_current_hdemo_sk int, c_current_addr_sk int, c_first_shipto_date_sk int, " +
				"c_first_sales_date_sk int, c_salutation char(10), c_first_name char(20), c_last_name char(30), " +
				"c_preferred_cust_flag char(1), c_birth_day int, c_birth_month int, c_birth_year int, " +
				"c_birth_country varchar(20), c_login char(13), c_email_address char(50), c_last_review_date_sk int " +
				")";

		String catalog_sales = "create column table catalog_sales( " +
				"cs_sold_date_sk int, cs_sold_time_sk int, " +
				"cs_ship_date_sk int, cs_bill_customer_sk int, cs_bill_cdemo_sk int, cs_bill_hdemo_sk int, " +
				"cs_bill_addr_sk int, cs_ship_customer_sk int, cs_ship_cdemo_sk int, cs_ship_hdemo_sk int, " +
				"cs_ship_addr_sk int, cs_call_center_sk int, cs_catalog_page_sk int, cs_ship_mode_sk int, " +
				"cs_warehouse_sk int, cs_item_sk int, cs_promo_sk int, cs_order_number int, cs_quantity int, " +
				"cs_wholesale_cost float, cs_list_price float, cs_sales_price float, cs_ext_discount_amt float, " +
				"cs_ext_sales_price float, cs_ext_wholesale_cost float, cs_ext_list_price float, " +
				"cs_ext_tax float, cs_coupon_amt float, cs_ext_ship_cost float, cs_net_paid float, " +
				"cs_net_paid_inc_tax float, cs_net_paid_inc_ship float, cs_net_paid_inc_ship_tax float, " +
				"cs_net_profit float " +
				") ";

		String store_sales = "create column table store_sales ( " +
				"ss_sold_date_sk int,ss_sold_time_sk int ,ss_item_sk int, " +
				"ss_customer_sk int,ss_cdemo_sk int,ss_hdemo_sk int,ss_addr_sk  int,ss_store_sk int,ss_promo_sk int, "  +
				"ss_ticket_number int,ss_quantity int,ss_wholesale_cost float,ss_list_price float,ss_sales_price float, " +
				"ss_ext_discount_amt float,ss_ext_sales_price float,ss_ext_wholesale_cost float,ss_ext_list_price float, " +
				"ss_ext_tax float,ss_coupon_amt float,ss_net_paid float ,ss_net_paid_inc_tax float, ss_net_profit float " +
				") "; 

		String web_sales = "create column table web_sales ( " +
				"ws_sold_date_sk  int, ws_sold_time_sk  int, ws_ship_date_sk int, " +
				"ws_item_sk int, ws_bill_customer_sk  int, ws_bill_cdemo_sk int, ws_bill_hdemo_sk int, ws_bill_addr_sk int, " +
				"ws_ship_customer_sk int, ws_ship_cdemo_sk int, ws_ship_hdemo_sk int, ws_ship_addr_sk int, ws_web_page_sk int, " +
				"ws_web_site_sk int, ws_ship_mode_sk int, ws_warehouse_sk int, ws_promo_sk int, ws_order_number int, " +
				"ws_quantity  int, ws_wholesale_cost float, ws_list_price float, ws_sales_price float, ws_ext_discount_amt float, " +
				"ws_ext_sales_price float, ws_ext_wholesale_cost float, ws_ext_list_price float, ws_ext_tax float, " +
				"ws_coupon_amt float, ws_ext_ship_cost float, ws_net_paid float, ws_net_paid_inc_tax float, ws_net_paid_inc_ship float, " +
				"ws_net_paid_inc_ship_tax  float, ws_net_profit float " +
				")";
				
		String item =	"create column table item ( " +
				"i_item_sk int, i_item_id char(16), i_rec_start_date date, i_rec_end_date date, " +
				"i_item_desc varchar(200), i_current_price float, i_wholesale_cost float, i_brand_id int, i_brand char(50), " +
				"i_claws_id int, i_class char(50), i_category_id int, i_category char(50), i_manufact_id int, i_manufact char(50), " +
				"i_size char(20), i_formulation char(20), i_color char(20), i_units char(10), i_container char(10), i_manager_id int, " +
				"i_product_name char(50) " +
				")";

		String promotion =	"create column table promotion ( " +
				"p_promo_sk int, p_promo_id char(16), p_start_date_sk int, " +
				"p_end_date_sk int, p_item_sk int, p_cost float, p_response_target int, p_promo_name char(50), " +
				"p_channel_dmail char(1), p_channel_email char(1), p_channel_catalog char(1), p_channel_tv char(1), " +
				"p_channel_radio char(1), p_channel_press char(1), p_channel_event char(1), p_channel_demo char(1), " +
				"p_channel_details varchar(100), p_purpose char(15), p_discount_active char(1) " +
				")";
		
		try {
			Statement stmt = connection.createStatement();
			stmt.execute(customer);
			stmt.execute(catalog_sales);
			stmt.execute(store_sales);
			stmt.execute(web_sales);
			stmt.execute(item);
			stmt.execute(promotion);
 
		} catch (SQLException e) {
			System.out.println("Please make sure to drop tables before load the data. \n" +
				"(customer, catalog_sales, store_sales, web_sales, item, promotion)");
			return false;
		}
		return true;
	
	}

	@Override
	public boolean load() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean preProcess(String srcHdfsPath) {
		// TODO Auto-generated method stub
		return false;
	}
}
