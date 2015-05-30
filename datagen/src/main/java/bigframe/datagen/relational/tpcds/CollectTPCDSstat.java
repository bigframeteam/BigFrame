package bigframe.datagen.relational.tpcds;

import java.io.BufferedReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;





public abstract class CollectTPCDSstat {
	Set<Integer> customer_account;

	public CollectTPCDSstat() {
		customer_account = new HashSet<Integer>();
	}

	public abstract List<TpcdsPromotedProduct> getPromotedProds();

	public Date getTPCDSdateBegin() {
		try {
			return TpcdsConstants.dateformatter.parse(TpcdsConstants.TPCDS_BEGINDATE);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public Date getTPCDSdateEnd() {
		try {
			return TpcdsConstants.dateformatter.parse(TpcdsConstants.TPCDS_ENDDATE);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	// So these numbers are TPCDS specific
	public static Date getDateBySK(int dateSK) {
		if(dateSK < 2415022 || dateSK > 2488070) {
			return null;
		}
		int baseSK = 2415022;
		int offset = dateSK - baseSK;

		Calendar c = Calendar.getInstance();
		c.set(1900,  Calendar.JANUARY, 2);

		c.add(Calendar.DATE, offset);

		return c.getTime();

	}

	public abstract  long getNumOfCustomer(float targetGB);
	public abstract long getNumOfItem(float targetGB);

	public abstract long[] getCustTwitterAcc(float tpcds_targetGB, float graph_targetGB);
	public abstract long [] getNonCustTwitterAcc(long[] customer_twitterAcc, int num_twitter_user);

	public abstract void setItemResult(BufferedReader in, TpcdsItemInfo item_info);
	public abstract void setPromtResult(BufferedReader in, TpcdsPromotionInfo promt_info);
	
	public abstract void collectHDFSPromtTBL(Configuration mapreduce_config, String tbl_file, TpcdsPromotionInfo promt_info);
	public abstract void collectHDFSItemTBL(Configuration mapreduce_config,
			String tbl_file, TpcdsItemInfo item_info);
}
