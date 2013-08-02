package bigframe.generator.datagen.relational;

import java.io.BufferedReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

	//public abstract void IntialCustTwitterAcc(String hdfs_path, DatagenConf conf);



	public Date getTPCDSdateBegin() {
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

		try {
			return formatter.parse(TpcdsConstants.TPCDS_BEGINDATE);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public Date getTPCDSdateEnd() {
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

		try {
			return formatter.parse(TpcdsConstants.TPCDS_ENDDATE);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public abstract  long getNumOfCustomer(int targetGB);
	public abstract long getNumOfItem(int targetGB);

	public abstract long[] getCustTwitterAcc(float tpcds_targetGB, float graph_targetGB);
	public abstract long [] getNonCustTwitterAcc(long[] customer_twitterAcc, int num_twitter_user);

	public abstract void setItemResult(BufferedReader in, TpcdsItemInfo item_info);
	public abstract void setPromtResult(BufferedReader in, TpcdsPromotionInfo promt_info);
	
	public abstract void collectHDFSPromtTBL(Configuration mapreduce_config, String tbl_file, TpcdsPromotionInfo promt_info);
	public abstract void collectHDFSItemTBL(Configuration mapreduce_config,
			String tbl_file, TpcdsItemInfo item_info);
}
