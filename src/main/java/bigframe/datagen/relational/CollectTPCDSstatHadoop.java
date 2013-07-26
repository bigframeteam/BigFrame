package bigframe.datagen.relational;

import java.io.BufferedReader;
import java.util.List;

import javax.xml.soap.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;



public class CollectTPCDSstatHadoop extends CollectTPCDSstat {

	
	class GetPromotedProdsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
	}

	
	@Override
	public List<TpcdsPromotedProduct> getPromotedProds() {
		// TODO Auto-generated method stub
		
		
		
		return null;
	}

/*
	@Override
	public void IntialCustTwitterAcc(String hdfs_path, DatagenConf conf) {

	}
*/
	@Override
	public long [] getNonCustTwitterAcc(long[] customer_twitterAcc, int num_twitter_user) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public long[] getCustTwitterAcc(float tpcds_targetGB, float graph_targetGB) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public long getNumOfCustomer(int targetGB) {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public long getNumOfItem(int targetGB) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void collectHDFSPromtTBL(Configuration mapreduce_config,
			String tbl_file, TpcdsPromotionInfo promt_info) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void collectHDFSItemTBL(Configuration mapreduce_config,
			String tbl_file, TpcdsItemInfo item_info) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setItemResult(BufferedReader in, TpcdsItemInfo item_info) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPromtResult(BufferedReader in, TpcdsPromotionInfo promt_info) {
		// TODO Auto-generated method stub
		
	}

}
