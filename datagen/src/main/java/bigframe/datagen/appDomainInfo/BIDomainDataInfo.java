package bigframe.datagen.appDomainInfo;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.DataGenerator;
import bigframe.datagen.graph.GraphDataGen;
import bigframe.datagen.graph.kroneckerGraph.KnonGraphConstants;
import bigframe.datagen.graph.kroneckerGraph.KronGraphGenHadoop;
import bigframe.datagen.nested.NestedDataGen;
import bigframe.datagen.nested.tweet.RawTweetGenGiraph;
import bigframe.datagen.nested.tweet.RawTweetGenHadoop;
import bigframe.datagen.relational.RelationalDataGen;
import bigframe.datagen.relational.tpcds.TpcdsDataGenNaive;
import bigframe.refreshing.DataPreparator;
import bigframe.refreshing.DataProducer;
import bigframe.refreshing.graph.TwitterGraphPreparator;
import bigframe.refreshing.nested.TweetDataProducer;
import bigframe.refreshing.relational.TpcdsDataPreparator;
import bigframe.refreshing.relational.TpcdsDataProducer;
import bigframe.util.Constants;

/**
 * A class encapsulates all the data generation constraints for 
 * the Business Intelligence application domain.  
 * 
 * @author andy
 *
 */
public class BIDomainDataInfo extends DomainDataInfo {
	

	public static final float R_STREAM_GB = 1;
	public static final float G_STREAM_GB = KnonGraphConstants.arScaleVolume2[0];
	
	private static final Log LOG = LogFactory.getLog(BIDomainDataInfo.class);
	
	public BIDomainDataInfo(BigDataInputFormat datainputformat) {
		super(datainputformat);
		
		// The data type supported by BI domain and their corresponding 
		// DataGenerator and DataPreparator
		supported_dataVariety.add(Constants.RELATIONAL);
		supported_dataVariety.add(Constants.GRAPH);
		supported_dataVariety.add(Constants.NESTED);
	}

	
	public  List<DataGenerator> getDataGens() {
		if (!isBigDataIFvalid()) {
			return null;
		}
		RelationalDataGen tpcds = new TpcdsDataGenNaive(datainputformat,
				datainputformat.getDataTypeTargetGB(Constants.RELATIONAL));
		datagen_map.put(Constants.RELATIONAL, tpcds);
		
		GraphDataGen twitter_graph = new KronGraphGenHadoop(datainputformat,
				datainputformat.getDataTypeTargetGB(Constants.GRAPH));
		datagen_map.put(Constants.GRAPH, twitter_graph);
		
//		NestedDataGen tweets = new RawTweetGenHadoop(datainputformat,
//				datainputformat.getDataTypeTargetGB(Constants.NESTED));
  NestedDataGen tweets = new RawTweetGenGiraph(datainputformat,
  datainputformat.getDataTypeTargetGB(Constants.NESTED));
		datagen_map.put(Constants.NESTED, tweets);
		
		List<DataGenerator> datagen_list = new LinkedList<DataGenerator>();
		Set<String> dataVariety = datainputformat.getDataVariety();
		
		for(String variety : dataVariety) {
			datagen_list.add(datagen_map.get(variety));
		}
		
		return datagen_list;
	}
	
	
	public List<DataPreparator> getDataPreps() {
		if (!isBigDataIFvalid()) {
			return null;
		}
	
		DataPreparator tpcds_prepare = new TpcdsDataPreparator(datainputformat);
		dataPrep_map.put(Constants.RELATIONAL, tpcds_prepare);
		
		DataPreparator twitter_graph_prepare = new TwitterGraphPreparator(datainputformat);
		dataPrep_map.put(Constants.GRAPH, twitter_graph_prepare);
		
		List<DataPreparator> dataPrep_list = new LinkedList<DataPreparator>();
		Set<String> dataVariety = datainputformat.getDataVariety();
		
		
		
		for(String variety : dataVariety) {
			if(dataPrep_map.containsKey(variety))
				dataPrep_list.add(dataPrep_map.get(variety));
		}
		return dataPrep_list;
	}
	
	public List<DataProducer> getDataProds() {
		if (!isBigDataIFvalid()) {
			return null;
		}
		DataProducer tpcds_producer = new TpcdsDataProducer(datainputformat);
		dataProd_map.put(Constants.RELATIONAL, tpcds_producer);
		
		DataProducer tweets_producer = new TweetDataProducer(datainputformat);
		dataProd_map.put(Constants.NESTED, tweets_producer);
		
		List<DataProducer> dataProd_list = new LinkedList<DataProducer>();
		Set<String> dataVariety = datainputformat.getDataVariety();
		
		for(String variety : dataVariety) {
			if(dataProd_map.containsKey(variety))
				dataProd_list.add(dataProd_map.get(variety));
		}
		return dataProd_list;
	}

	@Override
	protected boolean isBigDataIFvalid() {
		// TODO Auto-generated method stub
		
		Set<String> dataVariety = datainputformat.getDataVariety();
		
		for(String variety : dataVariety) {
			if (!containDataVariety(variety)) {
				LOG.error("BI domain does not contain data type: " + variety);
				return false;
			}
		}
		
		return true;
	}
	

}
