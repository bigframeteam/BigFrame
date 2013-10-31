package bigframe.datagen.appDomainInfo;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.graph.GraphDataGen;
import bigframe.datagen.graph.kroneckerGraph.KnonGraphConstants;
import bigframe.datagen.graph.kroneckerGraph.KronGraphGenHadoop;
import bigframe.datagen.nested.NestedDataGen;
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
 * A class encapsulates all the data generation restraint for 
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
		dataVariety.add(Constants.RELATIONAL);
		
		RelationalDataGen tpcds = new TpcdsDataGenNaive(datainputformat,
				datainputformat.getDataTypeTargetGB(Constants.RELATIONAL));
		datagen_map.put(Constants.RELATIONAL, tpcds);
		
		DataPreparator tpcds_prepare = new TpcdsDataPreparator(datainputformat);
		dataPrep_map.put(Constants.RELATIONAL, tpcds_prepare);
		
		DataProducer tpcds_producer = new TpcdsDataProducer(datainputformat);
		dataProd_map.put(Constants.RELATIONAL, tpcds_producer);
		
		dataVariety.add(Constants.GRAPH);
		
		GraphDataGen twitter_graph = new KronGraphGenHadoop(datainputformat,
				datainputformat.getDataTypeTargetGB(Constants.GRAPH));
		
		datagen_map.put(Constants.GRAPH, twitter_graph);
		DataPreparator twitter_graph_prepare = new TwitterGraphPreparator(datainputformat);
		dataPrep_map.put(Constants.GRAPH, twitter_graph_prepare);
		
		dataVariety.add(Constants.NESTED);
		NestedDataGen tweets = new RawTweetGenHadoop(datainputformat,
				datainputformat.getDataTypeTargetGB(Constants.NESTED));
		datagen_map.put(Constants.NESTED, tweets);
		
		DataProducer tweets_producer = new TweetDataProducer(datainputformat);
		dataProd_map.put(Constants.NESTED, tweets_producer);
		
		
		dataVariety.add("text");
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
