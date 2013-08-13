package bigframe.datagen.factory;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bigframe.bigif.BigDataInputFormat;
import bigframe.datagen.DataGenerator;
import bigframe.datagen.graph.GraphDataGen;
import bigframe.datagen.graph.kroneckerGraph.KronGraphGenHadoop;
import bigframe.datagen.nested.NestedDataGen;
import bigframe.datagen.nested.tweet.RawTweetGenHadoop;
import bigframe.datagen.relational.RelationalDataGen;
import bigframe.datagen.relational.tpcds.TpcdsDataGenNaive;
import bigframe.util.Constants;

public class BIDomainDataInfo extends DomainDataInfo {
	

	private static final Log LOG = LogFactory.getLog(BIDomainDataInfo.class);
	
	public BIDomainDataInfo(BigDataInputFormat datainputformat) {
		super(datainputformat);
		
		// The data type supported by BI domain and their corresponding 
		// DataGenerator
		dataVariety.add(Constants.RELATIONAL);
		RelationalDataGen tpcds = new TpcdsDataGenNaive(datainputformat,
				datainputformat.getDataTypeTargetGB(Constants.RELATIONAL));
		datagen_map.put(Constants.RELATIONAL, tpcds);
		
		dataVariety.add(Constants.GRAPH);
		GraphDataGen twitter_graph = new KronGraphGenHadoop(datainputformat,
				datainputformat.getDataTypeTargetGB(Constants.GRAPH));
		datagen_map.put(Constants.GRAPH, twitter_graph);
		
		dataVariety.add(Constants.NESTED);
		NestedDataGen tweets = new RawTweetGenHadoop(datainputformat,
				datainputformat.getDataTypeTargetGB(Constants.NESTED));
		datagen_map.put(Constants.NESTED, tweets);
		
		dataVariety.add("text");
	}

	@Override
	public List<DataGenerator> getDataGens() {
		
		if (!isBigDataIFvalid()) {
			return null;
		}
		
		List<DataGenerator> datagen_list = new LinkedList<DataGenerator>();
		Set<String> dataVariety = datainputformat.getDataVariety();
		
		for(String variety : dataVariety) {
			datagen_list.add(datagen_map.get(variety));
		}
		return datagen_list;
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
