package bigframe.datagen;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import bigframe.datagen.graph.GraphDataGen;
import bigframe.datagen.graph.KronGraphGenHadoop;
import bigframe.datagen.nested.NestedDataGen;
import bigframe.datagen.nested.RawTweetGenHadoop;
import bigframe.datagen.relational.RelationalDataGen;
import bigframe.datagen.relational.TpcdsDataGenNaive;
import bigframe.util.Constants;


public class DatagenFactory {
	private static final Logger LOG = Logger.getLogger(DatagenFactory.class);
	private DatagenConf conf;
	
	public DatagenFactory(DatagenConf conf) {
		this.conf = conf;
	}
	
	
	/**Create the set of data generator based on the data variety user specified. 
	 * 
	 * @return List of data generator
	 */
	public List<DataGenerator> createGenerators() {
		List<DataGenerator> datagen_list = new LinkedList<DataGenerator>();
		
		Set<String> dataVariety = conf.getDataVariety();
 		Map<String, Integer> dataScaleProportions = conf.getDataScaleProportions();
 		Map<String, Float> dataTargetGBs = new HashMap<String, Float>();
		
		int targetGB = conf.getDataVolume();
				
		
		/**
		 * Get the portion of each data type in terms of the whole volume		
		 */
		float sum = 0;
		for(String dataType : dataVariety) {
			if(dataType.equals("relational")) {
				sum += dataScaleProportions.get(Constants.BIGFRAME_DATAVOLUME_RELATIONAL_PROPORTION);
			}
			
			else if (dataType.equals("graph")) {
				sum += dataScaleProportions.get(Constants.BIGFRAME_DATAVOLUME_GRAPH_PROPORTION);
			}
			
			else if (dataType.equals("nested")) {
				sum += dataScaleProportions.get(Constants.BIGFRAME_DATAVOLUME_NESTED_PROPORTION);
			}
		}
		
		for (Map.Entry<String, Integer> entry: dataScaleProportions.entrySet()) {
			String key = entry.getKey();
			Integer value = entry.getValue();
			
			if(key.equals(Constants.BIGFRAME_DATAVOLUME_RELATIONAL_PROPORTION)) {
				if(value/sum*targetGB < 1) {
					System.out.println("target data size of relational data cannot be less than 1GB\n" +
							"Please choose a larger data volume or increase the relative ratio of relational data");
					System.exit(-1);
				}
				dataTargetGBs.put("relational", value/sum*targetGB);
			}
			
			else if (key.equals(Constants.BIGFRAME_DATAVOLUME_GRAPH_PROPORTION)) {
				dataTargetGBs.put("graph", value/sum*targetGB);
			}
			
			else if (key.equals(Constants.BIGFRAME_DATAVOLUME_NESTED_PROPORTION)) {
				dataTargetGBs.put("nested", value/sum*targetGB);
			}
		}
		
		Map<Integer, DataGenerator> datagen_map = new HashMap<Integer, DataGenerator>();
		for(String variety : dataVariety) {
			if(variety.equals("relational")) {
				RelationalDataGen tpcds = new TpcdsDataGenNaive(conf, dataTargetGBs.get("relational"));				
				datagen_map.put(1,tpcds);
			}
			
			else if (variety.equals("graph")) {				
				//GraphDataGen twitter_graph = new KronGraphGenNaive(conf, dataTargetGBs.get("graph"));
				GraphDataGen twitter_graph = new KronGraphGenHadoop(conf, dataTargetGBs.get("graph"));
				//System.out.println(dataTargetGBs.get("graph"));
				//System.out.println(KroneckerGraphGen.getNodeCount(dataTargetGBs.get("graph")));
				//System.exit(-1);
				datagen_map.put(2, twitter_graph);
			}
			
			else if (variety.equals("nested")) {
				//NestedDataGen tweets = new RawTweetGenNaive(conf, dataTargetGBs.get("nested"));
				NestedDataGen tweets = new RawTweetGenHadoop(conf, dataTargetGBs.get("nested"));
				datagen_map.put(3, tweets);
			}
		}
	
		
		Set<Integer> keySet = datagen_map.keySet();
		
		for (Integer key: keySet) {
			datagen_list.add(datagen_map.get(key));
		}
		
		
		return datagen_list;
	}
}
