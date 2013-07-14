package bigframe.datagen;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import bigframe.BigConfConstants;
import bigframe.datagen.graph.GraphDataGen;
import bigframe.datagen.graph.KronGraphGenHadoop;
import bigframe.datagen.nested.NestedDataGen;
import bigframe.datagen.nested.RawTweetGenHadoop;
import bigframe.datagen.relational.RelationalDataGen;
import bigframe.datagen.relational.TpcdsDataGenNaive;


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

		String app_domain = conf.getAppDomain();

		if (app_domain.equals(BigConfConstants.APPLICATION_BI)) {

			for(String variety : dataVariety) {
				if (variety.equals("graph")) {
					//GraphDataGen twitter_graph = new KronGraphGenNaive(conf, dataTargetGBs.get("graph"));
					GraphDataGen twitter_graph = new KronGraphGenHadoop(conf,
							conf.getDataTypeTargetGB("graph"));
					datagen_list.add(twitter_graph);
				}

				else if (variety.equals("nested")) {
					//NestedDataGen tweets = new RawTweetGenNaive(conf, dataTargetGBs.get("nested"));
					NestedDataGen tweets = new RawTweetGenHadoop(conf,
							conf.getDataTypeTargetGB("nested"));
					datagen_list.add(tweets);
				}

				else if (variety.equals("relational")) {
					RelationalDataGen tpcds = new TpcdsDataGenNaive(conf,
							conf.getDataTypeTargetGB("relational"));
					datagen_list.add(tpcds);
				}
			}
		}

		return datagen_list;
	}
}
