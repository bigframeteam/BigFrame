package bigframe.workflows.BusinessIntelligence.graph.exploratory;

import bigframe.workflows.HadoopJob;

import org.apache.hadoop.conf.Configuration;

/**
 * A Hadoop implementation for PageRank.
 * 
 * @author andy
 *
 */
public class PageRankHadoop extends HadoopJob {

	public PageRankHadoop(Configuration mapred_config) {
		super(mapred_config);
		// TODO Auto-generated constructor stub
	}

	public Boolean run(Configuration mapred_config) {
		
		if(mapred_config == null)
			mapred_config = mapred_config();
		
		return false;
	}
}
