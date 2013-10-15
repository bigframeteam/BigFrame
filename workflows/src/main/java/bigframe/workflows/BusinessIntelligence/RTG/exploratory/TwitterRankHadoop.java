package bigframe.workflows.BusinessIntelligence.RTG.exploratory;

import bigframe.workflows.HadoopJob;

import org.apache.hadoop.conf.Configuration;

public abstract class TwitterRankHadoop extends HadoopJob {

	protected int num_iter = 10;
	
	public TwitterRankHadoop(int num_iter, Configuration mapred_config) {
		super(mapred_config);
		this.num_iter = num_iter;
	}

}
