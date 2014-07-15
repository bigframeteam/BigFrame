package bigframe.qgen.engineDriver;

import java.util.ArrayList;
import java.util.List;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import bigframe.bigif.WorkflowInputFormat;
import bigframe.util.MapRedConfig;
import bigframe.workflows.runnable.GiraphRunnable;

public class GiraphEngineDriver extends EngineDriver {

	private GiraphConfiguration giraph_config;
	private static final Logger LOG = Logger.getLogger(GiraphEngineDriver.class);
	private List<GiraphRunnable> queries = new ArrayList<GiraphRunnable>();
	
	public GiraphEngineDriver(WorkflowInputFormat workIF) {
		super(workIF);
		Configuration mapred_config = MapRedConfig.getConfiguration(workIF);
		giraph_config = new GiraphConfiguration(mapred_config);
		Configuration.addDefaultResource("giraph-site.xml");
	}

	@Override
	public int numOfQueries() {
		// TODO Auto-generated method stub
		return queries.size();
	}

	@Override
	public void init() {
		// TODO Auto-generated method stub

	}

	public void addQuery(GiraphRunnable q) {
		queries.add(q);
	}
	
	@Override
	public void run() {
		LOG.info("Running Giraph queries!");
		for(GiraphRunnable query : queries) {
			if(query.runGiraph(giraph_config))
				LOG.info("Query Finished");
			else
				LOG.info("Query failed");
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
